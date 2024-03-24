use super::linked_list::{Link, LinkedList, Pointers};
use crate::context::coroutine::AsyncCoroutine;
use crate::loom::hint;
use crate::loom::sync::atomic::{AtomicU32, Ordering};
use crate::loom::sync::Mutex;
use futures::{Future, FutureExt};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::time::timeout;

macro_rules! generate_addr_of_methods {
    (
    impl<$($gen:ident)*> $struct_name:ty {$(
        $(#[$attrs:meta])*
	$vis:vis unsafe fn $fn_name:ident(self: NonNull<Self>) -> NonNull<$field_type:ty> {
            &self$(.$field_name:tt)+
	}
    )*}
    ) => {
        impl<$($gen)*> $struct_name {$(
            $(#[$attrs])*
	    $vis unsafe fn $fn_name(me: ::core::ptr::NonNull<Self>) -> ::core::ptr::NonNull<$field_type> {
                let me = me.as_ptr();
		let field = ::std::ptr::addr_of_mut!((*me) $(.$field_name)+ );
		::core::ptr::NonNull::new_unchecked(field)
            }
        )*}
    };
}

struct WaiterNode {
    pointers: Pointers<WaiterNode>,
    waker: Mutex<Option<Waker>>,
    /// Should not be `Unpin`.
    _p: PhantomPinned,
}

impl WaiterNode {
    fn new() -> Self {
        Self {
            pointers: Pointers::new(),
            waker: Mutex::new(None),
            _p: PhantomPinned,
        }
    }
}

generate_addr_of_methods! {
    impl<> WaiterNode {
        unsafe fn addr_of_pointers(self: NonNull<Self>) -> NonNull<Pointers<WaiterNode>> {
            &self.pointers
        }
    }
}

unsafe impl Link for WaiterNode {
    type Handle = NonNull<WaiterNode>;
    type Target = WaiterNode;

    fn as_raw(handle: &NonNull<WaiterNode>) -> NonNull<WaiterNode> {
        *handle
    }

    unsafe fn from_raw(ptr: NonNull<WaiterNode>) -> NonNull<WaiterNode> {
        ptr
    }

    unsafe fn pointers(target: NonNull<WaiterNode>) -> NonNull<Pointers<WaiterNode>> {
        WaiterNode::addr_of_pointers(target)
    }
}

struct FutexWaiter<'a> {
    node: WaiterNode,
    expected_value: u32,
    futex: &'a Futex,
    queued: bool,
}

impl Future for FutexWaiter<'_> {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.queued {
            let cur_val = self.futex.value.load(Ordering::Relaxed);
            if cur_val != self.expected_value {
                return Poll::Ready(libc::EWOULDBLOCK);
            }

            let mut waiters_list = self.futex.waiters_list.lock().unwrap();
            let cur_val = self.futex.value.load(Ordering::Relaxed);
            if cur_val != self.expected_value {
                return Poll::Ready(libc::EWOULDBLOCK);
            }

            let self_mut = unsafe { self.get_unchecked_mut() };
            self_mut.queued = true;
            assert!(self_mut
                .node
                .waker
                .lock()
                .unwrap()
                .replace(cx.waker().clone())
                .is_none());
            waiters_list.push_front(NonNull::from(&mut self_mut.node));
            Poll::Pending
        } else {
            let self_mut = unsafe { self.get_unchecked_mut() };
            let mut waker = self_mut.node.waker.lock().unwrap();
            match waker.as_mut() {
                None => {
                    self_mut.queued = false;
                    Poll::Ready(0)
                }
                Some(w) => {
                    if !w.will_wake(cx.waker()) {
                        waker.replace(cx.waker().clone());
                    }
                    Poll::Pending
                }
            }
        }
    }
}

impl Drop for FutexWaiter<'_> {
    fn drop(&mut self) {
        if self.queued {
            let mut waiters_list = self.futex.waiters_list.lock().unwrap();
            let waker = self.node.waker.lock().unwrap();
            if waker.is_some() {
                drop(waker);
                unsafe { waiters_list.remove(NonNull::from(&mut self.node)) };
            } else {
                Futex::wake_one_from_waiters(&mut waiters_list);
            }
        }
    }
}

#[repr(C)]
pub(crate) struct Futex {
    waiters_list: Mutex<LinkedList<WaiterNode, <WaiterNode as Link>::Target>>,
    pub(crate) value: AtomicU32,
    ref_cnt: AtomicU32,
}

impl Futex {
    #[inline]
    pub(crate) fn new(value: u32) -> Self {
        Self {
            waiters_list: Mutex::new(LinkedList::new()),
            value: AtomicU32::new(value),
            ref_cnt: AtomicU32::new(0),
        }
    }

    #[inline]
    pub(crate) fn inc_ref(&self) {
        self.ref_cnt.fetch_add(1, Ordering::Acquire);
    }

    #[inline]
    pub(crate) fn dec_ref(&self) {
        self.ref_cnt.fetch_sub(1, Ordering::Release);
    }

    fn wait(&self, expected_value: u32) -> FutexWaiter {
        FutexWaiter {
            node: WaiterNode::new(),
            expected_value,
            futex: self,
            queued: false,
        }
    }

    pub(crate) unsafe fn wait_until(
        &self,
        expected_value: u32,
        duration: Option<Duration>,
    ) -> bool {
        let waiter = self.wait(expected_value).fuse();
        if let Some(duration) = duration {
            AsyncCoroutine::tls_coroutine()
                .poll_until_ready(timeout(duration, waiter))
                .is_ok()
        } else {
            AsyncCoroutine::tls_coroutine().poll_until_ready(waiter);
            true
        }
    }

    #[inline]
    fn wake_one_from_waiters(
        waiters: &mut LinkedList<WaiterNode, <WaiterNode as Link>::Target>,
    ) -> bool {
        if let Some(waiter) = waiters.pop_back() {
            let mut waker = unsafe { waiter.as_ref().waker.lock().unwrap() };
            waker.take().unwrap().wake_by_ref();
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) unsafe fn wake_one(&self) -> bool {
        let mut waiters = self.waiters_list.lock().unwrap();
        Self::wake_one_from_waiters(&mut waiters)
    }

    // TODO(sundengyu): implement futex requeue
    #[inline]
    pub(crate) fn wake_all(&self) {
        let mut waiters = self.waiters_list.lock().unwrap();
        while Self::wake_one_from_waiters(&mut waiters) {}
    }

    #[inline]
    pub(crate) unsafe fn destroy(&self) {
        let mut nspin = 0;
        while self.ref_cnt.load(Ordering::Acquire) > 0 {
            hint::spin_loop();
            nspin += 1;
            if nspin % 100 == 0 {
                libc::sched_yield();
            }
        }
    }
}
