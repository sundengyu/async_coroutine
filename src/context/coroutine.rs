use super::libcontext::{fcontext_t, jump_fcontext, make_fcontext};
use super::stack::{fetch_or_alloc_stack, return_or_release_stack, Stack};
use dashmap::DashMap;
use futures::{Future, FutureExt};
use libc::{c_void, intptr_t};
use once_cell::sync::OnceCell;
use std::cell::Cell;
use std::collections::HashMap;
use std::mem::transmute;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::task::Context;
use std::task::Poll;

unsafe extern "C" fn task_runner<A, B>(_: intptr_t) {
    let tls_coroutine = AsyncCoroutine::tls_coroutine();
    let func: extern "C" fn(&A) -> B = transmute(tls_coroutine.func);
    let arg = &*(tls_coroutine.arg as *const A);
    let ret = tls_coroutine.ret as *mut B;
    *ret = func(arg);
    tls_coroutine.state = RunState::Done;
    jump_fcontext(
        &mut tls_coroutine.pollee_context,
        tls_coroutine.poller_context,
        0,
        1,
    );
}

unsafe extern "C" fn task_runner_c(_: intptr_t) {
    let tls_coroutine = AsyncCoroutine::tls_coroutine();
    let func: unsafe extern "C" fn(*mut c_void) -> *mut c_void = transmute(tls_coroutine.func);
    tls_coroutine.ret = func(tls_coroutine.ret);
    tls_coroutine.state = RunState::Done;
    jump_fcontext(
        &mut tls_coroutine.pollee_context,
        tls_coroutine.poller_context,
        0,
        1,
    );
}

static KEY_DESTRUCTORS: OnceCell<DashMap<u32, unsafe extern "C" fn(arg: *mut c_void)>> =
    OnceCell::new();

thread_local! {
    static TLS_COROUTINE: Cell<usize> = const { Cell::new(0) };
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum RunState {
    Runnable,
    Pending,
    Yielded,
    Done,
}

pub struct AsyncCoroutine {
    poller_context: fcontext_t,
    pollee_context: fcontext_t,
    stack: Option<Stack>,
    context: Option<NonNull<Context<'static>>>,
    state: RunState,
    arg: *const c_void,
    ret: *mut c_void,
    func: usize,
    specific: HashMap<u32, *mut c_void>,
    saved_errno: i32,
    pub(crate) id: u64,
}

const STACK_SIZE: usize = 1 << 20;

impl AsyncCoroutine {
    #[inline]
    pub(crate) fn tls_coroutine<'a>() -> &'a mut Self {
        unsafe { &mut *(TLS_COROUTINE.get() as *mut Self) }
    }

    #[inline]
    pub(crate) fn new_c(
        arg: *mut c_void,
        func: unsafe extern "C" fn(arg1: *mut c_void) -> *mut c_void,
    ) -> Self {
        let stack = fetch_or_alloc_stack(STACK_SIZE);
        let pollee_context =
            unsafe { make_fcontext(stack.stack_bottom, STACK_SIZE, Some(task_runner_c)) };
        Self {
            poller_context: ptr::null_mut(),
            pollee_context,
            id: stack.stack_id,
            stack: Some(stack),
            context: None,
            state: RunState::Runnable,
            arg: std::ptr::null(),
            ret: arg,
            func: func as usize,
            specific: HashMap::new(),
            saved_errno: 0,
        }
    }

    #[inline]
    fn new<A, B>(func: extern "C" fn(&A) -> B, arg: &A, ret: &mut B) -> Self {
        let stack = fetch_or_alloc_stack(STACK_SIZE);
        let pollee_context =
            unsafe { make_fcontext(stack.stack_bottom, STACK_SIZE, Some(task_runner::<A, B>)) };
        let arg = arg as *const A as *const c_void;
        let ret = ret as *mut B as *mut c_void;
        Self {
            poller_context: ptr::null_mut(),
            pollee_context,
            id: stack.stack_id,
            stack: Some(stack),
            context: None,
            state: RunState::Runnable,
            arg,
            ret,
            func: func as usize,
            specific: HashMap::new(),
            saved_errno: 0,
        }
    }

    pub async fn run_in_coroutine<A, B: Default>(func: extern "C" fn(&A) -> B, arg: &A) -> B {
        let mut ret = B::default();
        Self::new(func, arg, &mut ret).fuse().await;
        ret
    }

    #[inline]
    pub(crate) unsafe fn sched_yield(&mut self) {
        self.state = RunState::Yielded;
        jump_fcontext(&mut self.pollee_context, self.poller_context, 0, 1);
    }

    #[inline]
    pub(crate) unsafe fn exit(&mut self) {
        self.state = RunState::Done;
        jump_fcontext(&mut self.pollee_context, self.poller_context, 0, 1);
    }

    #[inline]
    pub(crate) unsafe fn pend_and_switch(&mut self) {
        self.state = RunState::Pending;
        jump_fcontext(&mut self.pollee_context, self.poller_context, 0, 1);
    }

    #[inline]
    unsafe fn run(&mut self, cx: &mut Context<'_>) -> RunState {
        self.state = RunState::Runnable;
        TLS_COROUTINE.set(self as *mut _ as usize);
        let cx: &mut Context<'static> = transmute(cx);
        self.context.replace(NonNull::from(cx));
        *libc::__errno_location() = self.saved_errno;
        jump_fcontext(&mut self.poller_context, self.pollee_context, 0, 1);
        self.saved_errno = *libc::__errno_location();
        TLS_COROUTINE.set(0);
        self.state
    }

    #[inline]
    pub(crate) fn set_specific(&mut self, k: u32, v: *mut c_void) {
        self.specific.insert(k, v);
    }

    #[inline]
    pub(crate) fn get_specific(&self, k: u32) -> *mut c_void {
        self.specific.get(&k).map_or(std::ptr::null_mut(), |v| *v)
    }

    #[inline]
    pub(crate) unsafe fn delete_key(&mut self, k: u32) {
        if let Some(v) = self.specific.remove(&k) {
            if let Some(destructor) = KEY_DESTRUCTORS.get().unwrap().get(&k) {
                let destructor = destructor.value().to_owned();
                destructor(v);
            }
        }
    }

    #[inline]
    pub(crate) unsafe fn poll_until_ready<F: Future<Output = T>, T>(&mut self, f: F) -> T {
        tokio::pin!(f);
        loop {
            match f.poll_unpin(self.context.unwrap().as_mut()) {
                Poll::Ready(res) => return res,
                Poll::Pending => self.pend_and_switch(),
            }
        }
    }
}

unsafe impl Send for AsyncCoroutine {}
unsafe impl Sync for AsyncCoroutine {}

impl Drop for AsyncCoroutine {
    fn drop(&mut self) {
        assert_eq!(self.state, RunState::Done);
        let stack: Stack = self.stack.take().unwrap();
        return_or_release_stack(stack);
        for (k, v) in &self.specific {
            if let Some(destructor) = KEY_DESTRUCTORS.get().unwrap().get(k) {
                let destructor = destructor.value().to_owned();
                unsafe { destructor(*v) };
            }
        }
    }
}

impl Future for AsyncCoroutine {
    type Output = usize;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match unsafe { self.run(cx) } {
            RunState::Runnable => panic!("runnable unexpected"),
            RunState::Pending => Poll::Pending,
            RunState::Yielded => {
                let fut = tokio::task::yield_now();
                tokio::pin!(fut);
                let _ = fut.poll(cx);
                Poll::Pending
            }
            RunState::Done => Poll::Ready(self.ret as usize),
        }
    }
}
