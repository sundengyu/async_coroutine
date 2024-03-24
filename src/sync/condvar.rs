use super::{futex::Futex, mutex::co_mutex_t};
use crate::loom::sync::atomic::Ordering::*;
use std::{ptr::NonNull, time::Duration};

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct co_cond_t {
    // The value of this atomic is simply incremented on every notification.
    // This is used by `.wait()` to not miss any notifications after
    // unlocking the mutex and before waiting for notifications.
    futex: Futex,
}

impl co_cond_t {
    #[inline]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            futex: Futex::new(0),
        }
    }

    // All the memory orderings here are `Relaxed`,
    // because synchronization is done by unlocking and locking the mutex.

    pub fn notify_one(&self) {
        self.futex.inc_ref();
        self.futex.value.fetch_add(1, Relaxed);
        unsafe { self.futex.wake_one() };
        self.futex.dec_ref();
    }

    pub fn notify_all(&self) {
        self.futex.inc_ref();
        self.futex.value.fetch_add(1, Relaxed);
        self.futex.wake_all();
        self.futex.dec_ref();
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn wait(&self, mutex: NonNull<co_mutex_t>) {
        self.wait_optional_timeout(mutex, None);
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn wait_timeout(&self, mutex: NonNull<co_mutex_t>, duration: Duration) -> bool {
        self.wait_optional_timeout(mutex, Some(duration))
    }

    unsafe fn wait_optional_timeout(
        &self,
        mutex: NonNull<co_mutex_t>,
        duration: Option<Duration>,
    ) -> bool {
        // Examine the notification counter _before_ we unlock the mutex.
        let futex_value = self.futex.value.load(Relaxed);

        // Unlock the mutex before going to sleep.
        mutex.as_ref().unlock();

        // Wait, but only if there hasn't been any
        // notification since we unlocked the mutex.
        let res = self.futex.wait_until(futex_value, duration);

        // Lock the mutex again.
        mutex.as_ref().lock();

        res
    }

    pub fn destroy(&self) {
        unsafe { self.futex.destroy() };
    }
}
