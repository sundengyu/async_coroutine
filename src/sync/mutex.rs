use crate::loom::sync::atomic::Ordering::*;
use crate::sync::futex::Futex;

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct co_mutex_t {
    pub(crate) futex: Futex,
}

impl co_mutex_t {
    #[inline]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            futex: Futex::new(0),
        }
    }

    #[inline]
    pub fn try_lock(&self) -> bool {
        self.futex
            .value
            .compare_exchange(0, 1, Acquire, Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn lock(&self) {
        if self
            .futex
            .value
            .compare_exchange(0, 1, Acquire, Relaxed)
            .is_err()
        {
            self.lock_contended();
        }
    }

    #[cold]
    fn lock_contended(&self) {
        // Spin first to speed things up if the lock is released quickly.
        let mut state = self.spin();

        // If it's unlocked now, attempt to take the lock
        // without marking it as contended.
        if state == 0 {
            match self.futex.value.compare_exchange(0, 1, Acquire, Relaxed) {
                Ok(_) => return, // Locked!
                Err(s) => state = s,
            }
        }

        loop {
            // Put the lock in contended state.
            // We avoid an unnecessary write if it as already set to 2,
            // to be friendlier for the caches.
            if state != 2 && self.futex.value.swap(2, Acquire) == 0 {
                // We changed it from 0 to 2, so we just successfully locked it.
                return;
            }

            unsafe { self.futex.wait_until(2, None) };

            // Spin again after waking up.
            state = self.spin();
        }
    }

    fn spin(&self) -> u32 {
        let mut spin = 100;
        loop {
            // We only use `load` (and not `swap` or `compare_exchange`)
            // while spinning, to be easier on the caches.
            let state = self.futex.value.load(Relaxed);

            // We stop spinning when the mutex is unlocked (0),
            // but also when it's contended (2).
            if state != 1 || spin == 0 {
                return state;
            }

            std::hint::spin_loop();
            spin -= 1;
        }
    }

    #[inline]
    pub fn unlock(&self) {
        self.futex.inc_ref();
        if self.futex.value.swap(0, Release) == 2 {
            // We only wake up one coroutine. When that coroutine locks the mutex, it
            // will mark the mutex as contended (2) (see lock_contended above),
            // which makes sure that any other waiting coroutines will also be
            // woken up eventually.
            self.wake();
        }
        self.futex.dec_ref();
    }

    #[inline]
    fn wake(&self) {
        unsafe { self.futex.wake_one() };
    }

    pub fn destroy(&self) {
        unsafe { self.futex.destroy() };
    }
}
