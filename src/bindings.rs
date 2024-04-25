use crate::{
    context::coroutine::AsyncCoroutine,
    io::async_io,
    sync::{condvar::co_cond_t, mutex::co_mutex_t, rwlock::co_rwlock_t},
};
use dashmap::DashMap;
use futures::FutureExt;
use libc::c_char;
use once_cell::sync::OnceCell;
use std::{
    os::raw::c_void,
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tokio::runtime::Handle;

static COROUTINE_KEY: AtomicU32 = AtomicU32::new(0);

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sched_yield() {
    AsyncCoroutine::tls_coroutine().sched_yield();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_exit() {
    AsyncCoroutine::tls_coroutine().exit();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_create_key(key: *mut u32) {
    *key = COROUTINE_KEY.fetch_add(1, Ordering::Relaxed);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_get_key(key: u32) -> *mut c_void {
    AsyncCoroutine::tls_coroutine().get_specific(key)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_set_key(key: u32, data: *mut c_void) {
    AsyncCoroutine::tls_coroutine().set_specific(key, data)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_delete_key(key: u32) {
    AsyncCoroutine::tls_coroutine().delete_key(key)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_self() -> u64 {
    AsyncCoroutine::tls_coroutine().id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sleep(duration: *const libc::timespec) {
    let duration = &*duration;
    let duration = Duration::new(duration.tv_sec as u64, duration.tv_nsec as u32);
    AsyncCoroutine::tls_coroutine().poll_until_ready(tokio::time::sleep(duration));
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_init(mutex: *mut co_mutex_t) {
    *mutex = co_mutex_t::new();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_destroy(mutex: *mut co_mutex_t) {
    (*mutex).destroy();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_lock(mutex: *mut co_mutex_t) {
    (*mutex).lock();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_trylock(mutex: *mut co_mutex_t) -> i32 {
    if (*mutex).try_lock() {
        0
    } else {
        libc::EBUSY
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_unlock(mutex: *mut co_mutex_t) {
    (*mutex).unlock();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_init(cond: *mut co_cond_t) {
    (*cond) = co_cond_t::new();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_destroy(cond: *mut co_cond_t) {
    (*cond).destroy();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_wait(cond: *mut co_cond_t, mutex: *mut co_mutex_t) {
    (*cond).wait(NonNull::new_unchecked(mutex));
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_timedwait(
    cond: *mut co_cond_t,
    mutex: *mut co_mutex_t,
    duration: *mut libc::timespec,
) -> i32 {
    let duration = &*duration;
    let duration = Duration::new(duration.tv_sec as u64, duration.tv_nsec as u32);
    if (*cond).wait_timeout(NonNull::new_unchecked(mutex), duration) {
        0
    } else {
        libc::ETIMEDOUT
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_signal(cond: *mut co_cond_t) {
    (*cond).notify_one();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_broadcast(cond: *mut co_cond_t) {
    (*cond).notify_all();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_init(rwlock: *mut co_rwlock_t) {
    *rwlock = co_rwlock_t::new();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_destroy(rwlock: *mut co_rwlock_t) {
    (*rwlock).destroy();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rw_lock_write(rwlock: *mut co_rwlock_t) {
    (*rwlock).write();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rw_lock_read(rwlock: *mut co_rwlock_t) {
    (*rwlock).read();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_try_write(rwlock: *mut co_rwlock_t) -> i32 {
    if (*rwlock).try_write() {
        0
    } else {
        libc::EBUSY
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_try_read(rwlock: *mut co_rwlock_t) -> i32 {
    if (*rwlock).try_read() {
        0
    } else {
        libc::EBUSY
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rw_unlock(rwlock: *mut co_rwlock_t) {
    (*rwlock).unlock();
}

static ID_TASK_HANDLE_MAP: OnceCell<DashMap<u64, tokio::task::JoinHandle<usize>>> = OnceCell::new();

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_create(
    thread_func: Option<unsafe extern "C" fn(arg1: *mut c_void) -> *mut c_void>,
    arg: *mut c_void,
    joinable: i32,
    blocking: i32,
) -> u64 {
    let coroutine = AsyncCoroutine::new_c(arg, thread_func.unwrap());
    let id = coroutine.id;
    let fut = coroutine.fuse();
    let handle = if blocking != 0 {
        tokio::task::spawn_blocking(move || Handle::current().block_on(fut))
    } else {
        tokio::spawn(fut)
    };

    if joinable != 0 {
        ID_TASK_HANDLE_MAP
            .get_or_init(DashMap::new)
            .insert(id, handle);
    }

    id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_join(id: u64, value_ptr: *mut *mut c_void) {
    let handle = ID_TASK_HANDLE_MAP.get().unwrap().remove(&id).unwrap().1;
    let ret = AsyncCoroutine::tls_coroutine()
        .poll_until_ready(handle)
        .unwrap();
    if !value_ptr.is_null() {
        *value_ptr = ret as *mut c_void;
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn register_fd(
    fd: i32,
    nr_requests: u32,
    cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
) {
    async_io::register_fd(fd, nr_requests, cb);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn unregister_fd(fd: i32) {
    AsyncCoroutine::tls_coroutine().poll_until_ready(async_io::unregister_fd(fd));
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_read(
    fd: i32,
    offset: u64,
    buf: *mut c_char,
    size: u64,
    arg: *mut c_void,
) {
    async_io::submit_read(fd, offset, buf as *mut u8, size, arg);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_write(
    fd: i32,
    offset: u64,
    buf: *const c_char,
    size: u64,
    arg: *mut c_void,
) {
    async_io::submit_write(fd, offset, buf as *const u8, size, arg);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_fsync(fd: i32, arg: *mut c_void) {
    async_io::submit_fsync(fd, arg);
}
