use async_coroutine::{bindings::*, context::coroutine::AsyncCoroutine, sync::mutex::co_mutex_t};
use libc::c_void;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};
use tokio::sync::Mutex;

struct LockedValue {
    lock: co_mutex_t,
    value: usize,
    nloops: usize,
}

unsafe extern "C" fn mutex_worker(arg: *mut c_void) -> *mut c_void {
    let value = &mut *(arg as *mut LockedValue);
    let nloops = value.nloops;

    for _ in 0..nloops {
        co_mutex_lock(&mut value.lock);
        value.value += 1;
        co_mutex_unlock(&mut value.lock);
    }
    std::ptr::null_mut()
}

static COUNT_LOCKED: Mutex<usize> = Mutex::const_new(0);
static COUNT_ATOMIC: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    let concurrency = 16;
    let nloops = 100000000;
    let mut locked_value = LockedValue {
        lock: co_mutex_t::new(),
	value: 0,
        nloops,
    };

    let now = SystemTime::now();
    let coroutines: Vec<_> = (0..concurrency)
        .map(|_| {
            AsyncCoroutine::new_c(
                &mut locked_value as *mut LockedValue as *mut c_void,
                mutex_worker,
            )
        })
        .collect();
    let handles: Vec<_> = coroutines.into_iter().map(|c| tokio::spawn(c)).collect();
    for handle in handles {
        handle.await.unwrap();
    }
    let micros = now.elapsed().unwrap().as_micros() as usize;
    let iops = concurrency * nloops * 1000000 / micros;
//     assert_eq!(locked_value.value, concurrency * nloops);
    println!("coroutine iops: {iops}");

    let now = SystemTime::now();
    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            tokio::spawn(async move {
                for _ in 0..nloops {
                    let mut count = COUNT_LOCKED.lock().await;
                    *count += 1;
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }
    let micros = now.elapsed().unwrap().as_micros() as usize;
    let iops = concurrency * nloops * 1000000 / micros;
    println!("locked iops: {iops}");

    let now = SystemTime::now();
    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            tokio::spawn(async move {
                for _ in 0..nloops {
                    COUNT_ATOMIC.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }
    let micros = now.elapsed().unwrap().as_micros() as usize;
    let iops = concurrency * nloops * 1000000 / micros;
    println!("atomic iops: {iops}")
}
