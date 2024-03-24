use crate::bindings::{
    co_cond_broadcast, co_cond_destroy, co_cond_wait, co_mutex_destroy, co_mutex_lock,
    co_mutex_unlock, co_rw_lock_read, co_rw_lock_write, co_rw_unlock,
};
use crate::context::coroutine::AsyncCoroutine;
use crate::sync::condvar::co_cond_t;
use crate::sync::mutex::co_mutex_t;
use crate::sync::rwlock::co_rwlock_t;
use libc::c_void;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};

struct LockedValue {
    lock: co_mutex_t,
    value: i32,
}

unsafe extern "C" fn mutex_worker(arg: *mut c_void) -> *mut c_void {
    let value = &mut *(arg as *mut LockedValue);
    co_mutex_lock(&mut value.lock);
    value.value += 1;
    co_mutex_unlock(&mut value.lock);
    std::ptr::null_mut()
}

#[tokio::test(flavor = "multi_thread")]
async fn mutex_test() {
    let concurrency = 256;
    for _ in 0..1000 {
        let mut value = LockedValue {
            lock: co_mutex_t::new(),
            value: 0,
        };
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let coroutine =
                AsyncCoroutine::new_c(&mut value as *mut LockedValue as *mut c_void, mutex_worker);
            handles.push(tokio::spawn(coroutine));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(value.value, concurrency as i32);
    }
}

struct RwlockedValue {
    lock: co_rwlock_t,
    value: i32,
}

unsafe extern "C" fn rwlock_worker(arg: *mut c_void) -> *mut c_void {
    let value = &mut *(arg as *mut RwlockedValue);
    co_rw_lock_write(&mut value.lock);
    value.value += 1;
    co_rw_unlock(&mut value.lock);

    co_rw_lock_read(&mut value.lock);
    let orig_val = value.value;
    for _ in 0..10 {
        assert_eq!(orig_val, value.value);
    }
    co_rw_unlock(&mut value.lock);
    std::ptr::null_mut()
}

#[tokio::test(flavor = "multi_thread")]
async fn rwlock_test() {
    let concurrency = 128;
    for _ in 0..1000 {
        let mut value = RwlockedValue {
            lock: co_rwlock_t::new(),
            value: 0,
        };
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let coroutine = AsyncCoroutine::new_c(
                &mut value as *mut RwlockedValue as *mut c_void,
                rwlock_worker,
            );
            handles.push(tokio::spawn(coroutine));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(value.value, concurrency as i32);
    }
}

struct BlockQueue {
    mutex: co_mutex_t,
    cv_pop: co_cond_t,
    cv_push: co_cond_t,
    queue: Vec<u64>,
    capacity: usize,
}

impl BlockQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            mutex: co_mutex_t::new(),
            cv_pop: co_cond_t::new(),
            cv_push: co_cond_t::new(),
            queue: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub unsafe fn pop(&mut self) -> u64 {
        co_mutex_lock(&mut self.mutex);
        while self.queue.is_empty() {
            co_cond_wait(&mut self.cv_pop, &mut self.mutex);
        }

        let res = self.queue.pop().unwrap();
        if self.queue.len() + 1 == self.capacity {
            co_cond_broadcast(&mut self.cv_push);
        }
        co_mutex_unlock(&mut self.mutex);

        res
    }

    pub unsafe fn push(&mut self, value: u64) {
        co_mutex_lock(&mut self.mutex);
        while self.queue.len() >= self.capacity {
            assert_eq!(self.queue.len(), self.capacity);
            co_cond_wait(&mut self.cv_push, &mut self.mutex);
        }

        self.queue.push(value);
        if self.queue.len() == 1 {
            co_cond_broadcast(&mut self.cv_pop);
        }
        co_mutex_unlock(&mut self.mutex);
    }
}

impl Drop for BlockQueue {
    fn drop(&mut self) {
        unsafe {
            co_mutex_destroy(&mut self.mutex);
            co_cond_destroy(&mut self.cv_pop);
            co_cond_destroy(&mut self.cv_push);
        }
    }
}

struct BlockQueueTest {
    bq: BlockQueue,
    sum_producer: AtomicU64,
    sum_consumer: AtomicU64,
    // same for producers and consumers
    nops_per_worker: usize,
}

unsafe extern "C" fn block_queue_consumer(queue: *mut c_void) -> *mut c_void {
    let bq_test = (queue as *mut BlockQueueTest).as_mut().unwrap();
    for _ in 0..bq_test.nops_per_worker {
        let value = bq_test.bq.pop();
        bq_test.sum_producer.fetch_add(value, Ordering::Relaxed);
    }
    std::ptr::null_mut()
}

unsafe extern "C" fn block_queue_producer(queue: *mut c_void) -> *mut c_void {
    let bq_test = (queue as *mut BlockQueueTest).as_mut().unwrap();
    for _ in 0..bq_test.nops_per_worker {
        let value = rand::thread_rng().gen_range(1..(1 << 20));
        bq_test.bq.push(value);
        bq_test.sum_consumer.fetch_add(value, Ordering::Relaxed);
    }
    std::ptr::null_mut()
}

#[tokio::test(flavor = "multi_thread")]
async fn block_queue_test() {
    let workers = 32;
    let queue_capacity = 20;
    let nops_per_worker = 1 << 10;

    let mut bq_test = BlockQueueTest {
        bq: BlockQueue::new(queue_capacity),
        sum_consumer: AtomicU64::new(0),
        sum_producer: AtomicU64::new(0),
        nops_per_worker,
    };

    let mut handles = Vec::new();
    for _ in 0..workers {
        let coroutine = AsyncCoroutine::new_c(
            &mut bq_test as *mut BlockQueueTest as *mut c_void,
            block_queue_producer,
        );
        handles.push(tokio::spawn(coroutine));

        let coroutine = AsyncCoroutine::new_c(
            &mut bq_test as *mut BlockQueueTest as *mut c_void,
            block_queue_consumer,
        );
        handles.push(tokio::spawn(coroutine));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(
        bq_test.sum_consumer.load(Ordering::Relaxed),
        bq_test.sum_producer.load(Ordering::Relaxed)
    );
}
