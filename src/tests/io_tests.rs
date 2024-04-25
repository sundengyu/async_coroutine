use crate::{
    bindings::{register_fd, submit_read, submit_write},
    context::coroutine::AsyncCoroutine,
};
use libc::c_void;
use rand::Rng;
use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
use tokio::sync::Semaphore;

const BLOCK_SIZE: usize = 4 << 10;
unsafe extern "C" fn io_cb(arg: *mut libc::c_void, res: i64) {
    assert_eq!(res, BLOCK_SIZE as i64);
    let sem = &*(arg as *mut Semaphore);
    sem.add_permits(1);
}

struct WriteContext {
    fd: i32,
    offset: u64,
    size: u64,
    data: *const i8,
    arg: *mut c_void,
}

unsafe extern "C" fn write_submitter(arg: *mut c_void) -> *mut c_void {
    let context = &*(arg as *const WriteContext);
    submit_write(
        context.fd,
        context.offset,
        context.data,
        context.size,
        context.arg,
    );
    std::ptr::null_mut()
}

struct ReadContext {
    fd: i32,
    offset: u64,
    size: u64,
    data: *mut i8,
    arg: *mut c_void,
}

unsafe extern "C" fn read_submitter(arg: *mut c_void) -> *mut c_void {
    let context = &*(arg as *const ReadContext);
    submit_read(
        context.fd,
        context.offset,
        context.data,
        context.size,
        context.arg,
    );
    std::ptr::null_mut()
}

#[tokio::test(flavor = "multi_thread")]
async fn async_io_test() {
    let dev = "/dev/sdb";
    let mut options = std::fs::OpenOptions::new();
    let f = options
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(&dev)
        .unwrap();
    let fd = f.as_raw_fd();
    unsafe { register_fd(fd, 256, io_cb) };

    for _ in 0..100 {
        let concurrency = 32 << 10;
        let write_data: Vec<Vec<u8>> = (0..concurrency)
            .map(|_| vec![rand::thread_rng().gen(); BLOCK_SIZE])
            .collect();
        let mut sem = Semaphore::new(0);
        let mut write_contexts: Vec<_> = write_data
            .iter()
            .enumerate()
            .map(|(i, data)| WriteContext {
                fd,
                offset: (i * BLOCK_SIZE) as u64,
                size: data.len() as u64,
                data: data.as_ptr() as *const i8,
                arg: &mut sem as *mut Semaphore as *mut c_void,
            })
            .collect();

        for context in &mut write_contexts {
            let coroutine =
                AsyncCoroutine::new_c(context as *mut WriteContext as *mut c_void, write_submitter);
            tokio::spawn(coroutine);
        }

        sem.acquire_many(concurrency).await.unwrap().forget();

        let mut read_data: Vec<Vec<u8>> = (0..concurrency)
            .map(|_| {
                let mut data = Vec::with_capacity(BLOCK_SIZE);
                unsafe { data.set_len(BLOCK_SIZE) };
                data
            })
            .collect();
        let mut read_contexts: Vec<_> = read_data
            .iter_mut()
            .enumerate()
            .map(|(i, data)| ReadContext {
                fd,
                offset: (i * BLOCK_SIZE) as u64,
                size: data.len() as u64,
                data: data.as_mut_ptr() as *mut i8,
                arg: &mut sem as *mut Semaphore as *mut c_void,
            })
            .collect();

        for context in &mut read_contexts {
            let coroutine =
                AsyncCoroutine::new_c(context as *mut ReadContext as *mut c_void, read_submitter);
            tokio::spawn(coroutine);
        }

        sem.acquire_many(concurrency).await.unwrap().forget();
        assert_eq!(read_data, write_data);
    }
}
