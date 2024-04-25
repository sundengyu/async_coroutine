use super::{aio::*, eventfd::EventFd};
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use std::{
    io::{Error, ErrorKind},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::mpsc::*, task::JoinHandle};

unsafe fn io_setup() -> Result<aio_context_t, Error> {
    let mut io_ctx: aio_context_t = 0;
    let res = libc::syscall(
        libc::SYS_io_setup,
        4096 as libc::c_long,
        &mut io_ctx as *mut aio_context_t,
    );

    if res != 0 {
        Err(Error::last_os_error())
    } else {
        Ok(io_ctx)
    }
}

unsafe fn io_destroy(io_ctx: aio_context_t) -> Result<(), Error> {
    if libc::syscall(libc::SYS_io_destroy, io_ctx) != 0 {
        Err(Error::last_os_error())
    } else {
        Ok(())
    }
}

unsafe fn io_submit(io_ctx: aio_context_t, iocbs: &[iocb]) -> Result<(), Error> {
    let iocb_ptrs: Vec<_> = iocbs.iter().map(|iocb| iocb as *const iocb).collect();
    let mut nsubmitted = 0;
    while nsubmitted < iocb_ptrs.len() {
        let ret = libc::syscall(
            libc::SYS_io_submit,
            io_ctx,
            iocb_ptrs.len() as libc::c_long,
            iocb_ptrs.as_ptr(),
        );
        if ret > 0 {
            nsubmitted += ret as usize;
            continue;
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted && err.kind() != ErrorKind::WouldBlock {
            return Err(err);
        }
    }

    Ok(())
}

unsafe fn io_getevents(io_ctx: aio_context_t, ncompleted: usize) -> Result<Vec<io_event>, Error> {
    let mut events: Vec<io_event> = Vec::with_capacity(ncompleted);
    let mut nreaped = 0;
    while nreaped < ncompleted {
        let ret = libc::syscall(
            libc::SYS_io_getevents,
            io_ctx,
            1,
            (ncompleted - nreaped) as u64,
            events.as_mut_ptr().add(nreaped),
            std::ptr::null_mut::<libc::timespec>(),
        );

        if ret > 0 {
            nreaped += ret as usize;
            continue;
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted && err.kind() != ErrorKind::WouldBlock {
            return Err(err);
        }
    }

    events.set_len(ncompleted);
    Ok(events)
}

#[derive(Default)]
struct IoContent {
    data_ptr: usize,
    offset: u64,
    size: u64,
}

enum IoType {
    Read,
    Write,
    Sync,
}

struct AioCallback {
    io_type: IoType,
    io_content: IoContent,
    arg: *mut libc::c_void,
}

unsafe impl Send for AioCallback {}
unsafe impl Sync for AioCallback {}

struct AioContex {
    io_fd: i32,
    io_ctx: aio_context_t,
    eventfd: EventFd,
    nr_requests: u32,
    inflight_reqs: u32,
    receiver: Option<UnboundedReceiver<AioCallback>>,
    cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
}

impl AioContex {
    fn new(
        io_fd: i32,
        nr_requests: u32,
        receiver: UnboundedReceiver<AioCallback>,
        cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
    ) -> Result<Self, Error> {
        let io_ctx = unsafe { io_setup()? };
        let eventfd = EventFd::new(0)?;
        Ok(Self {
            io_fd,
            io_ctx,
            eventfd,
            nr_requests,
            inflight_reqs: 0,
            receiver: Some(receiver),
            cb,
        })
    }

    fn submit_tasks(&mut self, tasks: Vec<AioCallback>) {
        self.inflight_reqs += tasks.len() as u32;
        let iocbs: Vec<iocb> = tasks
            .into_iter()
            .map(|aio_cb| {
                let opcode = match aio_cb.io_type {
                    IoType::Read => IOCB_CMD_PREAD,
                    IoType::Write => IOCB_CMD_PWRITE,
                    IoType::Sync => IOCB_CMD_FSYNC,
                };

                let mut iocb = iocb {
                    aio_data: 0,
                    aio_key: 0,
                    aio_rw_flags: 0,
                    aio_lio_opcode: opcode as u16,
                    aio_reqprio: 0,
                    aio_fildes: self.io_fd as u32,
                    aio_buf: aio_cb.io_content.data_ptr as u64,
                    aio_nbytes: aio_cb.io_content.size,
                    aio_offset: aio_cb.io_content.offset as i64,
                    aio_reserved2: 0,
                    aio_flags: IOCB_FLAG_RESFD,
                    aio_resfd: self.eventfd.raw_fd() as u32,
                };
                iocb.aio_data = aio_cb.arg as u64;

                iocb
            })
            .collect();
        unsafe { io_submit(self.io_ctx, &iocbs).unwrap() };
    }

    fn reap_tasks(&mut self, ncompleted: u64) {
        let completions = unsafe { io_getevents(self.io_ctx, ncompleted as usize).unwrap() };
        assert_eq!(ncompleted, completions.len() as u64);
        for completion in completions {
            unsafe { (self.cb)(completion.data as *mut libc::c_void, completion.res) };
            self.inflight_reqs -= 1;
        }
    }

    async fn recv_many(
        receiver: &mut UnboundedReceiver<AioCallback>,
        limit: usize,
    ) -> Result<Vec<AioCallback>, ()> {
        let mut tasks = Vec::with_capacity(limit);
        let nrecv = receiver.recv_many(&mut tasks, limit).await;
        if nrecv == 0 {
            Err(())
        } else {
            Ok(tasks)
        }
    }

    async fn submit_and_reap_tasks(&mut self) -> Result<(), ()> {
        let mut receiver = self.receiver.take().unwrap();
        loop {
            let limit = self.nr_requests - self.inflight_reqs;
            if self.inflight_reqs == 0 {
                let tasks = Self::recv_many(&mut receiver, limit as usize).await?;
                self.submit_tasks(tasks);
            } else if self.inflight_reqs < self.nr_requests {
                tokio::select! {
                    tasks = Self::recv_many(&mut receiver, limit as usize) => {
                        self.submit_tasks(tasks?);
                    }
                    ncompleted = self.eventfd.read_events() => {
                        self.reap_tasks(ncompleted.unwrap());
                    }
                }
            } else {
                let ncompleted = self.eventfd.read_events().await.unwrap();
                self.reap_tasks(ncompleted);
            }
        }
    }
}

impl Drop for AioContex {
    fn drop(&mut self) {
        unsafe { io_destroy(self.io_ctx).unwrap() }
    }
}

struct AioHandle {
    sender: UnboundedSender<AioCallback>,
    task_handle: JoinHandle<()>,
}

static FD_CONTEXT_MAP: OnceCell<DashMap<i32, Arc<AioHandle>>> = OnceCell::new();

pub(crate) fn register_fd(
    fd: i32,
    nr_requests: u32,
    cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
) {
    let (sender, receiver) = unbounded_channel();
    let mut aio_context = AioContex::new(fd, nr_requests, receiver, cb).unwrap();
    let task_handle = tokio::spawn(async move {
        let _ = aio_context.submit_and_reap_tasks().await;
    });
    let handle = Arc::new(AioHandle {
        sender,
        task_handle,
    });
    FD_CONTEXT_MAP.get_or_init(DashMap::new).insert(fd, handle);
}

pub(crate) async fn unregister_fd(fd: i32) {
    if let Some((_, mut aio_ctx)) = FD_CONTEXT_MAP.get().unwrap().remove(&fd) {
        while Arc::strong_count(&aio_ctx) > 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        aio_ctx.task_handle.abort();
        let handle = &mut Arc::get_mut(&mut aio_ctx).unwrap().task_handle;
        handle.await.unwrap_err();
    }
}

pub(crate) fn submit_read(fd: i32, offset: u64, buf: *mut u8, size: u64, arg: *mut libc::c_void) {
    let ctx = FD_CONTEXT_MAP.get().unwrap().get(&fd).unwrap().clone();
    let io_content = IoContent {
        data_ptr: buf as usize,
        offset,
        size,
    };
    ctx.sender
        .send(AioCallback {
            io_type: IoType::Read,
            io_content,
            arg,
        })
        .unwrap();
}

pub(crate) fn submit_write(
    fd: i32,
    offset: u64,
    buf: *const u8,
    size: u64,
    arg: *mut libc::c_void,
) {
    let ctx = FD_CONTEXT_MAP.get().unwrap().get(&fd).unwrap().clone();
    let io_content = IoContent {
        data_ptr: buf as usize,
        offset,
        size,
    };
    ctx.sender
        .send(AioCallback {
            io_type: IoType::Write,
            io_content,
            arg,
        })
        .unwrap();
}

pub(crate) fn submit_fsync(fd: i32, arg: *mut libc::c_void) {
    let ctx = FD_CONTEXT_MAP.get().unwrap().get(&fd).unwrap().clone();
    let io_content = IoContent::default();
    ctx.sender
        .send(AioCallback {
            io_type: IoType::Sync,
            io_content,
            arg,
        })
        .unwrap();
}
