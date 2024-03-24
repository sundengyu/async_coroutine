use std::{
    fs::File,
    io::{Error, ErrorKind, Read},
    mem,
    os::fd::FromRawFd,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{unix::AsyncFd, AsyncRead, ReadBuf};

pub(super) struct EventFd(AsyncFd<i32>);

impl EventFd {
    pub(super) fn new(init: u32) -> Result<Self, Error> {
        let flags = libc::EFD_NONBLOCK | libc::EFD_CLOEXEC;
        let fd = unsafe { libc::eventfd(init, flags) };
        if fd < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(Self(AsyncFd::new(fd)?))
        }
    }

    pub(super) fn raw_fd(&self) -> i32 {
        self.0.get_ref().to_owned()
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        if unsafe { libc::close(self.raw_fd()) } < 0 {
            panic!(
                "unexpected error when closing eventfd, {}",
                Error::last_os_error()
            );
        }
    }
}

impl AsyncRead for EventFd {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), Error>> {
        loop {
            match self.0.poll_read_ready(cx)? {
                Poll::Ready(guard) => {
                    let unfilled = buf.initialize_unfilled();
                    let mut f = unsafe { File::from_raw_fd(*guard.get_inner()) };
                    let res = match f.read(unfilled) {
                        Ok(nread) => {
                            assert!(nread == 8);
                            buf.advance(nread);
                            Ok(())
                        }
                        Err(err) => match err.kind() {
                            ErrorKind::WouldBlock => continue,
                            _ => Err(err),
                        },
                    };
                    mem::forget(f);
                    return Poll::Ready(res);
                }
                Poll::Pending => return Poll::Pending,
            };
        }
    }
}
