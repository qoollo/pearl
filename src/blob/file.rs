use crate::prelude::*;

use super::core::{Error, Result};

#[derive(Debug, Clone)]
pub(crate) struct File {
    pub(crate) read_fd: Arc<fs::File>,
    pub(crate) write_fd: Arc<Mutex<fs::File>>,
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IOResult<usize>> {
        let mut file = self.read_fd.as_ref();
        match file.read(buf) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == IOErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IOResult<usize>> {
        let mut file = self.read_fd.as_ref();
        match file.write_all(buf) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(ref e) if e.kind() == IOErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IOResult<()>> {
        unimplemented!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IOResult<()>> {
        unimplemented!()
    }
}

impl AsyncSeek for File {
    fn poll_seek(self: Pin<&mut Self>, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<IOResult<u64>> {
        let mut file = self.read_fd.as_ref();
        match file.seek(pos) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == IOErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl File {
    pub(crate) fn metadata(&self) -> IOResult<fs::Metadata> {
        self.read_fd.metadata()
    }

    pub(crate) async fn write_at(&mut self, buf: Vec<u8>, offset: u64) -> Result<usize> {
        let mut fd = self.write_fd.lock().await;
        let write_fut = WriteAt {
            fd: &mut fd,
            buf,
            offset,
        };
        write_fut.await
    }

    pub(crate) async fn read_at(&self, len: usize, offset: u64) -> Result<Vec<u8>> {
        let read_fut = ReadAt {
            fd: self.read_fd.clone(),
            len,
            offset,
        };
        read_fut.await
    }

    pub(crate) fn from_std_file(fd: fs::File) -> IOResult<Self> {
        fd.try_clone().map(|file| File {
            read_fd: Arc::new(file),
            write_fd: Arc::new(Mutex::new(fd)),
        })
    }
}

struct WriteAt<'a> {
    fd: &'a mut fs::File,
    buf: Vec<u8>,
    offset: u64,
}

impl<'a> Future for WriteAt<'a> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.fd.write_at(&self.buf, self.offset) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == IOErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(Error::new(e))),
        }
    }
}

struct ReadAt {
    fd: Arc<fs::File>,
    len: usize,
    offset: u64,
}

impl Future for ReadAt {
    type Output = Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut buf = vec![0; self.len];
        match self.fd.read_at(&mut buf, self.offset) {
            Ok(t) => {
                if t == self.len {
                    Poll::Ready(Ok(buf))
                } else {
                    Poll::Ready(Err(Error::from(IOError::from(IOErrorKind::UnexpectedEof))))
                }
            }
            Err(ref e) if e.kind() == IOErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(Error::new(e))),
        }
    }
}
