use std::fs;
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures::lock::Mutex;

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
    ) -> Poll<StdResult<usize, io::Error>> {
        let mut file = self.read_fd.as_ref();
        match file.read(buf) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<StdResult<usize, io::Error>> {
        let mut file = self.read_fd.as_ref();
        match file.write_all(buf) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<StdResult<(), io::Error>> {
        unimplemented!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<StdResult<(), io::Error>> {
        unimplemented!()
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<StdResult<u64, io::Error>> {
        let mut file = self.read_fd.as_ref();
        match file.seek(pos) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl File {
    pub(crate) fn metadata(&self) -> io::Result<fs::Metadata> {
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

    pub(crate) fn from_std_file(fd: fs::File) -> Result<Self> {
        Ok(File {
            read_fd: Arc::new(fd.try_clone().map_err(Error::new)?),
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
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
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
            Ok(_t) => Poll::Ready(Ok(buf)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(Error::new(e))),
        }
    }
}
