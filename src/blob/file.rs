use crate::prelude::*;

const WOULDBLOCK_RETRY_INTERVAL_MS: u64 = 10;

#[derive(Debug, Clone)]
pub(crate) struct File {
    pub(crate) no_lock_fd: Arc<fs::File>, // requires for async implementations of read_at/write_at
    pub(crate) locked_fd: Arc<Mutex<fs::File>>,
}

#[inline]
fn schedule_wake(waker: Waker) {
    tokio::spawn(async move {
        delay_for(Duration::from_millis(WOULDBLOCK_RETRY_INTERVAL_MS))
            .map(|_| waker.wake_by_ref())
            .await;
    });
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IOResult<usize>> {
        let mut file_ref = Pin::get_ref(self.as_ref());
        let pinned_file_ref = Pin::new(&mut file_ref);
        pinned_file_ref.poll_read(cx, buf)
    }
}

fn warn_and_wake(waker: Waker) {
    warn!(
        "file read operation wouldblock or interrupted, retry in {}ms",
        WOULDBLOCK_RETRY_INTERVAL_MS
    );
    schedule_wake(waker);
}

impl AsyncRead for &File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IOResult<usize>> {
        let mut file = self.no_lock_fd.as_ref();
        match file.read(buf) {
            Err(ref e)
                if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
            {
                warn_and_wake(cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
            Ok(n) => Poll::Ready(Ok(n)),
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IOResult<usize>> {
        let mut file = self.no_lock_fd.as_ref();
        match file.write_all(buf) {
            Err(ref e)
                if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
            {
                warn_and_wake(cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
            Ok(_) => Poll::Ready(Ok(buf.len())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IOResult<()>> {
        let mut file = self.no_lock_fd.as_ref();
        match file.flush() {
            Err(ref e)
                if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
            {
                warn_and_wake(cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
            Ok(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IOResult<()>> {
        self.poll_flush(cx)
    }
}

impl File {
    pub(crate) fn metadata(&self) -> IOResult<fs::Metadata> {
        self.no_lock_fd.metadata()
    }

    pub(crate) async fn write_at(&mut self, buf: Vec<u8>, offset: u64) -> IOResult<usize> {
        let mut fd = self.locked_fd.lock().await;
        let write_fut = WriteAt {
            fd: &mut fd,
            buf,
            offset,
        };
        write_fut.await
    }

    pub(crate) async fn read_at(&self, len: usize, offset: u64) -> IOResult<Vec<u8>> {
        let read_fut = ReadAt {
            fd: self.no_lock_fd.clone(),
            len,
            offset,
        };
        read_fut.await
    }

    pub(crate) async fn read_all(&self) -> IOResult<Vec<u8>> {
        let len = self.metadata()?.len(); // @TODO remove syncronous call. Will be removed after std file replacement with tokio file.
        self.read_at(len.try_into().expect("u64 to usize"), 0).await
    }

    pub(crate) fn from_std_file(fd: fs::File) -> IOResult<Self> {
        fd.try_clone().map(|file| Self {
            no_lock_fd: Arc::new(file),
            locked_fd: Arc::new(Mutex::new(fd)),
        })
    }
}

struct WriteAt<'a> {
    fd: &'a mut fs::File,
    buf: Vec<u8>,
    offset: u64,
}

impl<'a> Future for WriteAt<'a> {
    type Output = IOResult<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.fd.write_at(&self.buf, self.offset) {
            Err(ref e)
                if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
            {
                warn_and_wake(cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
            Ok(_) => Poll::Ready(Ok(self.buf.len())),
        }
    }
}

#[derive(Debug)]
struct ReadAt {
    fd: Arc<fs::File>,
    len: usize,
    offset: u64,
}

impl Future for ReadAt {
    type Output = IOResult<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        trace!("read at poll {:?}", self);
        let mut buf = vec![0; self.len];
        match self.fd.read_at(&mut buf, self.offset) {
            Err(ref e)
                if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
            {
                warn_and_wake(cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
            Ok(n) => Poll::Ready(Ok(buf[0..n].to_vec())),
        }
    }
}
