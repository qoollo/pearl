use crate::prelude::*;

const WOULDBLOCK_RETRY_INTERVAL_MS: u64 = 10;

#[derive(Debug, Clone)]
pub(crate) struct File {
    read_fd: Arc<StdFile>,
    write_fd: Arc<RwLock<TokioFile>>,
}

impl File {
    pub(crate) async fn open(path: impl AsRef<Path>) -> IOResult<Self> {
        let file = TokioOpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path.as_ref())
            .await?;
        Self::from_tokio_file(file).await
    }

    pub(crate) async fn create(path: impl AsRef<Path>) -> IOResult<Self> {
        let file = TokioOpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        Self::from_tokio_file(file).await
    }
    pub(crate) fn metadata(&self) -> IOResult<std::fs::Metadata> {
        self.read_fd.metadata()
    }

    pub(crate) async fn write(&self, buf: &[u8]) -> IOResult<usize> {
        let mut file = self.write_fd.write().await;
        file.write(buf).await
    }

    pub(crate) async fn write_all(&self, buf: &[u8]) -> IOResult<()> {
        let mut file = self.write_fd.write().await;
        file.write_all(buf).await
    }

    pub(crate) async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IOResult<usize> {
        let fd = self.read_fd.clone();
        let write_fut = WriteAt { fd, buf, offset };
        write_fut.await
    }

    pub(crate) async fn read_exact(&self, buf: &mut [u8]) -> IOResult<usize> {
        let mut file = self.write_fd.write().await;
        file.read_exact(buf).await
    }

    pub(crate) async fn read_to_end(&self, buf: &mut Vec<u8>) -> IOResult<usize> {
        let mut file = self.write_fd.write().await;
        file.read_to_end(buf).await
    }

    pub(crate) async fn read_at(&self, len: usize, offset: u64) -> IOResult<Vec<u8>> {
        let read_fut = ReadAt {
            fd: self.read_fd.clone(),
            len,
            offset,
        };
        read_fut.await
    }

    pub(crate) async fn seek(&self, from: SeekFrom) -> IOResult<u64> {
        let mut file = self.write_fd.write().await;
        file.seek(from).await
    }

    async fn from_tokio_file(file: TokioFile) -> IOResult<Self> {
        let tokio_file = file.try_clone().await?;
        let std_file = tokio_file.try_into_std().expect("tokio file into std");
        let file = Self {
            read_fd: Arc::new(std_file),
            write_fd: Arc::new(RwLock::new(file)),
        };
        Ok(file)
    }

    pub(crate) fn from_std_file(fd: StdFile) -> IOResult<Self> {
        let file = fd.try_clone()?;
        Ok(Self {
            read_fd: Arc::new(file),
            write_fd: Arc::new(RwLock::new(TokioFile::from_std(fd))),
        })
    }

    pub(crate) async fn fsync(&self) {
        todo!()
    }
}

#[inline]
fn schedule_wake(waker: Waker) {
    tokio::spawn(async move {
        delay_for(Duration::from_millis(WOULDBLOCK_RETRY_INTERVAL_MS))
            .map(|_| waker.wake_by_ref())
            .await;
    });
}

fn warn_and_wake(waker: Waker) {
    warn!(
        "file read operation wouldblock or interrupted, retry in {}ms",
        WOULDBLOCK_RETRY_INTERVAL_MS
    );
    schedule_wake(waker);
}

struct WriteAt {
    fd: Arc<StdFile>,
    buf: Vec<u8>,
    offset: u64,
}

impl<'a> Future for WriteAt {
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
    fd: Arc<StdFile>,
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
