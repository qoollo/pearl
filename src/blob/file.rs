use crate::prelude::*;

// const WOULDBLOCK_RETRY_INTERVAL_MS: u64 = 10;

#[derive(Debug, Clone)]
pub(crate) struct File {
    ioring: Rio,
    no_lock_fd: Arc<StdFile>, // requires only for read_at/write_at methods
    write_fd: Arc<RwLock<TokioFile>>,
}

impl File {
    pub(crate) async fn open(path: impl AsRef<Path>, ioring: Rio) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path.as_ref())
            .await?;
        Self::from_tokio_file(file, ioring).await
    }

    pub(crate) async fn create(path: impl AsRef<Path>, ioring: Rio) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        Self::from_tokio_file(file, ioring).await
    }

    pub(crate) async fn metadata(&self) -> IOResult<Metadata> {
        self.write_fd.read().await.metadata().await
    }

    // pub(crate) async fn write(&self, buf: &[u8]) -> IOResult<usize> {
    //     let mut file = self.write_fd.write().await;
    //     file.write(buf).await
    // }

    // pub(crate) async fn write_all(&self, buf: &[u8]) -> IOResult<()> {
    //     let mut file = self.write_fd.write().await;
    //     debug!("write all {}b to file", buf.len());
    //     file.write_all(buf).await
    // }

    pub(crate) async fn write_at(&self, buf: &Vec<u8>, offset: u64) -> IOResult<usize> {
        let compl = self.ioring.write_at(&*self.no_lock_fd, buf, offset);
        compl.await
    }

    pub(crate) async fn read_all(&self) -> IOResult<Vec<u8>> {
        let len = self.metadata().await?.len();
        let mut buf = Vec::with_capacity(len as usize);
        self.read_at(&mut buf, 0).await?; // TODO: verify read size
        Ok(buf)
    }

    // pub(crate) async fn read_exact(&self, buf: &mut [u8]) -> IOResult<usize> {
    //     let mut file = self.write_fd.write().await;
    //     file.read_exact(buf).await
    // }

    pub(crate) async fn read_at(&self, buf: &mut Vec<u8>, offset: u64) -> IOResult<usize> {
        let compl = self.ioring.read_at(&*self.no_lock_fd, buf, offset);
        compl.await
    }

    async fn from_tokio_file(file: TokioFile, ioring: Rio) -> IOResult<Self> {
        let tokio_file = file.try_clone().await?;
        let std_file = tokio_file.try_into_std().expect("tokio file into std");
        let file = Self {
            ioring,
            no_lock_fd: Arc::new(std_file),
            write_fd: Arc::new(RwLock::new(file)),
        };
        Ok(file)
    }

    pub(crate) fn from_std_file(fd: StdFile, ioring: Rio) -> IOResult<Self> {
        let file = fd.try_clone()?;
        Ok(Self {
            ioring,
            no_lock_fd: Arc::new(file),
            write_fd: Arc::new(RwLock::new(TokioFile::from_std(fd))),
        })
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        let compl = self.ioring.fdatasync(&*self.no_lock_fd);
        compl.await
    }
}

// #[inline]
// fn schedule_wake(waker: Waker) {
//     tokio::spawn(async move {
//         delay_for(Duration::from_millis(WOULDBLOCK_RETRY_INTERVAL_MS))
//             .map(|_| waker.wake_by_ref())
//             .await;
//     });
// }

// fn warn_and_wake(waker: Waker) {
//     warn!(
//         "file read operation wouldblock or interrupted, retry in {}ms",
//         WOULDBLOCK_RETRY_INTERVAL_MS
//     );
//     schedule_wake(waker);
// }

// struct WriteAt {
//     fd: Arc<StdFile>,
//     buf: Vec<u8>,
//     offset: u64,
// }

// impl<'a> Future for WriteAt {
//     type Output = IOResult<usize>;

//     fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
//         match self.fd.write_at(&self.buf, self.offset) {
//             Err(ref e)
//                 if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
//             {
//                 warn_and_wake(cx.waker().clone());
//                 Poll::Pending
//             }
//             Err(e) => Poll::Ready(Err(e)),
//             Ok(_) => Poll::Ready(Ok(self.buf.len())),
//         }
//     }
// }

// #[derive(Debug)]
// struct ReadAt {
//     fd: Arc<StdFile>,
//     len: usize,
//     offset: u64,
// }

// impl Future for ReadAt {
//     type Output = IOResult<Vec<u8>>;

//     fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
//         trace!("read at poll {:?}", self);
//         let mut buf = vec![0; self.len];
//         match self.fd.read_at(&mut buf, self.offset) {
//             Err(ref e)
//                 if e.kind() == IOErrorKind::WouldBlock || e.kind() == IOErrorKind::Interrupted =>
//             {
//                 warn_and_wake(cx.waker().clone());
//                 Poll::Pending
//             }
//             Err(e) => Poll::Ready(Err(e)),
//             Ok(n) => Poll::Ready(Ok(buf[0..n].to_vec())),
//         }
//     }
// }
