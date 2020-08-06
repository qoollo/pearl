use super::prelude::*;

#[derive(Debug, Clone)]
pub struct File {
    ioring: Rio,
    no_lock_fd: Arc<StdFile>, // requires only for read_at/write_at methods
    write_fd: Arc<RwLock<TokioFile>>,
    size: Arc<AtomicU64>,
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

    pub fn size(&self) -> u64 {
        self.size.load(ORD)
    }

    pub(crate) async fn _metadata(&self) -> IOResult<Metadata> {
        self.write_fd.read().await.metadata().await
    }

    pub(crate) async fn write_at(&self, buf: &[u8], offset: u64) -> IOResult<usize> {
        let compl = self.ioring.write_at(&*self.no_lock_fd, &buf, offset);
        let add_len = compl.await?;
        self.size
            .fetch_max(offset + add_len as u64, Ordering::SeqCst);
        Ok(add_len)
    }

    pub(crate) async fn read_all(&self) -> AnyResult<Vec<u8>> {
        let mut buf = vec![0; self.size() as usize];
        self.read_at(&mut buf, 0).await?; // TODO: verify read size
        Ok(buf)
    }

    pub(crate) async fn read_at(&self, buf: &mut [u8], offset: u64) -> AnyResult<usize> {
        trace!("file read at");
        if buf.is_empty() {
            warn!("file read_at empty buf");
        }
        let compl = self.ioring.read_at(&*self.no_lock_fd, &buf, offset);
        trace!("io uring completion created");
        let size = compl.await.with_context(|| "read at failed")?;
        trace!("read finished: {} bytes", size);
        Ok(size)
    }

    async fn from_tokio_file(file: TokioFile, ioring: Rio) -> IOResult<Self> {
        let tokio_file = file.try_clone().await?;
        let size = tokio_file.metadata().await?.len();
        let size = Self::size_from_u64(size);
        let std_file = tokio_file.try_into_std().expect("tokio file into std");
        let file = Self {
            ioring,
            no_lock_fd: Arc::new(std_file),
            write_fd: Arc::new(RwLock::new(file)),
            size,
        };
        Ok(file)
    }

    pub(crate) fn from_std_file(fd: StdFile, ioring: Rio) -> IOResult<Self> {
        let file = fd.try_clone()?;
        let size = file.metadata()?.len();
        let size = Self::size_from_u64(size);
        let file = Self {
            ioring,
            no_lock_fd: Arc::new(file),
            write_fd: Arc::new(RwLock::new(TokioFile::from_std(fd))),
            size,
        };
        Ok(file)
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        let compl = self.ioring.fdatasync(&*self.no_lock_fd);
        compl.await
    }

    fn size_from_u64(len: u64) -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(len))
    }
}
