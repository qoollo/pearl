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

    pub(crate) async fn write_append(&self, buf: &[u8]) -> IOResult<usize> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        let compl = self.ioring.write_at(&*self.no_lock_fd, &buf, offset);
        let count = compl.await?;
        if count < buf.len() {
            panic!(
                "internal IO error, written bytes: {}, buf len: {}",
                count,
                buf.len()
            );
        }
        Ok(count)
    }

    pub(crate) async fn read_all(&self) -> AnyResult<Vec<u8>> {
        let mut buf = vec![0; self.size().try_into()?];
        self.read_at(&mut buf, 0).await?; // TODO: verify read size
        Ok(buf)
    }

    pub(crate) async fn read_at(&self, buf: &mut [u8], offset: u64) -> AnyResult<usize> {
        debug!("blob file read at");
        if buf.is_empty() {
            warn!("file read_at empty buf");
        }
        debug!(
            "blob file read at buf len: {}, offset: {}",
            buf.len(),
            offset
        );
        let compl = self.ioring.read_at(&*self.no_lock_fd, &buf, offset);
        let size = compl.await.with_context(|| "read at failed")?;
        debug!("blob file read at bytes: {}", size);
        Ok(size)
    }

    async fn from_tokio_file(file: TokioFile, ioring: Rio) -> IOResult<Self> {
        let tokio_file = file.try_clone().await?;
        let size = tokio_file.metadata().await?.len();
        let size = Arc::new(AtomicU64::new(size));
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
        let size = Arc::new(AtomicU64::new(size));
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
}
