use std::{io::Write, os::unix::prelude::FileExt};

use super::prelude::*;

#[derive(Debug, Clone)]
pub struct File {
    #[cfg(feature = "aio")]
    ioring: Rio,
    no_lock_fd: Arc<StdFile>, // requires only for read_at/write_at methods
    write_fd: Arc<RwLock<TokioFile>>,
    size: Arc<AtomicU64>,
}

impl File {
    #[cfg(feature = "aio")]
    pub(crate) async fn open(path: impl AsRef<Path>, ioring: Rio) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path.as_ref())
            .await?;
        Self::from_tokio_file(file, ioring).await
    }

    pub(crate) async fn open(path: impl AsRef<Path>) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path.as_ref())
            .await?;
        Self::from_tokio_file(file).await
    }

    #[cfg(feature = "aio")]
    pub(crate) async fn create(path: impl AsRef<Path>, ioring: Rio) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        Self::from_tokio_file(file, ioring).await
    }

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn create(path: impl AsRef<Path>) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        Self::from_tokio_file(file).await
    }

    pub fn size(&self) -> u64 {
        self.size.load(ORD)
    }

    pub(crate) async fn _metadata(&self) -> IOResult<Metadata> {
        self.write_fd.read().await.metadata().await
    }

    #[cfg(feature = "aio")]
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

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn write_append(&self, buf: &[u8]) -> IOResult<usize> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.no_lock_fd.write_at(buf, offset)
    }

    pub(crate) async fn read_all(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0; self.size().try_into()?];
        self.read_at(&mut buf, 0).await?; // TODO: verify read size
        Ok(buf)
    }

    #[cfg(feature = "aio")]
    pub(crate) async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
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
        let mut size = compl.await.with_context(|| "read at failed")?;
        while size < buf.len() {
            debug!(
                "io uring read partial block, trying to read the rest, completed {}/{} bytes",
                size,
                buf.len()
            );
            let slice = buf.split_at_mut(size).1;
            debug!("blob file read at buf to fill up: {}b", slice.len());
            let compl = self
                .ioring
                .read_at(&*self.no_lock_fd, &slice, offset + size as u64);
            let remainder_size = compl.await.with_context(|| "second read at failed")?;
            debug!(
                "blob file read at second read, completed {}/{} of remains bytes",
                remainder_size,
                slice.len()
            );
            if remainder_size == 0 {
                let msg = "blob file read failed, second read returns zero bytes".to_string();
                return Err(Error::io(msg).into());
            }
            size += remainder_size;
            debug!("blob file read at proggress {}/{}", size, buf.len());
        }
        debug!("blob file read at complited: {}", size);
        Ok(size)
    }

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        debug!("blob file read at");
        if buf.is_empty() {
            warn!("file read_at empty buf");
        }
        debug!(
            "blob file read at buf len: {}, offset: {}",
            buf.len(),
            offset
        );
        let count = self.no_lock_fd.read_at(buf, offset)?;
        Ok(count)
    }

    #[cfg(feature = "aio")]
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

    #[cfg(not(feature = "aio"))]
    async fn from_tokio_file(file: TokioFile) -> IOResult<Self> {
        let tokio_file = file.try_clone().await?;
        let size = tokio_file.metadata().await?.len();
        let size = Arc::new(AtomicU64::new(size));
        let std_file = tokio_file.try_into_std().expect("tokio file into std");
        let file = Self {
            no_lock_fd: Arc::new(std_file),
            write_fd: Arc::new(RwLock::new(file)),
            size,
        };
        Ok(file)
    }

    #[cfg(feature = "aio")]
    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        let compl = self.ioring.fdatasync(&*self.no_lock_fd);
        compl.await
    }

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        self.no_lock_fd.as_ref().flush()
    }
}
