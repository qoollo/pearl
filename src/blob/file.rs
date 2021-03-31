use std::os::unix::prelude::FileExt;

use super::prelude::*;

#[derive(Debug, Clone)]
pub struct File {
    ioring: Option<Rio>,
    no_lock_fd: Arc<StdFile>, // requires only for read_at/write_at methods
    write_fd: Arc<RwLock<TokioFile>>,
    size: Arc<AtomicU64>,
}

impl File {
    pub(crate) async fn open(path: impl AsRef<Path>, ioring: Option<Rio>) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path.as_ref())
            .await?;
        Self::from_tokio_file(file, ioring).await
    }

    pub(crate) async fn create(path: impl AsRef<Path>, ioring: Option<Rio>) -> IOResult<Self> {
        let file = OpenOptions::new()
            .create(true)
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
        if let Some(ref ioring) = self.ioring {
            self.write_append_aio(buf, ioring).await
        } else {
            self.write_append_sync(buf).await
        }
    }

    pub(crate) async fn write_append_sync(&self, buf: &[u8]) -> IOResult<usize> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.write_at_sync(offset, buf).await
    }

    async fn write_append_aio(&self, buf: &[u8], ioring: &Rio) -> IOResult<usize> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.write_at_aio(offset, buf, ioring).await
    }

    pub(crate) async fn write_at(&self, offset: u64, buf: &[u8]) -> IOResult<usize> {
        if let Some(ref ioring) = self.ioring {
            self.write_at_aio(offset, buf, ioring).await
        } else {
            self.write_at_sync(offset, buf).await
        }
    }

    async fn write_at_aio(&self, offset: u64, buf: &[u8], ioring: &Rio) -> IOResult<usize> {
        let compl = ioring.write_at(&*self.no_lock_fd, &buf, offset);
        let count = compl.await?;
        // on blob level this error will be treated as unavailable file (rio doesn't return error
        // in explicit format so we do that for it)
        // P.S.: previous solution was to panic, where this situation appears, seems like this solution
        // is a little better (for example, in case of cluster which contains few pearl storages it
        // won't break the whole cluster, but only pass information outside, that last write wasn't sucessful)
        if count < buf.len() {
            return Err(IOError::from_raw_os_error(5));
        }
        Ok(count)
    }

    async fn write_at_sync(&self, offset: u64, buf: &[u8]) -> IOResult<usize> {
        let buf = buf.to_vec();
        let file = self.no_lock_fd.clone();
        Self::blocking_call(move || file.write_at(&buf, offset)).await
    }

    pub(crate) async fn read_all(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0; self.size().try_into()?];
        self.read_at(&mut buf, 0).await?; // TODO: verify read size
        Ok(buf)
    }

    pub(crate) async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if let Some(ref ioring) = self.ioring {
            self.read_at_aio(buf, offset, ioring).await
        } else {
            self.read_at_sync(buf, offset).await
        }
    }

    //pub(crate) async fn partial_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    //if let Some(ref ioring) = self.ioring {
    //self.partial_read_at_aio(buf, offset, ioring).await
    //} else {
    //self.partial_read_at_sync(buf, offset).await
    //}
    //}

    pub(crate) async fn read_at_sync(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let file = self.no_lock_fd.clone();
        let mut new_buf = buf.to_vec();

        let (count, new_buf) = Self::blocking_call(move || {
            file.read_at(&mut new_buf, offset)
                .map(|count| (count, new_buf))
        })
        .await?;
        buf.clone_from_slice(new_buf.as_slice());
        Ok(count)
    }

    async fn read_at_aio(&self, buf: &mut [u8], offset: u64, ioring: &Rio) -> Result<usize> {
        debug!("blob file read at");
        if buf.is_empty() {
            warn!("file read_at empty buf");
        }
        debug!(
            "blob file read at buf len: {}, offset: {}",
            buf.len(),
            offset
        );
        let compl = ioring.read_at(&*self.no_lock_fd, &buf, offset);
        let mut size = compl.await.with_context(|| "read at failed")?;
        while size < buf.len() {
            debug!(
                "io uring read partial block, trying to read the rest, completed {}/{} bytes",
                size,
                buf.len()
            );
            let slice = buf.split_at_mut(size).1;
            debug!("blob file read at buf to fill up: {}b", slice.len());
            let compl = ioring.read_at(&*self.no_lock_fd, &slice, offset + size as u64);
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

    async fn from_tokio_file(file: TokioFile, ioring: Option<Rio>) -> IOResult<Self> {
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

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref ioring) = self.ioring {
            let compl = ioring.fsync(&*self.no_lock_fd);
            compl.await
        } else {
            self.write_fd.read().await.sync_all().await
        }
    }

    async fn blocking_call<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::spawn_blocking(move || f())
            .await
            .expect("spawned blocking task failed")
    }
}
