use std::{io::Write, os::unix::prelude::FileExt};

use tokio::sync::oneshot::channel;

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
        let file = self.no_lock_fd.clone();
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        let buf = buf.to_vec();
        Self::blocking_call(move || file.write_at(&buf, offset)).await
    }

    async fn write_append_aio(&self, buf: &[u8], ioring: &Rio) -> IOResult<usize> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        let compl = ioring.write_at(&*self.no_lock_fd, &buf, offset);
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
            let compl = ioring.fdatasync(&*self.no_lock_fd);
            compl.await
        } else {
            let file = self.no_lock_fd.clone();
            Self::blocking_call(move || file.as_ref().flush()).await
        }
    }

    async fn blocking_call<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = channel();
        tokio::task::spawn_blocking(move || {
            let res = f();
            tx.send(res)
        });
        rx.await.expect("spawned blocking task failed")
    }
}
