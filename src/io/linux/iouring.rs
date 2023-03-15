use super::sync::{File as SyncFile, IODriver as SyncDriver};
use crate::prelude::*;
use bytes::{Bytes, BytesMut};
use rio::Rio;
use std::time::SystemTime;

/// IO driver for file operations with optional async support
#[derive(Debug, Clone)]
pub struct IODriver {
    rio: Option<Arc<Rio>>,
    sync_driver: SyncDriver,
}

impl IODriver {
    /// Create new iodriver with optional async support
    pub fn new(rio: Option<Rio>) -> Self {
        Self {
            rio: rio.map(Arc::new),
            sync_driver: SyncDriver::default(),
        }
    }

    pub(crate) async fn open(&self, path: impl AsRef<Path>) -> IOResult<File> {
        let sync = self.sync_driver.open(path).await?;
        File::from_file(sync, self.rio.clone()).await
    }

    pub(crate) async fn create(&self, path: impl AsRef<Path>) -> IOResult<File> {
        let sync = self.sync_driver.create(path).await?;
        File::from_file(sync, self.rio.clone()).await
    }
}

impl Default for IODriver {
    fn default() -> Self {
        let rio = rio::new().ok();
        Self::new(rio)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct File {
    sync: SyncFile,
    rio: Option<Arc<Rio>>,
}

impl File {
    pub(crate) fn size(&self) -> u64 {
        self.sync.size()
    }

    pub(crate) async fn write_append_all(&self, buf: Bytes) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            let offset = self.sync.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
            self.write_all_at_aio(offset, buf, rio).await
        } else {
            self.sync.write_append_all(buf).await
        }
    }

    pub(crate) async fn write_append_all_buffers(
        &self,
        first_buf: Bytes,
        second_buf: Bytes,
    ) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            let first_len = first_buf.len() as u64;
            let second_len = second_buf.len() as u64;
            let mut offset = self
                .sync
                .size
                .fetch_add(first_len + second_len, Ordering::SeqCst);
            self.write_all_at_aio(offset, first_buf, rio).await?;
            offset = offset + first_len;
            self.write_all_at_aio(offset, second_buf, rio).await
        } else {
            self.sync
                .write_append_all_buffers(first_buf, second_buf)
                .await
        }
    }

    pub(crate) async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            self.write_all_at_aio(offset, buf, rio).await
        } else {
            self.sync.write_all_at(offset, buf).await
        }
    }

    pub(crate) async fn read_all(&self) -> Result<BytesMut> {
        self.read_exact_at_allocate(self.size().try_into()?, 0)
            .await
    }

    pub(crate) async fn read_exact_at_allocate(
        &self,
        size: usize,
        offset: u64,
    ) -> Result<BytesMut> {
        let buf = BytesMut::zeroed(size);
        self.read_exact_at(buf, offset).await
    }

    pub(crate) async fn read_exact_at(&self, mut buf: BytesMut, offset: u64) -> Result<BytesMut> {
        debug!("blob file read at");
        if buf.is_empty() {
            warn!("file read_at empty buf");
        }
        debug!(
            "blob file read at buf len: {}, offset: {}",
            buf.len(),
            offset
        );
        if let Some(ref rio) = self.rio {
            let compl = rio.read_at(&*self.sync.no_lock_fd, &buf, offset);
            let mut size = compl.await.with_context(|| "read at failed")?;
            while size < buf.len() {
                debug!(
                    "io uring read partial block, trying to read the rest, completed {}/{} bytes",
                    size,
                    buf.len()
                );
                let slice = buf.split_at_mut(size).1;
                debug!("blob file read at buf to fill up: {}b", slice.len());
                let compl = rio.read_at(&*self.sync.no_lock_fd, &slice, offset + size as u64);
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
            Ok(buf)
        } else {
            self.sync.read_exact_at(buf, offset).await
        }
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            rio.fsync(&*self.sync.no_lock_fd).await
        } else {
            self.sync.fsyncdata().await
        }
    }

    pub(crate) fn created_at(&self) -> Result<SystemTime> {
        self.sync.created_at()
    }

    async fn write_all_at_aio(&self, offset: u64, buf: Bytes, rio: &Rio) -> IOResult<()> {
        let compl = rio.write_at(&*self.sync.no_lock_fd, &buf, offset);
        let count = compl.await?;
        // on blob level this error will be treated as unavailable file (rio doesn't return error
        // in explicit format so we do that for it)
        // P.S.: previous solution was to panic, where this situation appears, seems like this solution
        // is a little better (for example, in case of cluster which contains few pearl storages it
        // won't break the whole cluster, but only pass information outside, that last write wasn't sucessful)
        if count < buf.len() {
            return Err(IOError::from_raw_os_error(5));
        }
        Ok(())
    }

    async fn from_file(sync: SyncFile, rio: Option<Arc<Rio>>) -> IOResult<Self> {
        Ok(Self { sync, rio })
    }
}
