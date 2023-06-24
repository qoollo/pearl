use super::sync::{File as SyncFile, IoDriver as SyncDriver};
use crate::prelude::*;
use bytes::{Bytes, BytesMut};
use rio::Rio;
use std::time::SystemTime;

/// IO driver for file operations with optional async support
#[derive(Debug, Clone)]
pub struct IoDriver {
    rio: Option<Rio>,
    sync_driver: SyncDriver,
}

impl IoDriver {
    /// Create new iodriver with async if available
    pub fn new() -> Self {
        Self::new_async_fallback_sync()
    }

    /// Create new sync iodriver
    pub fn new_sync() -> Self {
        Self {
            rio: None,
            sync_driver: SyncDriver::new_sync(),
        }
    }

    /// Create new async iodriver
    pub fn new_async() -> Result<Self> {
        rio::new().map(Self::new_async_rio).map_err(|e| e.into())
    }

    /// Create new async iodriver from existing rio instance
    pub fn new_async_rio(rio: Rio) -> Self {
        Self {
            rio: Some(rio),
            sync_driver: SyncDriver::new_sync(),
        }
    }

    /// Create new iodriver with async if available    
    pub fn new_async_fallback_sync() -> Self {
        if let Ok(rio) = rio::new() {
            Self::new_async_rio(rio)
        } else {
            Self::new_sync()
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

#[derive(Debug, Clone)]
pub(crate) struct File {
    sync: SyncFile,
    rio: Option<Rio>,
}

impl File {
    pub(crate) fn size(&self) -> u64 {
        self.sync.size()
    }

    pub(crate) fn dirty_bytes(&self) -> u64 {
        self.sync.dirty_bytes()
    }

    pub(crate) async fn write_append_writable_data<R: Send + 'static>(
        &self,
        c: impl WritableDataCreator<R>,
    ) -> IOResult<R> {
        if let Some(ref rio) = self.rio {
            let len = c.len();
            let mut offset = self.sync.file_size_append(len);
            let (res, data) = c.create(offset);
            match res {
                WritableData::Single(bytes) => self.write_all_at_aio(offset, bytes, rio).await?,
                WritableData::Double(b1, b2) => {
                    let first_len = b1.len() as u64;
                    self.write_all_at_aio(offset, b1, rio).await?;
                    offset = offset + first_len;
                    self.write_all_at_aio(offset, b2, rio).await?;
                }
            }
            Ok(data)
        } else {
            self.sync.write_append_writable_data(c).await
        }
    }

    pub(crate) async fn write_append_all(&self, buf: Bytes) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            let offset = self.sync.file_size_append(buf.len() as u64);
            self.write_all_at_aio(offset, buf, rio).await
        } else {
            self.sync.write_append_all(buf).await
        }
    }

    pub(crate) async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        debug_assert!(offset + buf.len() as u64 <= self.size());
        if let Some(ref rio) = self.rio {
            self.write_all_at_aio(offset, buf, rio).await
        } else {
            self.sync.write_all_at(offset, buf).await
        }
    }

    pub(crate) async fn read_all(&self) -> IOResult<BytesMut> {
        self.read_exact_at_allocate(self.size().try_into().expect("File is too large"), 0)
            .await
    }

    pub(crate) async fn read_exact_at_allocate(
        &self,
        size: usize,
        offset: u64,
    ) -> IOResult<BytesMut> {
        let buf = BytesMut::zeroed(size);
        self.read_exact_at(buf, offset).await
    }

    pub(crate) async fn read_exact_at(&self, buf: BytesMut, offset: u64) -> IOResult<BytesMut> {
        debug!("File read at buf len: {}, offset: {}", buf.len(), offset);
        if let Some(ref rio) = self.rio {
            self.read_exact_at_aio(buf, offset, rio).await
        } else {
            self.sync.read_exact_at(buf, offset).await
        }
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            let size = self.size();
            rio.fsync(self.sync.std_file_ref()).await?;
            self.sync.set_synced_size(size);
            Ok(())
        } else {
            self.sync.fsyncdata().await
        }
    }

    pub(crate) fn created_at(&self) -> IOResult<SystemTime> {
        self.sync.created_at()
    }

    async fn write_all_at_aio(&self, offset: u64, buf: Bytes, rio: &Rio) -> IOResult<()> {
        let compl = rio.write_at(self.sync.std_file_ref(), &buf, offset);
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

    async fn read_exact_at_aio(
        &self,
        mut buf: BytesMut,
        offset: u64,
        rio: &Rio,
    ) -> IOResult<BytesMut> {
        let compl = rio.read_at(self.sync.std_file_ref(), &buf, offset);
        let mut size = compl.await?;
        while size < buf.len() {
            debug!(
                "io uring read partial block, trying to read the rest, completed {}/{} bytes",
                size,
                buf.len()
            );
            let slice = buf.split_at_mut(size).1;
            debug!("blob file read at buf to fill up: {}b", slice.len());
            let compl = rio.read_at(self.sync.std_file_ref(), &slice, offset + size as u64);
            let remainder_size = compl.await?;
            debug!(
                "blob file read at second read, completed {}/{} of remains bytes",
                remainder_size,
                slice.len()
            );
            if remainder_size == 0 {
                return Err(IOError::new(IOErrorKind::UnexpectedEof, "failed to fill whole buffer"));
            }
            size += remainder_size;
            debug!("blob file read at proggress {}/{}", size, buf.len());
        }
        debug!("blob file read at complited: {}", size);
        Ok(buf)
    }

    async fn from_file(sync: SyncFile, rio: Option<Rio>) -> IOResult<Self> {
        Ok(Self { sync, rio })
    }
}


/// Trait implementation to track that all required function are implemented.
/// It should not contain the actual implementation of the functions, because `async_trait` adds the overhead by boxing the resulting `Future`.
#[async_trait::async_trait]
impl super::super::FileTrait for File {
    fn size(&self) -> u64 {
        self.size()
    }
    fn created_at(&self) -> IOResult<SystemTime> {
        self.created_at()
    }
    fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes()
    }

    async fn write_append_writable_data<R: Send + 'static>(&self, c: impl WritableDataCreator<R>) -> IOResult<R> {
        self.write_append_writable_data(c).await
    }
    async fn write_append_all(&self, buf: Bytes) -> IOResult<()> {
        self.write_append_all(buf).await
    }
    async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        self.write_all_at(offset, buf).await
    }
    async fn read_all(&self) -> IOResult<BytesMut> {
        self.read_all().await
    }
    async fn read_exact_at_allocate(&self, size: usize, offset: u64) -> IOResult<BytesMut> {
        self.read_exact_at_allocate(size, offset).await
    }
    async fn read_exact_at(&self, buf: BytesMut, offset: u64) -> IOResult<BytesMut> {
        self.read_exact_at(buf, offset).await
    }
    async fn fsyncdata(&self) -> IOResult<()> {
        self.fsyncdata().await
    }
}
