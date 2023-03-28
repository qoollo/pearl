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

    pub(crate) async fn write_append_bytes_creator<R: Send + 'static>(
        &self,
        bc: impl BytesCreator<R>,
        len: u64,
    ) -> Result<R> {
        if let Some(ref rio) = self.rio {
            let mut offset = self.sync.file_size_append(len);
            let (res, data) = bc(offset)?;
            match res {
                Ok(bytes) => self.write_all_at_aio(bytes, offset, rio).await,
                Error((b1, b2)) => {
                    self.write_all_at_aio(b1, offset, rio).await?;
                    offset = offset + b1.len() as u64;
                    self.write_all_at_aio(b2, offset, rio).await
                }
            }
            Ok(data)
        } else {
            self.sync.write_append_bytes_creator(bc, len).await
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

    pub(crate) async fn write_append_all_buffers(
        &self,
        first_buf: Bytes,
        second_buf: Bytes,
    ) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            let first_len = first_buf.len() as u64;
            let second_len = second_buf.len() as u64;
            let mut offset = self.sync.file_size_append(first_len + second_len);
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
        debug_assert!(offset + buf.len() as u64 <= self.size());
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

    pub(crate) async fn read_exact_at(&self, buf: BytesMut, offset: u64) -> Result<BytesMut> {
        debug!("File read at buf len: {}, offset: {}", buf.len(), offset);
        if let Some(ref rio) = self.rio {
            self.read_exact_at_aio(buf, offset, rio).await
        } else {
            self.sync.read_exact_at(buf, offset).await
        }
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref rio) = self.rio {
            rio.fsync(self.sync.std_file_ref()).await
        } else {
            self.sync.fsyncdata().await
        }
    }

    pub(crate) fn created_at(&self) -> Result<SystemTime> {
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
    ) -> Result<BytesMut> {
        let compl = rio.read_at(self.sync.std_file_ref(), &buf, offset);
        let mut size = compl.await.with_context(|| "read at failed")?;
        while size < buf.len() {
            debug!(
                "io uring read partial block, trying to read the rest, completed {}/{} bytes",
                size,
                buf.len()
            );
            let slice = buf.split_at_mut(size).1;
            debug!("blob file read at buf to fill up: {}b", slice.len());
            let compl = rio.read_at(self.sync.std_file_ref(), &slice, offset + size as u64);
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
    }

    async fn from_file(sync: SyncFile, rio: Option<Rio>) -> IOResult<Self> {
        Ok(Self { sync, rio })
    }
}
