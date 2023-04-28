// Sync windows

use crate::prelude::*;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::AtomicU64;
use std::{
    os::windows::fs::{FileExt, OpenOptionsExt},
    time::SystemTime,
};
//use tokio::fs::OpenOptions;
use std::fs::OpenOptions;

/// IO driver for file operations
#[derive(Debug, Clone)]
pub struct IoDriver;

impl IoDriver {
    /// Create new sync io driver
    pub fn new() -> Self {
        Self::new_sync()
    }

    /// Create new sync io driver
    pub fn new_sync() -> Self {
        Self {}
    }

    pub(crate) async fn open(&self, path: impl AsRef<Path>) -> IOResult<File> {
        File::from_file(path, |f| f.create(false).append(true).read(true))
    }

    pub(crate) async fn create(&self, path: impl AsRef<Path>) -> IOResult<File> {
        File::from_file(path, |f| f.create(true).write(true).read(true))
    }
}



#[derive(Debug, Clone)]
pub(crate) struct File {
    inner: Arc<FileInner>
}

#[derive(Debug)]
struct FileInner {
    std_file: StdFile,
    size: AtomicU64
}


impl File {
    pub(crate) fn size(&self) -> u64 {
        self.inner.size.load(Ordering::SeqCst)
    }

    pub(crate) async fn write_append_writable_data<R: Send + 'static>(
        &self,
        c: impl WritableDataCreator<R>,
    ) -> IOResult<R> {
        let len = c.len();
        let file_inner = self.inner.clone();
        Self::background_sync_call(move || {
            let offset = file_inner.size.fetch_add(len, Ordering::SeqCst);
            let (res, data) = c.create(offset);
            Self::write_data(&file_inner, offset, res)?;
            Ok(data)
        })
        .await
    }

    fn write_data(file: &FileInner, mut offset: u64, writable_data: WritableData) -> IOResult<()> {
        match writable_data {
            WritableData::Single(bytes) => file.write_all_at(&bytes, offset),
            WritableData::Double(b1, b2) => file.write_all_at(&b1, offset).and_then(|_| {
                offset = offset + b1.len() as u64;
                file.write_all_at(&b2, offset)
            }),
        }
    }

    pub(crate) async fn write_append_all(&self, buf: Bytes) -> IOResult<()> {
        let file_inner = self.inner.clone();
        Self::background_sync_call(move || {
            let offset = file_inner.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
            file_inner.write_all_at(&buf, offset)
        })
        .await
    }

    pub(crate) async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        debug_assert!(offset + buf.len() as u64 <= self.size());
        let file_inner = self.inner.clone();
        Self::background_sync_call(move || file_inner.write_all_at(&buf, offset)).await
    }

    pub(crate) async fn read_all(&self) -> Result<BytesMut> {
        self.read_exact_at_allocate(self.size().try_into()?, 0).await
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
        let file_inner = self.inner.clone();
        Self::background_sync_call(move || file_inner.read_exact_at(&mut buf, offset).map(|_| buf)).await
            .map_err(|err| err.into())
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        let file_inner = self.inner.clone();
        Self::background_sync_call(move || file_inner.std_file.sync_all()).await
    }

    pub(crate) fn created_at(&self) -> IOResult<SystemTime> {
        let metadata = self.inner.std_file.metadata()?;
        Ok(metadata.created().unwrap_or(SystemTime::now()))
    }

    async fn background_sync_call<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::spawn_blocking(move || f())
            .await
            .expect("spawned blocking task failed")
    }



    fn from_file(
        path: impl AsRef<Path>,
        setup: impl Fn(&mut OpenOptions) -> &mut OpenOptions,
    ) -> IOResult<Self> {
        const FILE_SHARE_READ: u32 = 1;
        let file = setup(&mut OpenOptions::new().share_mode(FILE_SHARE_READ)).open(path.as_ref())?;

        /* 
        if Self::advisory_write_lock_file(file.as_raw_fd()) == LockAcquisitionResult::AlreadyLocked
        {
            error!("File {:?} is locked", path.as_ref());
            panic!("File {:?} is locked", path.as_ref());
        }
        */

        let size = file.metadata()?.len();
        let size = AtomicU64::new(size);

        Ok(Self {
            inner: Arc::new(FileInner {
                std_file: file,
                size
            })
        })
    }
}


impl FileInner {
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> IOResult<()> {
        while !buf.is_empty() {
            match self.std_file.seek_read(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"))
        } else {
            Ok(())
        }
    }

    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> IOResult<()> {
        while !buf.is_empty() {
            match self.std_file.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "failed to write whole buffer"));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}


/// Trait implementation to track that all required function are implemented
#[async_trait::async_trait]
impl super::super::FileTrait for File {
    fn size(&self) -> u64 {
        self.size()
    }
    fn created_at(&self) -> IOResult<SystemTime> {
        self.created_at()
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
    async fn read_all(&self) -> Result<BytesMut> {
        self.read_all().await
    }
    async fn read_exact_at_allocate(&self, size: usize, offset: u64) -> Result<BytesMut> {
        self.read_exact_at_allocate(size, offset).await
    }
    async fn read_exact_at(&self, buf: BytesMut, offset: u64) -> Result<BytesMut> {
        self.read_exact_at(buf, offset).await
    }
    async fn fsyncdata(&self) -> IOResult<()> {
        self.fsyncdata().await
    }
}