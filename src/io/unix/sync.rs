// Sync linux

use crate::prelude::*;
use bytes::{Bytes, BytesMut};
use nix::errno::Errno;
use nix::fcntl::FcntlArg;
use std::sync::atomic::AtomicU64;
use std::{
    os::unix::prelude::{AsRawFd, FileExt},
    time::SystemTime,
};
use tokio::fs::{
    File as TokioFile,
    OpenOptions
};

/// IO driver for file operations
#[derive(Debug, Clone)]
pub struct IoDriver;

impl IoDriver {
    #[cfg(not(feature = "async-io-rio"))]
    /// Create new sync io driver
    pub fn new() -> Self {
        Self::new_sync()
    }

    /// Create new sync io driver
    pub fn new_sync() -> Self {
        Self {}
    }

    pub(crate) async fn open(&self, path: impl AsRef<Path>) -> IOResult<File> {
        File::from_file(path, |f| f.create(false).append(true).read(true)).await
    }

    pub(crate) async fn create(&self, path: impl AsRef<Path>) -> IOResult<File> {
        File::from_file(path, |f| f.create(true).write(true).read(true)).await
    }
}

// This size was selected based on results of tests
// These tests were performed on 8-core machine with HDD
// Results are confirmed on 8 and 80 threads, for put and get
const MAX_SYNC_OPERATION_SIZE: usize = 81_920;

#[derive(Debug, Clone)]
pub(crate) struct File {
    inner: Arc<FileInner>
}

#[derive(Debug)]
struct FileInner {
    std_file: StdFile,
    size: AtomicU64,
    synced_size: AtomicU64
}

#[derive(PartialEq, Eq)]
enum LockAcquisitionResult {
    Acquired,
    AlreadyLocked,
    Error(Errno),
}

impl File {
    pub(crate) fn size(&self) -> u64 {
        self.inner.size.load(Ordering::SeqCst)
    }
    pub(crate) fn synced_size(&self) -> u64 {
        self.inner.synced_size.load(Ordering::SeqCst)
    }
    pub(crate) fn dirty_bytes(&self) -> u64 {
        self.size() - self.synced_size()
    }

    pub(crate) async fn write_append_writable_data<R: Send + 'static>(
        &self,
        c: impl WritableDataCreator<R>,
    ) -> IOResult<R> {
        let len = c.len();
        let file_inner = self.inner.clone();
        if Self::can_run_inplace(len) {
            Self::inplace_sync_call(move || {
                let offset = file_inner.size.fetch_add(len, Ordering::SeqCst);
                let (res, data) = c.create(offset);
                Self::write_data(&file_inner.std_file, offset, res)?;
                Ok(data)
            })
        } else {
            Self::background_sync_call(move || {
                let offset = file_inner.size.fetch_add(len, Ordering::SeqCst);
                let (res, data) = c.create(offset);
                Self::write_data(&file_inner.std_file, offset, res)?;
                Ok(data)
            })
            .await
        }
    }

    fn write_data(file: &StdFile, mut offset: u64, writable_data: WritableData) -> IOResult<()> {
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
        if Self::can_run_inplace(buf.len() as u64) {
            Self::inplace_sync_call(move || {
                let offset = file_inner.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
                file_inner.std_file.write_all_at(&buf, offset)
            })
        } else {
            Self::background_sync_call(move || {
                let offset = file_inner.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
                file_inner.std_file.write_all_at(&buf, offset)
            })
            .await
        }
    }

    pub(crate) async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        debug_assert!(offset + buf.len() as u64 <= self.size());
        let file_inner = self.inner.clone();
        if Self::can_run_inplace(buf.len() as u64) {
            Self::inplace_sync_call(move || file_inner.std_file.write_all_at(&buf, offset))
        } else {
            Self::background_sync_call(move || file_inner.std_file.write_all_at(&buf, offset)).await
        }
    }

    #[cfg(not(feature = "async-io-rio"))]
    pub(crate) async fn read_all(&self) -> IOResult<BytesMut> {
        self.read_exact_at_allocate(self.size().try_into().expect("File is too large"), 0)
            .await
    }

    #[cfg(not(feature = "async-io-rio"))]
    pub(crate) async fn read_exact_at_allocate(
        &self,
        size: usize,
        offset: u64,
    ) -> IOResult<BytesMut> {
        let buf = BytesMut::zeroed(size);
        self.read_exact_at(buf, offset).await
    }

    pub(crate) async fn read_exact_at(&self, mut buf: BytesMut, offset: u64) -> IOResult<BytesMut> {
        let file_inner = self.inner.clone();

        Ok(if Self::can_run_inplace(buf.len() as u64) {
            Self::inplace_sync_call(move || file_inner.std_file.read_exact_at(&mut buf, offset).map(|_| buf))
        } else {
            Self::background_sync_call(move || file_inner.std_file.read_exact_at(&mut buf, offset).map(|_| buf))
                .await
        }?)
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        let file_inner = self.inner.clone();
        let size = self.size();
        Self::background_sync_call(
            move || file_inner.std_file.sync_all().map(|_| file_inner.synced_size.store(size, Ordering::SeqCst))
        ).await
    }

    pub(crate) fn created_at(&self) -> IOResult<SystemTime> {
        let metadata = self.inner.std_file.metadata()?;
        Ok(metadata.created().unwrap_or(SystemTime::now()))
    }

    #[cfg(feature = "async-io-rio")]
    pub(super) fn std_file_ref(&self) -> &StdFile {
        &self.inner.std_file
    }

    #[cfg(feature = "async-io-rio")]
    pub(super) fn file_size_append(&self, len: u64) -> u64 {
        self.inner.size.fetch_add(len, Ordering::SeqCst)
    }

    fn advisory_write_lock_file(fd: i32) -> LockAcquisitionResult {
        let flock = libc::flock {
            l_len: 0, // 0 means "whole file"
            l_start: 0,
            l_whence: libc::SEEK_SET as i16,
            l_type: libc::F_WRLCK as i16,
            l_pid: -1, // pid of current file owner, if any (when fcntl is invoked with F_GETLK)
        };
        let res = nix::fcntl::fcntl(fd, FcntlArg::F_SETLK(&flock));
        if let Err(e) = res {
            warn!("acquiring writelock failed, errno: {:?}", e);
            match e {
                Errno::EACCES | Errno::EAGAIN => LockAcquisitionResult::AlreadyLocked,
                e => LockAcquisitionResult::Error(e),
            }
        } else {
            LockAcquisitionResult::Acquired
        }
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

    fn inplace_sync_call<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::block_in_place(move || f())
    }

    fn can_run_inplace(len: u64) -> bool {
        use tokio::runtime::{Handle, RuntimeFlavor};
        len <= MAX_SYNC_OPERATION_SIZE as u64
            && Handle::current().runtime_flavor() != RuntimeFlavor::CurrentThread
    }

    async fn from_file(
        path: impl AsRef<Path>,
        setup: impl Fn(&mut OpenOptions) -> &mut OpenOptions,
    ) -> IOResult<Self> {
        let file = setup(&mut OpenOptions::new()).open(path.as_ref()).await?;

        if Self::advisory_write_lock_file(file.as_raw_fd()) == LockAcquisitionResult::AlreadyLocked
        {
            error!("File {:?} is locked", path.as_ref());
            panic!("File {:?} is locked", path.as_ref());
        }

        Self::from_tokio_file(file).await
    }

    async fn from_tokio_file(file: TokioFile) -> IOResult<Self> {
        let size = file.metadata().await?.len();
        let synced_size = AtomicU64::new(size);
        let size = AtomicU64::new(size);
        let std_file = file.try_into_std().expect("tokio file into std");

        let file = Self {
            inner: Arc::new(FileInner { 
                std_file, 
                size,
                synced_size
            })
        };
        Ok(file)
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
