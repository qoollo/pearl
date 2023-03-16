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
use tokio::fs::OpenOptions;

/// IO driver for file operations
#[derive(Debug, Clone, Default)]
pub struct IoDriver;

impl IoDriver {
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
    pub(super) no_lock_fd: Arc<StdFile>,
    pub(super) size: Arc<AtomicU64>,
}

#[derive(PartialEq, Eq)]
enum LockAcquisitionResult {
    Acquired,
    AlreadyLocked,
    Error(Errno),
}

impl File {
    pub(crate) fn size(&self) -> u64 {
        self.size.load(ORD)
    }

    pub(crate) async fn write_append_all(&self, buf: Bytes) -> IOResult<()> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.write_all_at(offset, buf).await
    }

    pub(crate) async fn write_append_all_buffers(
        &self,
        first_buf: Bytes,
        second_buf: Bytes,
    ) -> IOResult<()> {
        let first_len = first_buf.len() as u64;
        let second_len = second_buf.len() as u64;
        let len = first_len + second_len;
        let mut offset = self.size.fetch_add(len, Ordering::SeqCst);
        let file = self.no_lock_fd.clone();
        if len <= MAX_SYNC_OPERATION_SIZE as u64 {
            Self::inplace_sync_call(move || {
                file.write_all_at(&first_buf, offset)?;
                offset = offset + first_len;
                file.write_all_at(&second_buf, offset)?;
                Ok(())
            })
        } else {
            Self::background_sync_call(move || {
                file.write_all_at(&first_buf, offset)?;
                offset = offset + first_len;
                file.write_all_at(&second_buf, offset)?;
                Ok(())
            })
            .await
        }
    }

    pub(crate) async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        let file = self.no_lock_fd.clone();
        if buf.len() <= MAX_SYNC_OPERATION_SIZE {
            Self::inplace_sync_call(move || file.write_all_at(&buf, offset))
        } else {
            Self::background_sync_call(move || file.write_all_at(&buf, offset)).await
        }
    }

    #[cfg(not(feature = "async-io-rio"))]
    pub(crate) async fn read_all(&self) -> Result<BytesMut> {
        self.read_exact_at_allocate(self.size().try_into()?, 0)
            .await
    }

    #[cfg(not(feature = "async-io-rio"))]
    pub(crate) async fn read_exact_at_allocate(
        &self,
        size: usize,
        offset: u64,
    ) -> Result<BytesMut> {
        let buf = BytesMut::zeroed(size);
        self.read_exact_at(buf, offset).await
    }

    pub(crate) async fn read_exact_at(&self, mut buf: BytesMut, offset: u64) -> Result<BytesMut> {
        let file = self.no_lock_fd.clone();

        Ok(if buf.len() <= MAX_SYNC_OPERATION_SIZE {
            Self::inplace_sync_call(move || file.read_exact_at(&mut buf, offset).map(|_| buf))
        } else {
            Self::background_sync_call(move || file.read_exact_at(&mut buf, offset).map(|_| buf))
                .await
        }?)
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        let fd = self.no_lock_fd.clone();
        Self::background_sync_call(move || fd.sync_all()).await
    }

    pub(crate) fn created_at(&self) -> Result<SystemTime> {
        let metadata = self.no_lock_fd.metadata()?;
        Ok(metadata.created().unwrap_or(SystemTime::now()))
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
        match tokio::runtime::Handle::current().runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => f(),
            _ => tokio::task::block_in_place(move || f()),
        }
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
        let size = Arc::new(AtomicU64::new(size));
        let std_file = file.try_into_std().expect("tokio file into std");

        let file = Self {
            no_lock_fd: Arc::new(std_file),
            size,
        };
        Ok(file)
    }
}
