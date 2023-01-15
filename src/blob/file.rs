use std::{
    os::unix::prelude::{AsRawFd, FileExt},
    time::SystemTime,
};

use bytes::{Bytes, BytesMut};
use nix::{errno::Errno, fcntl::FcntlArg};

use super::prelude::*;

// This size was selected based on results of tests
// These tests were performed on 8-core machine with HDD
// Results are confirmed on 8 and 80 threads, for put and get
const MAX_SYNC_OPERATION_SIZE: usize = 100_000;

#[derive(Debug, Clone)]
pub struct File {
    ioring: Option<Rio>,
    no_lock_fd: Arc<StdFile>, // requires only for read_at/write_at methods
    size: Arc<AtomicU64>,
}

#[derive(PartialEq, Eq)]
enum LockAcquisitionResult {
    Acquired,
    AlreadyLocked,
    Error(Errno),
}

impl File {
    pub(crate) async fn open(path: impl AsRef<Path>, ioring: Option<Rio>) -> IOResult<Self> {
        Self::from_file(path, |f| f.create(false).append(true).read(true), ioring).await
    }

    pub(crate) async fn create(path: impl AsRef<Path>, ioring: Option<Rio>) -> IOResult<Self> {
        Self::from_file(path, |f| f.create(true).write(true).read(true), ioring).await
    }

    async fn from_file(
        path: impl AsRef<Path>,
        setup: impl Fn(&mut OpenOptions) -> &mut OpenOptions,
        ioring: Option<Rio>,
    ) -> IOResult<Self> {
        let file = setup(&mut OpenOptions::new()).open(path.as_ref()).await?;

        if Self::advisory_write_lock_file(file.as_raw_fd()) == LockAcquisitionResult::AlreadyLocked
        {
            error!("File {:?} is locked", path.as_ref());
            panic!("File {:?} is locked", path.as_ref());
        }

        Self::from_tokio_file(file, ioring).await
    }

    pub fn size(&self) -> u64 {
        self.size.load(ORD)
    }

    pub(crate) async fn write_append_all(&self, buf: Bytes) -> IOResult<()> {
        if let Some(ref ioring) = self.ioring {
            self.write_append_all_aio(buf, ioring).await?;
        } else {
            self.write_append_all_sync(buf).await?;
        }
        Ok(())
    }

    pub(crate) async fn write_append_all_sync(&self, buf: Bytes) -> IOResult<()> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.write_all_at_sync(offset, buf).await
    }

    async fn write_append_all_aio(&self, buf: Bytes, ioring: &Rio) -> IOResult<()> {
        let offset = self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.write_all_at_aio(offset, buf, ioring).await
    }

    pub(crate) async fn write_append_all_buffers(
        &self,
        first_buf: Bytes,
        second_buf: Bytes,
    ) -> IOResult<()> {
        if let Some(ref ioring) = self.ioring {
            self.write_append_all_buffers_aio(ioring, first_buf, second_buf)
                .await?;
        } else {
            self.write_append_all_buffers_sync(first_buf, second_buf)
                .await?;
        }
        Ok(())
    }

    async fn write_append_all_buffers_aio(
        &self,
        ioring: &Rio,
        first_buf: Bytes,
        second_buf: Bytes,
    ) -> IOResult<()> {
        let first_len = first_buf.len() as u64;
        let second_len = second_buf.len() as u64;
        let mut offset = self
            .size
            .fetch_add(first_len + second_len, Ordering::SeqCst);
        self.write_all_at_aio(offset, first_buf, ioring).await?;
        offset = offset + first_len;
        self.write_all_at_aio(offset, second_buf, ioring).await?;
        Ok(())
    }

    async fn write_append_all_buffers_sync(
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
        if let Some(ref ioring) = self.ioring {
            self.write_all_at_aio(offset, buf, ioring).await
        } else {
            self.write_all_at_sync(offset, buf).await
        }
    }

    async fn write_all_at_aio(&self, offset: u64, buf: Bytes, ioring: &Rio) -> IOResult<()> {
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
        Ok(())
    }

    async fn write_all_at_sync(&self, offset: u64, buf: Bytes) -> IOResult<()> {
        let file = self.no_lock_fd.clone();
        if buf.len() <= MAX_SYNC_OPERATION_SIZE {
            Self::inplace_sync_call(move || file.write_all_at(&buf, offset))
        } else {
            Self::background_sync_call(move || file.write_all_at(&buf, offset)).await
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
        if let Some(ref ioring) = self.ioring {
            self.read_exact_at_aio(buf, offset, ioring).await
        } else {
            self.read_exact_at_sync(buf, offset).await
        }
    }

    pub(crate) async fn read_exact_at_sync(
        &self,
        mut buf: BytesMut,
        offset: u64,
    ) -> Result<BytesMut> {
        let file = self.no_lock_fd.clone();

        Ok(if buf.len() <= MAX_SYNC_OPERATION_SIZE {
            Self::inplace_sync_call(move || file.read_exact_at(&mut buf, offset).map(|_| buf))
        } else {
            Self::background_sync_call(move || file.read_exact_at(&mut buf, offset).map(|_| buf))
                .await
        }?)
    }

    async fn read_exact_at_aio(
        &self,
        mut buf: BytesMut,
        offset: u64,
        ioring: &Rio,
    ) -> Result<BytesMut> {
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
        Ok(buf)
    }

    async fn from_tokio_file(file: TokioFile, ioring: Option<Rio>) -> IOResult<Self> {
        let size = file.metadata().await?.len();
        let size = Arc::new(AtomicU64::new(size));
        let std_file = file.try_into_std().expect("tokio file into std");

        let file = Self {
            ioring,
            no_lock_fd: Arc::new(std_file),
            size,
        };
        Ok(file)
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        if let Some(ref ioring) = self.ioring {
            let compl = ioring.fsync(&*self.no_lock_fd);
            compl.await
        } else {
            let fd = self.no_lock_fd.clone();
            Self::background_sync_call(move || fd.sync_all()).await
        }
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

    pub(crate) fn created_at(&self) -> Result<SystemTime> {
        let metadata = self.no_lock_fd.metadata()?;
        Ok(metadata.created().unwrap_or(SystemTime::now()))
    }
}
