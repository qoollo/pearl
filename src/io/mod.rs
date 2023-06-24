use bytes::{Bytes, BytesMut};
use std::io::Result as IOResult;
use std::time::SystemTime;

#[cfg(target_family = "unix")]
mod unix;
#[cfg(target_family = "unix")]
pub(crate) use unix::File;
#[cfg(target_family = "unix")]
pub use unix::IoDriver;

#[cfg(target_family = "windows")]
mod windows;
#[cfg(target_family = "windows")]
pub(crate) use windows::File;
#[cfg(target_family = "windows")]
pub use windows::IoDriver;

#[cfg(not(any(target_family = "unix", target_family = "windows")))]
compile_error!("Specified target platform is not supported (only unix and windows family supported)");


pub(crate) enum WritableData {
    Single(Bytes),
    Double(Bytes, Bytes),
}

pub(crate) trait WritableDataCreator<R>: Send + 'static {
    fn create(self, offset: u64) -> (WritableData, R);
    fn len(&self) -> u64;
}


/// Trait that all Files implementations should support.
/// Used to track that File implementations have all required functions
#[async_trait::async_trait]
trait FileTrait: Sized {
    fn size(&self) -> u64;
    fn created_at(&self) -> IOResult<SystemTime>;
    fn dirty_bytes(&self) -> u64;

    async fn write_append_writable_data<R: Send + 'static>(&self, c: impl WritableDataCreator<R>) -> IOResult<R>;
    async fn write_append_all(&self, buf: Bytes) -> IOResult<()>;
    async fn write_all_at(&self, offset: u64, buf: Bytes) -> IOResult<()>;

    async fn read_all(&self) -> IOResult<BytesMut>;
    async fn read_exact_at_allocate(&self, size: usize, offset: u64) -> IOResult<BytesMut>;
    async fn read_exact_at(&self, mut buf: BytesMut, offset: u64) -> IOResult<BytesMut>;

    async fn fsyncdata(&self) -> IOResult<()>;
}
