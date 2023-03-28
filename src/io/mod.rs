use anyhow::Result;
use bytes::Bytes;

#[cfg(target_family = "unix")]
mod unix;
#[cfg(target_family = "unix")]
pub(crate) use unix::File;
#[cfg(target_family = "unix")]
pub use unix::IoDriver;

#[cfg(not(target_family = "unix"))]
compile_error!("Specified target platform is not supported (only unix family supported)");

pub(crate) trait BytesCreator<R>:
    FnOnce(u64) -> Result<(Result<Bytes, (Bytes, Bytes)>, R)> + Send + 'static
{
}

impl<T, R> BytesCreator<R> for T where
    T: FnOnce(u64) -> Result<(Result<Bytes, (Bytes, Bytes)>, R)> + Send + 'static
{
}
