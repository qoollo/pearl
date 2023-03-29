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

pub(crate) enum WritableData {
    Single(Bytes),
    Double(Bytes, Bytes),
}

pub(crate) trait WritableDataCreator<R>:
    FnOnce(u64) -> Result<(WritableData, R)> + Send + 'static
{
}

impl<T, R> WritableDataCreator<R> for T where
    T: FnOnce(u64) -> Result<(WritableData, R)> + Send + 'static
{
}
