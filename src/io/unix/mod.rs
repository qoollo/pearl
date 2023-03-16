#[cfg(not(feature = "async-io-rio"))]
mod sync;
#[cfg(not(feature = "async-io-rio"))]
pub(crate) use sync::File;
#[cfg(not(feature = "async-io-rio"))]
pub use sync::IoDriver;

#[cfg(feature = "async-io-rio")]
mod iouring;
#[cfg(feature = "async-io-rio")]
mod sync;
#[cfg(feature = "async-io-rio")]
pub(crate) use iouring::File;
#[cfg(feature = "async-io-rio")]
pub use iouring::IoDriver;
