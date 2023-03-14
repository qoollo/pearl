mod iouring;
mod sync;

#[cfg(not(feature = "async-io-rio"))]
pub(crate) use sync::File;
#[cfg(not(feature = "async-io-rio"))]
pub use sync::IODriver;
