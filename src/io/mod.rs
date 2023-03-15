#[cfg(target_os = "linux")]
mod unix;
#[cfg(target_os = "linux")]
pub(crate) use unix::File;
#[cfg(target_os = "linux")]
pub use unix::IODriver;

#[cfg(all(target_os = "macos", not(feature = "async-io-rio")))]
mod linux;
#[cfg(all(target_os = "macos", not(feature = "async-io-rio")))]
pub(crate) use unix::File;
#[cfg(all(target_os = "macos", not(feature = "async-io-rio")))]
pub use unix::IODriver;
