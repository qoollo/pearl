#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::File;
#[cfg(target_os = "linux")]
pub use linux::IODriver;

#[cfg(all(target_os = "macos", not(feature = "async-io-rio")))]
mod linux;
#[cfg(all(target_os = "macos", not(feature = "async-io-rio")))]
pub(crate) use linux::File;
#[cfg(all(target_os = "macos", not(feature = "async-io-rio")))]
pub use linux::IODriver;
