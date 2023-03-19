#[cfg(target_family = "unix")]
mod unix;
#[cfg(target_family = "unix")]
pub(crate) use unix::File;
#[cfg(target_family = "unix")]
pub use unix::IoDriver;

#[cfg(not(target_family = "unix"))]
compile_error!("Specified target platform is not supported (only unix family supported)");
