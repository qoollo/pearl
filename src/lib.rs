#![feature(async_await, arbitrary_self_types)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![allow(clippy::needless_lifetimes)]

//! # pearl
//!
//! The `pearl` library is a Append only key-value blob storage on disk.
//! Crate `pearl` provides [`Futures 0.3`] interface. Tokio runtime required.
//!
//! [`Futures 0.3`]: https://rust-lang-nursery.github.io/futures-api-docs#latest
//!
//! # Examples
//! The following example shows a storage building and initialization.
//! For more advanced usage see the benchmark tool as the example
//!
//! ```no-run
//! #![feature(async_await)]
//! use pearl::{Storage, Builder, Key};
//!
//! struct Id(String);
//!
//! impl AsRef<[u8]> for Id {
//!     fn as_ref(&self) -> &[u8] {
//!         self.0.as_bytes()
//!     }
//! }
//!
//! impl Key for Id {
//!     const LEN: u16 = 4;
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut storage: Storage<Id> = Builder::new()
//!         .work_dir("/tmp/pearl/")
//!         .max_blob_size(1_000_000)
//!         .max_data_in_blob(1_000_000_000)
//!         .blob_file_name_prefix("pearl-test")
//!         .build()
//!         .unwrap();
//!     storage.init().await.unwrap();
//!     let key = Id("test".to_string());
//!     let data = b"Hello World!".to_vec();
//!     storage.write(key, data).await.unwrap();
//! }
//! ```

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate futures;

mod blob;
mod record;
mod storage;

pub use storage::{Builder, Error, ErrorKind, Key, Storage};

mod prelude {
    pub(crate) use crate::blob::{self, Blob};
    pub(crate) use crate::{record::Record, storage::Key};

    pub(crate) use futures::stream::{
        futures_unordered::FuturesUnordered, StreamExt, TryStreamExt,
    };
    pub(crate) use futures::{future, lock::Mutex, FutureExt};

    pub(crate) use tokio::timer::Interval;

    pub(crate) use bincode::{deserialize, serialize};
    pub(crate) use crc::crc32::checksum_castagnoli as crc32;
    pub(crate) use std::fmt::{Display, Formatter, Result as FmtResult};
    pub(crate) use std::fs::{self, DirEntry, File, OpenOptions};
    pub(crate) use std::path::{Path, PathBuf};
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    pub(crate) use std::time::{Duration, Instant};
    pub(crate) use std::{error, marker::PhantomData, os::unix::fs::OpenOptionsExt, sync::Arc};
}
