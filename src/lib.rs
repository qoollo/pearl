#![feature(await_macro, async_await, arbitrary_self_types)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![allow(clippy::needless_lifetimes)]

//! # pearl
//!
//! The `pearl` library is a Append only key-value blob storage on disk.
//! Crate `pearl` provides [`Futures 0.3`] interface.
//!
//! [`Futures 0.3`]: https://rust-lang-nursery.github.io/futures-api-docs#latest
//!
//! # Examples
//! The following example shows a storage building and initialization.
//! For more advanced usage see the benchmark tool as the example
//!
//! ```no-run
//! #![feature(async_await, await_macro)]
//! use pearl::{Storage, Builder, Key};
//! use futures::executor::ThreadPool;
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
//! let mut pool = ThreadPool::new().unwrap();
//! let spawner = pool.clone();
//! let task = async {
//!     let mut storage: Storage<Id> = Builder::new()
//!         .work_dir("/tmp/pearl/")
//!         .max_blob_size(1_000_000)
//!         .max_data_in_blob(1_000_000_000)
//!         .blob_file_name_prefix("pearl-test")
//!         .build()
//!         .unwrap();
//!     storage.init(spawner).await.unwrap();
//!     let key = Id("test".to_string());
//!     let data = b"Hello World!".to_vec();
//!     storage.write(key, data).await.unwrap();
//! };
//! pool.run(task);
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
    pub(crate) use crate::record::Record;

    pub(crate) use futures::stream::{
        futures_unordered::FuturesUnordered, StreamExt, TryStreamExt,
    };
    pub(crate) use futures::task::{Spawn, SpawnExt};
    pub(crate) use futures::{future, lock::Mutex, FutureExt};

    pub(crate) use futures_timer::Interval;

    pub(crate) use std::fs::{self, DirEntry, File, OpenOptions};
    pub(crate) use std::path::{Path, PathBuf};
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    pub(crate) use std::{
        marker::PhantomData, os::unix::fs::OpenOptionsExt, sync::Arc, thread, time::Duration,
    };
}
