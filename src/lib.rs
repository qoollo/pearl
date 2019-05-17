#![feature(await_macro, async_await, arbitrary_self_types)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

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
//! ```
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
//!     await!(storage.init(spawner)).unwrap();
//!     let key = Id("test".to_string());
//!     let data = b"Hello World!".to_vec();
//!     await!(storage.write(key, data)).unwrap();
//! };
//! pool.run(task);
//! ```

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod blob;
mod record;
mod storage;

pub(crate) use record::Header as RecordHeader;
pub use storage::{Builder, Key, Storage};
