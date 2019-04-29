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
//! ```
//! // Initialize new builder and set required params
//! let storage = Builder::new()
//!         .blob_file_name_prefix("benchmark")
//!         .max_blob_size(max_blob_size)
//!         .max_data_in_blob(max_data_in_blob)
//!         .work_dir(tmp_dir.join("pearl_benchmark"))
//!         .key_size(8)
//!         .build()
//!         .unwrap();
//! // Init storage
//! storage.init().unwrap();
//! ```

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod blob;
mod record;
mod storage;

pub use storage::{Builder, Storage, Key};
