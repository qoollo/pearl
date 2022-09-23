#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![warn(clippy::all)]
// #![warn(clippy::nursery)]
// #![warn(clippy::pedantic)]
// #![warn(clippy::cargo)]

//! # pearl
//!
//! The `pearl` library is an asyncronous Append only key-value blob storage on disk.
//! Crate `pearl` provides [`Futures 0.3`] interface. Tokio runtime required.
//! Storage follows no harm policy, which means that it won't delete or change any of the stored data.
//!
//! [`Futures 0.3`]: https://rust-lang-nursery.github.io/futures-api-docs#latest
//!
//! # Examples
//! The following example shows a storage building and initialization.
//! For more advanced usage see the benchmark tool as the example
//!
//! ```no_run
//! use pearl::{Storage, Builder, ArrayKey};
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut storage: Storage<ArrayKey<8>> = Builder::new()
//!         .work_dir("/tmp/pearl/")
//!         .max_blob_size(1_000_000)
//!         .max_data_in_blob(1_000_000_000)
//!         .blob_file_name_prefix("pearl-test")
//!         .allow_duplicates()
//!         .build()
//!         .unwrap();
//!     storage.init().await.unwrap();
//!     let key = ArrayKey::<8>::default();
//!     let data = b"Hello World!".to_vec();
//!     storage.write(key, data).await.unwrap();
//! }
//! ```

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;

extern crate ring;

extern crate async_std;

/// Basic info about current build.
pub mod build_info;

mod blob;
/// Types representing various errors that can occur in pearl.
pub mod error;
mod record;
mod storage;

/// bloom filter for faster check record contains in blob
pub mod filter;
pub use filter::{Bloom, BloomDataProvider, BloomProvider, Config as BloomConfig, FilterResult};

/// tools to interact with pearl structures
pub mod tools;

pub use blob::Entry;
pub use error::{Error, Kind as ErrorKind};
pub use record::Meta;
pub use rio;
pub use storage::{Builder, Key, RefKey, Storage, ArrayKey};

mod prelude {
    use crc::{Crc, CRC_32_ISCSI};
    pub const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

    pub(crate) use super::*;
    pub(crate) use std::collections::BTreeMap;
    pub(crate) const ORD: Ordering = Ordering::Relaxed;

    pub(crate) use anyhow::{Context as ErrorContexts, Result};
    pub(crate) use bincode::{deserialize, serialize, serialize_into, serialized_size};
    pub(crate) use blob::{self, Blob, IndexConfig};
    pub(crate) use filter::{Bloom, BloomProvider, Config as BloomConfig, HierarchicalFilters};
    pub(crate) use futures::{
        future,
        lock::Mutex,
        stream::{futures_unordered::FuturesUnordered, TryStreamExt},
    };
    pub(crate) use record::{Header as RecordHeader, Record, RECORD_MAGIC_BYTE};
    pub(crate) use rio::Rio;
    pub(crate) use std::{
        cmp::Ordering as CmpOrdering,
        collections::HashMap,
        convert::TryInto,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::File as StdFile,
        io::Error as IOError,
        io::ErrorKind as IOErrorKind,
        io::Result as IOResult,
        marker::PhantomData,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    pub(crate) use thiserror::Error;
    pub(crate) use tokio::{
        fs::{read_dir, DirEntry, File as TokioFile, OpenOptions},
        sync::{RwLock, Semaphore},
    };
    pub(crate) use tokio_stream::StreamExt;
    pub(crate) use async_std::sync::{
        RwLock as ASRwLock, RwLockUpgradableReadGuard as ASRwLockUpgradableReadGuard,
        RwLockWriteGuard as ASRwLockWriteGuard,
    };
}
