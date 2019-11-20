#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![allow(clippy::needless_doctest_main)]
// #![warn(clippy::pedantic)]

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
mod error;
mod record;
mod storage;

pub use blob::{Entries, Entry};
pub use error::{Error, Kind as ErrorKind, Result};
pub use record::Meta;
pub use storage::{Builder, Key, ReadAll, Storage};

mod prelude {
    pub(crate) type PinBox<T> = Pin<Box<T>>;

    pub(crate) use super::*;
    pub(crate) use bincode::{deserialize, serialize, serialize_into, serialized_size};
    pub(crate) use blob::{self, Blob, File, Location};
    pub(crate) use crc::crc32::checksum_castagnoli as crc32;
    pub(crate) use futures::{
        future::{self, Future, FutureExt, TryFutureExt},
        io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
        lock::{Mutex, MutexGuard},
        stream::{futures_unordered::FuturesUnordered, Stream, StreamExt, TryStreamExt},
    };
    pub(crate) use record::{Header as RecordHeader, Record};
    pub(crate) use std::{
        cell::RefCell,
        cmp::Ordering as CmpOrdering,
        collections::HashMap,
        convert::TryInto,
        error,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::{self, DirEntry, File as StdFile, OpenOptions},
        io::{
            Error as IOError, ErrorKind as IOErrorKind, Read, Result as IOResult, Seek, SeekFrom,
            Write,
        },
        marker::PhantomData,
        num::TryFromIntError,
        os::unix::fs::{FileExt, OpenOptionsExt},
        path::{Path, PathBuf},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    pub(crate) use tokio::timer::{delay, Interval};
    pub(crate) use {Key, Meta};
}
