mod core;
pub(crate) mod config;
mod entry;
pub(crate) mod file_name;
pub(crate) mod header;
pub(crate) mod index;

pub(crate) use self::config::BlobConfig;
pub(crate) use self::core::BLOB_INDEX_FILE_EXTENSION;
pub(crate) use self::core::{Blob, DeleteResult};
pub(crate) use self::file_name::FileName;
pub use self::entry::Entry;
pub(crate) use self::index::IndexConfig;
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;
    pub(crate) use async_lock::RwLock as ASRwLock;
    pub(crate) use index::Index;
    pub(crate) use std::sync::RwLock as SRwLock;
}
