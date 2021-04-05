use crate::prelude::*;

/// bloom filter for faster check record contains in blob
pub mod bloom;
mod bptree;
mod core;
mod header;
mod simple;
mod tools;

use bptree::BPTreeFileIndex;
use header::IndexHeader;

pub(crate) use self::core::{
    FileIndexTrait, InMemoryIndex, Index, IndexConfig, MemoryAttrs, HEADER_VERSION,
};
pub(crate) use super::prelude::*;
pub(crate) use bloom::{Bloom, Config as BloomConfig};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use ahash::AHasher;
    pub(crate) use bitvec::prelude::*;
    pub(crate) use std::hash::Hasher;
    pub(crate) use tools::*;
}

#[async_trait::async_trait]
pub(crate) trait IndexTrait: Send + Sync {
    async fn get_all(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>>;
    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>>;
    fn push(&mut self, h: RecordHeader) -> Result<()>;
    async fn contains_key(&self, key: &[u8]) -> Result<bool>;
    fn count(&self) -> usize;
    async fn dump(&mut self) -> Result<usize>;
    async fn load(&mut self) -> Result<()>;
}
