use crate::prelude::*;

mod bptree;
mod core;
pub(crate) mod header;
mod simple;
mod tools;

#[cfg(test)]
mod benchmarks;

pub(crate) use bptree::BPTreeFileIndex;
use header::IndexHeader;

pub(crate) use self::core::{
    FileIndexTrait, InMemoryIndex, Index, IndexConfig, MemoryAttrs, HEADER_VERSION,
    INDEX_HEADER_MAGIC_BYTE,
};
pub(crate) use super::prelude::*;
pub(crate) use crate::filter::range::RangeFilter;

mod prelude {
    pub(crate) use super::*;
    pub(crate) use tools::*;
}

#[async_trait::async_trait]
pub(crate) trait IndexTrait<K>: Send + Sync {
    async fn get_all(&self, key: &K) -> Result<Option<Vec<RecordHeader>>>;
    async fn get_any(&self, key: &K) -> Result<Option<RecordHeader>>;
    fn push(&mut self, key: &K, h: RecordHeader) -> Result<()>;
    async fn contains_key(&self, key: &K) -> Result<bool>;
    fn count(&self) -> usize;
    async fn dump(&mut self, blob_size: u64) -> Result<usize>;
    async fn load(&mut self, blob_size: u64) -> Result<()>;
    fn mark_all_as_deleted(&mut self, key: &K, default_header: RecordHeader) -> Result<()>;
}
