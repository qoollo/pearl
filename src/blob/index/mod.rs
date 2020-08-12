use crate::prelude::*;

/// bloom filter for faster check record contains in blob
pub mod bloom;
mod simple;

pub(crate) use super::prelude::*;
pub(crate) use bloom::{Bloom, Config};
pub(crate) use simple::{InMemoryIndex, Simple, State};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use ahash::AHasher;
    pub(crate) use bitvec::prelude::*;
    pub(crate) use std::hash::Hasher;
}

#[async_trait::async_trait]
pub(crate) trait Index: Send + Sync {
    //TODO: get_all
    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>>;
    fn push(&mut self, h: RecordHeader) -> Result<()>;
    async fn contains_key(&self, key: &[u8]) -> Result<bool>;
    async fn count(&self) -> Result<usize>;
    async fn dump(&mut self) -> Result<usize>;
    async fn load(&mut self) -> Result<()>;
}
