use super::*;

/// Provider for raw filter data
#[async_trait::async_trait]
pub trait BloomDataProvider: Clone + Send + Sync {
    /// Read byte from raw filter data
    async fn read_byte(&self, index: u64) -> Result<u8>;
    /// Read all raw filter data
    async fn read_all(&self) -> Result<Vec<u8>>;
}

/// Trait for scructs which contains bloom filters
#[async_trait::async_trait]
pub trait BloomProvider {
    /// Bloom key
    type Key: Sync + Send + ?Sized;
    /// Check if element in filter
    async fn check_filter(&self, item: &Self::Key) -> Option<bool>;
    /// Returns freed memory
    async fn offload_buffer(&mut self, needed_memory: usize) -> usize;
    /// Returns overall filter
    async fn get_filter(&self) -> Option<Bloom>;
    /// Returns allocated memory
    async fn filter_memory_allocated(&self) -> usize;
}
