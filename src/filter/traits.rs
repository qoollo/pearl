use super::*;

/// Provider for raw filter data
#[async_trait::async_trait]
pub trait BloomDataProvider: Send + Sync {
    /// Read byte from raw filter data
    async fn read_byte(&self, index: u64) -> Result<u8>;
}

/// Trait for scructs which contains bloom filters
#[async_trait::async_trait]
pub trait BloomProvider<Key: Send + Sync>: Sync + Send {
    /// Inner filter type
    type Filter: FilterTrait<Key>;
    /// Check if element in filter
    async fn check_filter(&self, item: &Key) -> FilterResult;
    /// Check if element in filter
    fn check_filter_fast(&self, item: &Key) -> FilterResult;
    /// Returns freed memory
    async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize;
    /// Returns overall filter
    async fn get_filter(&self) -> Option<Self::Filter>;
    /// Returns overall filter
    fn get_filter_fast(&self) -> Option<&Self::Filter>;
    /// Returns allocated memory
    async fn filter_memory_allocated(&self) -> usize;
}

/// Trait filters should implement
#[async_trait::async_trait]
pub trait FilterTrait<Key: Send + Sync>: Clone + Sync + Send {
    /// Add key to filter
    fn add(&mut self, key: &Key);

    /// Check if key in filter (should be implemented if filter can be checked without waiting)
    fn contains_fast(&self, key: &Key) -> FilterResult;

    /// Check if key in filter (can take some time)
    async fn contains<P: BloomDataProvider>(&self, _provider: &P, key: &Key) -> FilterResult {
        self.contains_fast(key)
    }

    /// Offload filter from memory if possible
    fn offload_filter(&mut self) -> usize {
        0
    }

    /// Add another filter to this filter
    #[must_use]
    fn checked_add_assign(&mut self, other: &Self) -> bool;

    /// Memory used by filter
    fn memory_allocated(&self) -> usize {
        0
    }
}

/// Temp doc
pub trait SeededHash: std::hash::Hasher + Send + Sync {
    /// Temp doc
    fn new(seed: u128) -> Self
    where
        Self: Sized;
    /// Temp doc
    fn box_clone(&self) -> Box<dyn SeededHash>;
}
