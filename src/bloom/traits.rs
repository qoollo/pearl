use super::*;

/// Provider for raw filter data
#[async_trait::async_trait]
pub trait BloomDataProvider: Send + Sync {
    /// Read byte from raw filter data
    async fn read_byte(&self, index: u64) -> Result<u8>;
}

/// Trait for scructs which contains bloom filters
#[async_trait::async_trait]
pub trait BloomProvider<Key>: Sync + Send {
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
pub trait FilterTrait<Key>: Clone + Sync + Send {
    /// Add key to filter
    fn add(&mut self, key: &Key);

    /// Check if key in filter (should be implemented if filter can be checked without waiting)
    fn contains_fast(&self, key: &Key) -> FilterResult;

    /// Check if key in filter (can take some time)
    async fn contains<P: BloomDataProvider>(&self, provider: &P, key: &Key) -> FilterResult;

    /// Offload filter from memory if possible
    fn offload_filter(&mut self) -> usize;

    /// Add another filter to this filter
    fn checked_add_assign(&mut self, other: &Self) -> Option<()>;

    /// Memory used by filter
    fn memory_allocated(&self) -> usize;
}
