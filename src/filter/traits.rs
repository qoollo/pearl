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

    /// Clear all filter data
    fn clear_filter(&mut self);

    /// Check if filter was offloaded
    fn is_filter_offloaded(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl<T, K> FilterTrait<K> for Option<T>
where
    K: Send + Sync,
    T: Send + Sync + Clone + FilterTrait<K>,
{
    fn add(&mut self, key: &K) {
        if let Some(filter) = self {
            filter.add(key);
        }
    }

    fn contains_fast(&self, key: &K) -> FilterResult {
        if let Some(filter) = &self {
            filter.contains_fast(key)
        } else {
            FilterResult::NeedAdditionalCheck
        }
    }

    fn checked_add_assign(&mut self, other: &Self) -> bool {
        match (self, other) {
            (Some(this), Some(other)) => this.checked_add_assign(other),
            (None, None) => true,
            _ => false,
        }
    }

    fn clear_filter(&mut self) {
        if let Some(filter) = self {
            filter.clear_filter();
        }
    }

    /// Check if key in filter (can take some time)
    async fn contains<P: BloomDataProvider>(&self, provider: &P, key: &K) -> FilterResult {
        if let Some(filter) = &self {
            filter.contains(provider, key).await
        } else {
            FilterResult::NeedAdditionalCheck
        }
    }

    fn offload_filter(&mut self) -> usize {
        if let Some(filter) = self {
            filter.offload_filter()
        } else {
            0
        }
    }

    fn memory_allocated(&self) -> usize {
        if let Some(filter) = self {
            filter.memory_allocated()
        } else {
            0
        }
    }

    fn is_filter_offloaded(&self) -> bool {
        if let Some(filter) = &self {
            filter.is_filter_offloaded()
        } else {
            false
        }
    }
}
