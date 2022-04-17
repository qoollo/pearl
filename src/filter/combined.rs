use super::*;
pub use crate::filter::range::*;

#[derive(Debug, Clone)]
/// Combined bloom and range filter
pub struct CombinedFilter<K>
where
    for<'a> K: Key<'a>,
{
    bloom: Option<Bloom>,
    range: RangeFilter<K>,
}

impl<K> CombinedFilter<K>
where
    for<'a> K: Key<'a>,
{
    /// Create new CombinedFilter
    pub fn new(bloom: Option<Bloom>, range: RangeFilter<K>) -> Self {
        Self { bloom, range }
    }

    /// Get inner bloom filter
    pub fn bloom(&self) -> &Option<Bloom> {
        &self.bloom
    }

    /// Get inner range filter
    pub fn range(&self) -> &RangeFilter<K> {
        &self.range
    }
}

/// Trait filters should implement
#[async_trait::async_trait]
impl<K> FilterTrait<K> for CombinedFilter<K>
where
    for<'a> K: Key<'a> + Send + Sync,
    Bloom: FilterTrait<K>,
{
    /// Add key to filter
    fn add(&mut self, key: &K) {
        self.range.add(key);
        self.bloom.add(key);
    }

    /// Check if key in filter (should be implemented if filter can be checked without waiting)
    fn contains_fast(&self, key: &K) -> FilterResult {
        let res_range = self.range.contains_fast(key);
        if let FilterResult::NeedAdditionalCheck = res_range {
            self.bloom.contains_fast(key)
        } else {
            res_range
        }
    }

    /// Check if key in filter (can take some time)
    async fn contains<P: BloomDataProvider>(&self, provider: &P, key: &K) -> FilterResult {
        let res_range = self.range.contains_fast(key);
        if let FilterResult::NeedAdditionalCheck = res_range {
            FilterTrait::contains(&self.bloom, provider, key).await
        } else {
            res_range
        }
    }

    /// Offload filter from memory if possible
    fn offload_filter(&mut self) -> usize {
        self.bloom.offload_filter()
    }

    /// Add another filter to this filter
    fn checked_add_assign(&mut self, other: &Self) -> bool {
        self.range.checked_add_assign(&other.range) && self.bloom.checked_add_assign(&other.bloom)
    }

    /// Memory used by filter
    fn memory_allocated(&self) -> usize {
        self.range.memory_allocated() + self.bloom.memory_allocated()
    }

    fn clear_filter(&mut self) {
        self.range.clear_filter();
        self.bloom.clear_filter();
    }

    fn is_filter_offloaded(&self) -> bool {
        self.bloom.is_filter_offloaded()
    }
}
