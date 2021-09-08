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
    type Key: ?Sized + Sync + Send;
    /// Check if element in filter
    async fn check_filter(&self, item: &Self::Key) -> Result<Option<bool>>;
    /// Returns freed memory
    async fn offload_buffer(&mut self, needed_memory: usize) -> usize;
}

#[async_trait::async_trait]
impl<P, K> BloomProvider for Arc<P>
where
    P: BloomProvider<Key = K> + Send + Sync,
    K: Sync + Send + ?Sized,
{
    type Key = K;

    async fn check_filter(&self, item: &Self::Key) -> Result<Option<bool>> {
        P::check_filter(self, item).await
    }

    async fn offload_buffer(&mut self, needed_memory: usize) -> usize {
        P::offload_buffer(&mut self, needed_memory).await
    }
}
