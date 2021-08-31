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
pub trait BloomProvider: Send + Sync {
    /// Provider
    type Inner: BloomProvider;
    /// Data provider
    type DataProvider: BloomDataProvider;
    /// Get filter
    async fn get_bloom(&self) -> Option<&Bloom<Self::DataProvider>> {
        None
    }
    /// Get children filters
    async fn children(&self) -> Vec<&Self::Inner> {
        vec![]
    }
    /// Check if element in filter
    async fn contains(&self, item: &[u8]) -> Result<bool>;
}
