use super::*;

#[async_trait::async_trait]
impl<T: BloomDataProvider + Send + Sync> BloomProvider for Bloom<T> {
    type Inner = ();
    type DataProvider = T;

    async fn get_bloom(&self) -> Option<&Bloom<Self::DataProvider>> {
        Some(self)
    }

    async fn children(&self) -> Vec<&Self::Inner> {
        vec![]
    }

    async fn contains(&self, item: &[u8]) -> Result<bool> {
        let mut hashers = self.hashers.clone();
        if self.bits_count == 0 {
            return Ok(false);
        }
        for index in hashers.iter_mut().map(|hasher| {
            hasher.write(item.as_ref());
            hasher.finish() % self.bits_count as u64
        }) {
            if !self.get_bit(index as usize).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[async_trait::async_trait]
impl BloomProvider for () {
    type Inner = ();
    type DataProvider = ();

    async fn get_bloom(&self) -> Option<&Bloom<Self::DataProvider>> {
        None
    }

    async fn children(&self) -> Vec<&Self::Inner> {
        vec![]
    }

    async fn contains(&self, _: &[u8]) -> Result<bool> {
        Err(anyhow::anyhow!("Unimplemented"))
    }
}

#[async_trait::async_trait]
impl BloomDataProvider for () {
    async fn read_byte(&self, _index: u64) -> Result<u8> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    async fn read_all(&self) -> Result<Vec<u8>> {
        Err(anyhow::anyhow!("Unimplemented"))
    }
}
