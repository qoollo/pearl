mod impls;
mod traits;
pub use traits::*;

pub use ahash::AHasher;
pub use bitvec::prelude::*;
pub(crate) use std::hash::Hasher;

use super::prelude::*;
use bitvec::order::Lsb0;

/// Bloom filter
#[derive(Debug, Clone)]
pub struct Bloom<P: BloomDataProvider> {
    inner: BloomInner<P>,
    bits_count: usize,
    hashers: Vec<AHasher>,
    config: Config,
}

#[derive(Debug, Clone)]
enum BloomInner<P: BloomDataProvider> {
    InMemory(BitVec<Lsb0, u64>),
    OnDisk(P),
}

impl<P: BloomDataProvider> Default for Bloom<P> {
    fn default() -> Self {
        Self {
            inner: BloomInner::InMemory(Default::default()),
            ..Default::default()
        }
    }
}

/// Bloom filter configuration parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// records count in one blob.
    pub elements: usize,
    /// number of hash functions, the more hash functions.
    /// you have, the slower bloom filter, and the quicker it fills up. If you
    /// have too few, however, you may suffer too many false positives.
    pub hashers_count: usize,
    /// number of bits in the inner buffer.
    pub max_buf_bits_count: usize,
    /// filter buf increase value.
    pub buf_increase_step: usize,
    /// filter incrementally increases buffer
    /// size by step and checks result false positive rate to be less than param.
    /// It stops once buffer reaches size of max_buf_bits_count.
    pub preferred_false_positive_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Save {
    config: Config,
    buf: Vec<u64>,
    bits_count: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            elements: 100_000,
            hashers_count: 2,
            max_buf_bits_count: 8_388_608, // 1Mb
            buf_increase_step: 8196,
            preferred_false_positive_rate: 0.001,
        }
    }
}

fn false_positive_rate(k: f64, n: f64, m: f64) -> f64 {
    (1_f64 - 1_f64.exp().powf(-k * n / m)).powi(k as i32)
}

impl<P: BloomDataProvider> Bloom<P> {
    /// Create new bloom filter
    pub fn new(config: Config) -> Self {
        let elements = config.elements as f64;
        trace!("bloom filter for {} elements", elements);
        let max_bit_count = config.max_buf_bits_count; // 1Mb
        trace!("max bit count: {}", max_bit_count);
        let k = config.hashers_count;
        let mut bits_count = (elements * k as f64 / 2_f64.ln()) as usize;
        let bits_step = config.buf_increase_step;
        let mut fpr = 1_f64;
        while fpr > config.preferred_false_positive_rate {
            fpr = false_positive_rate(k as f64, elements, bits_count as f64);
            if bits_count >= max_bit_count {
                trace!("false positive: {:.6}", fpr,);
                break;
            } else {
                bits_count = max_bit_count.min(bits_step + bits_count);
            }
        }
        Self {
            inner: BloomInner::InMemory(bitvec![Lsb0, u64; 0; bits_count]),
            hashers: Self::hashers(k),
            config,
            bits_count,
        }
    }

    /// Merge filters
    pub fn checked_add_assign<O: BloomDataProvider>(&mut self, other: &Bloom<O>) -> Option<()> {
        match (&mut self.inner, &other.inner) {
            (BloomInner::InMemory(inner), BloomInner::InMemory(other_inner))
                if inner.len() == other_inner.len() =>
            {
                inner
                    .as_mut_raw_slice()
                    .iter_mut()
                    .zip(other_inner.as_raw_slice())
                    .for_each(|(a, b)| *a |= *b);
                Some(())
            }
            _ => None,
        }
    }

    /// Read filter buffer
    pub async fn read_buffer(&self) -> Result<Vec<u64>> {
        match &self.inner {
            BloomInner::InMemory(buf) => Ok(buf.as_raw_slice().to_vec()),
            BloomInner::OnDisk(provider) => {
                let buf = provider.read_all().await?;
                let save: Save = bincode::deserialize(&buf)?;
                Ok(save.buf)
            }
        }
    }

    /// Clear filter
    pub fn clear(&mut self) {
        self.inner = BloomInner::InMemory(bitvec![Lsb0, u64; 0; self.bits_count]);
    }

    /// Offload filter buffer
    pub fn offload_from_memory(&mut self, provider: P) {
        self.inner = BloomInner::OnDisk(provider);
    }

    pub(crate) fn hashers(k: usize) -> Vec<AHasher> {
        trace!("@TODO create configurable hashers???");
        (0..k)
            .map(|i| AHasher::new_with_keys((i + 1) as u128, (i + 2) as u128))
            .collect()
    }

    fn save(&self) -> Option<Save> {
        if let BloomInner::InMemory(inner) = &self.inner {
            Some(Save {
                config: self.config.clone(),
                buf: inner.as_raw_slice().to_vec(),
                bits_count: inner.len(),
            })
        } else {
            None
        }
    }

    fn from(save: Save) -> Self {
        let mut inner = BitVec::from_vec(save.buf);
        inner.truncate(save.bits_count);
        Self {
            hashers: Self::hashers(save.config.hashers_count),
            config: save.config,
            inner: BloomInner::InMemory(inner),
            bits_count: save.bits_count,
        }
    }

    /// Serialize filter
    pub fn to_raw(&self) -> Result<Vec<u8>> {
        let save = self
            .save()
            .ok_or_else(|| anyhow::anyhow!("Filter buffer offloaded, can't serialize"))?;
        bincode::serialize(&save).map_err(Into::into)
    }

    /// Deserialize filter
    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        let save: Save = bincode::deserialize(buf)?;
        Ok(Self::from(save))
    }

    /// Create filter from provider
    pub async fn from_provider(provider: &P) -> Result<Self> {
        let buf = provider.read_all().await?;
        Self::from_raw(&buf)
    }

    /// Add item to filter
    pub fn add(&mut self, item: impl AsRef<[u8]>) -> Result<()> {
        if let BloomInner::InMemory(inner) = &mut self.inner {
            let mut hashers = self.hashers.clone();
            let len = inner.len() as u64;
            for h in hashers.iter_mut().map(|hasher| {
                hasher.write(item.as_ref());
                hasher.finish() % len
            }) {
                *inner
                    .get_mut(h as usize)
                    .expect("impossible due to mod by len") = true;
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Can't add to in-file filter"))
        }
    }

    pub(crate) async fn get_bit(&self, index: usize) -> Result<bool> {
        Ok(match &self.inner {
            BloomInner::InMemory(buf) => *buf.get(index).expect("unreachable"),
            BloomInner::OnDisk(provider) => {
                let start_pos = self.buffer_start_position()?;
                let pos = start_pos + (index / 8) as u64;
                let byte = provider.read_byte(pos).await?;
                let res = *byte
                    .view_bits::<Lsb0>()
                    .get(index as usize % 8)
                    .expect("unreachable")
                    .clone();
                res
            }
        })
    }

    // bincode write len as u64 before Vec elements. sizeof(config) + sizeof(u64)
    fn buffer_start_position(&self) -> Result<u64> {
        Ok(bincode::serialized_size(&self.config)? + std::mem::size_of::<u64>() as u64)
    }

    /// Get memory allocated for buffer in heap
    pub fn memory_allocated(&self) -> usize {
        match &self.inner {
            BloomInner::InMemory(inner) => inner.capacity() / 8,
            BloomInner::OnDisk(_) => 0,
        }
    }
}
