use super::prelude::*;

#[derive(Debug, Default, Clone)]
pub(crate) struct Bloom {
    inner: BitVec,
    hashers: Vec<AHasher>,
    config: Config,
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
    buf: Vec<usize>,
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

impl Bloom {
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
            inner: bitvec![0; bits_count],
            hashers: Self::hashers(k),
            config,
        }
    }

    pub fn clear(&mut self) {
        self.inner = bitvec![0; self.inner.len()];
    }

    pub fn hashers(k: usize) -> Vec<AHasher> {
        trace!("@TODO create configurable hashers???");
        (0..k)
            .map(|i| AHasher::new_with_keys((i + 1) as u128, (i + 2) as u128))
            .collect()
    }

    fn save(&self) -> Save {
        Save {
            config: self.config.clone(),
            buf: self.inner.as_raw_slice().to_vec(),
            bits_count: self.inner.len(),
        }
    }

    fn from(save: Save) -> Self {
        let mut inner = BitVec::from_vec(save.buf);
        inner.truncate(save.bits_count);
        Self {
            hashers: Self::hashers(save.config.hashers_count),
            config: save.config,
            inner,
        }
    }

    pub fn to_raw(&self) -> Result<Vec<u8>> {
        let save = self.save();
        bincode::serialize(&save).map_err(Into::into)
    }

    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        let save: Save = bincode::deserialize(buf)?;
        Ok(Self::from(save))
    }

    pub fn add(&mut self, item: impl AsRef<[u8]>) {
        let mut hashers = self.hashers.clone();
        let len = self.inner.len() as u64;
        for h in hashers.iter_mut().map(|hasher| {
            hasher.write(item.as_ref());
            hasher.finish() % len
        }) {
            *self
                .inner
                .get_mut(h as usize)
                .expect("impossible due to mod by len") = true;
        }
    }

    pub fn contains(&self, item: impl AsRef<[u8]>) -> bool {
        let mut hashers = self.hashers.clone();
        let len = self.inner.len() as u64;
        if len == 0 {
            return false;
        }
        hashers
            .iter_mut()
            .map(|hasher| {
                hasher.write(item.as_ref());
                hasher.finish() % len
            })
            .all(|i| *self.inner.get(i as usize).expect("unreachable"))
    }
}
