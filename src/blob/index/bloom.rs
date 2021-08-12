use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Bloom {
    inner: Option<BitVec>,
    bits_count: usize,
    hashers: Vec<AHasher>,
    config: Config,
}

impl Default for Bloom {
    fn default() -> Self {
        Self {
            inner: Some(Default::default()),
            bits_count: Default::default(),
            hashers: Default::default(),
            config: Config::default(),
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
            inner: Some(bitvec![0; bits_count]),
            hashers: Self::hashers(k),
            config,
            bits_count,
        }
    }

    pub fn clear(&mut self) {
        self.inner = Some(bitvec![0; self.bits_count]);
    }

    pub fn offload_from_memory(&mut self) {
        self.inner = None;
    }

    pub fn hashers(k: usize) -> Vec<AHasher> {
        trace!("@TODO create configurable hashers???");
        (0..k)
            .map(|i| AHasher::new_with_keys((i + 1) as u128, (i + 2) as u128))
            .collect()
    }

    fn save(&self) -> Option<Save> {
        if let Some(inner) = &self.inner {
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
            inner: Some(inner),
            bits_count: save.bits_count,
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

    pub fn add(&mut self, item: impl AsRef<[u8]>) -> Result<()> {
        if let Some(inner) = &mut self.inner {
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

    pub fn contains_in_memory(&self, item: impl AsRef<[u8]>) -> Option<bool> {
        if let Some(inner) = &self.inner {
            let mut hashers = self.hashers.clone();
            let len = inner.len() as u64;
            if len == 0 {
                return Some(false);
            }
            let result = hashers
                .iter_mut()
                .map(|hasher| {
                    hasher.write(item.as_ref());
                    hasher.finish() % len
                })
                .all(|i| *inner.get(i as usize).expect("unreachable"));
            Some(result)
        } else {
            None
        }
    }

    pub async fn contains_in_file<F: FileIndexTrait>(
        &self,
        findex: &F,
        item: impl AsRef<[u8]>,
    ) -> Result<bool> {
        let mut hashers = self.hashers.clone();
        if self.bits_count == 0 {
            return Ok(false);
        }
        let start_pos = self.buffer_start_position()?;
        for index in hashers.iter_mut().map(|hasher| {
            hasher.write(item.as_ref());
            hasher.finish() % self.bits_count as u64
        }) {
            let pos = start_pos + (index / 8) as usize;
            let byte = findex.read_meta_at(pos).await?;
            if byte == 0 {
                return Ok(false);
            }
            /*
            if !byte
                .view_bits::<bitvec::order::Lsb0>()
                .get(index as usize % 8)
                .expect("unreachable")
            {
                return Ok(false);
            }
            */
        }
        Ok(true)
    }

    // bincode write len as u64 before Vec elements
    fn buffer_start_position(&self) -> Result<usize> {
        Ok(bincode::serialized_size(&self.config)? as usize + 8)
    }

    // sizeof(u64) / sizeof(usize). bincode serialize usize as u64
    fn multiplier() -> usize {
        8 / std::mem::size_of::<usize>()
    }
}
