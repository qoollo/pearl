use super::prelude::*;
use bitvec::order::Lsb0;

// All usizes in structures are serialized as u64 in binary
#[derive(Debug, Clone)]
pub(crate) struct Bloom {
    inner: Option<BitVec<Lsb0, u64>>,
    bits_count: usize,
    hashers: Vec<AHasher>,
    config: Config,
}

impl Default for Bloom {
    fn default() -> Self {
        Self {
            inner: Some(Default::default()),
            bits_count: 0,
            hashers: vec![],
            config: Default::default(),
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

fn m_from_fpr(fpr: f64, k: f64, n: f64) -> f64 {
    -k * n / (1_f64 - fpr.powf(1_f64 / k)).ln()
}

fn bits_count_from_formula(config: &Config) -> usize {
    let max_bit_count = config.max_buf_bits_count; // 1Mb
    trace!("max bit count: {}", max_bit_count);
    let k = config.hashers_count;
    let n = config.elements as f64;
    trace!("bloom filter for {} elements", n);
    let mut bits_count = (n * k as f64 / 2_f64.ln()) as usize;
    let fpr = config.preferred_false_positive_rate;
    bits_count = bits_count.max(max_bit_count.min(m_from_fpr(fpr, k as f64, n) as usize));
    bits_count
}

#[allow(dead_code)]
fn false_positive_rate(k: f64, n: f64, m: f64) -> f64 {
    (1_f64 - 1_f64.exp().powf(-k * n / m)).powi(k as i32)
}

#[allow(dead_code)]
fn bits_count_via_iterations(config: &Config) -> usize {
    let max_bit_count = config.max_buf_bits_count; // 1Mb
    trace!("max bit count: {}", max_bit_count);
    let k = config.hashers_count;
    let elements = config.elements as f64;
    trace!("bloom filter for {} elements", elements);
    let mut bits_count = (elements * k as f64 / 2_f64.ln()) as usize;
    let bits_step = config.buf_increase_step;
    let mut fpr = 1_f64;
    while fpr > config.preferred_false_positive_rate {
        fpr = false_positive_rate(k as f64, elements, bits_count as f64);
        if bits_count >= max_bit_count {
            break;
        } else {
            bits_count = max_bit_count.min(bits_step + bits_count);
        }
    }
    bits_count
}

impl Bloom {
    pub fn new(config: Config) -> Self {
        let bits_count = bits_count_from_formula(&config);
        Self {
            inner: Some(bitvec![Lsb0, u64; 0; bits_count]),
            hashers: Self::hashers(config.hashers_count),
            config,
            bits_count,
        }
    }

    pub fn clear(&mut self) {
        self.inner = Some(bitvec![Lsb0, u64; 0; self.bits_count]);
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
        let save = self
            .save()
            .ok_or_else(|| anyhow::anyhow!("Filter buffer offloaded, can't serialize"))?;
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

    pub async fn contains_in_file<P: BloomDataProvider>(
        &self,
        provider: &P,
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
            let pos = start_pos + (index / 8);
            let byte = provider.read_byte(pos).await?;

            if !byte
                .view_bits::<Lsb0>()
                .get(index as usize % 8)
                .expect("unreachable")
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // bincode write len as u64 before Vec elements. sizeof(config) + sizeof(u64)
    fn buffer_start_position(&self) -> Result<u64> {
        Ok(bincode::serialized_size(&self.config)? + std::mem::size_of::<u64>() as u64)
    }

    pub fn memory_allocated(&self) -> usize {
        self.inner.as_ref().map_or(0, |buf| buf.capacity() / 8)
    }
}

#[async_trait::async_trait]
pub(crate) trait BloomDataProvider {
    async fn read_byte(&self, index: u64) -> Result<u8>;
    async fn read_all(&self) -> Result<Vec<u8>>;
}

mod tests {
    #[test]
    fn check_inversed_formula() {
        use super::Config;
        const EPSILON: f64 = 0.01;

        let config = Config::default();

        let inversed_formula_value = super::bits_count_from_formula(&config);
        let iterations_method_value = super::bits_count_via_iterations(&config);

        let min_value = iterations_method_value.min(inversed_formula_value) as f64;
        let diff =
            ((iterations_method_value as f64 - inversed_formula_value as f64) / min_value).abs();

        assert!(
            diff < EPSILON,
            "description: {}\ninversed formula value: {}, iteration value: {}, diff: {}",
            "bits_count relative diff is more than EPSILON",
            inversed_formula_value,
            iterations_method_value,
            diff
        );

        let inversed_formula_fpr = super::false_positive_rate(
            config.hashers_count as f64,
            config.elements as f64,
            inversed_formula_value as f64,
        );
        let iterations_method_fpr = super::false_positive_rate(
            config.hashers_count as f64,
            config.elements as f64,
            iterations_method_value as f64,
        );

        let min_val = inversed_formula_fpr.min(iterations_method_fpr);
        let diff = (inversed_formula_fpr - iterations_method_fpr).abs() / min_val;
        assert!(
            diff < EPSILON,
            "description: {}\nfpr (inversed formula): {}, fpr (iterations method): {}, diff: {}",
            "fpr relative diff is more than EPSILON",
            inversed_formula_fpr,
            iterations_method_fpr,
            diff
        );
    }
}
