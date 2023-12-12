//! aHash version 0.7.4 (commit ffa04fcb81f39755f636c75c9b7aa06533c0ae75) is used for `Bloom`
//! filter. Several key tests were performed in order to determine the best hash function for this
//! scenario. Results for aHash were compared with Wyhash as the second best after aHash.
//!
//! Results are the following:
//! 1. [SMHasher](https://github.com/rurban/smhasher)
//!     a. AHash: All tests passed
//!     b. Wyhash: PerlinNoise test failed
//! 2. [SMHasher3](https://gitlab.com/fwojcik/smhasher3)
//!     a. AHash: 118 / 188 passed
//!     b. Wyhash: 160 / 188 passed
//! 3. Mean Bloom filter's false-positive amount of 100k keys in 1M-sized bitvec:
//!     a. AHash: 29574.6
//!     b. Wyhash: 29546
//! 4. Average Speed:
//!     a. AHash: 23.20 cycles/hash
//!     b. Wyhash: 22.27 cycles/hash
//!

use super::*;
use ahash::AHasher;
use atomic_bitvec::*;
use std::hash::Hasher;
use std::sync::RwLock;
use std::sync::OnceLock;

// All usizes in structures are serialized as u64 in binary
/// Bloom filter
pub struct Bloom {
    /// Bit vector. None - means offloaded
    inner: Option<AtomicBitVec>,
    /// Protects `save` and `checked_add_assign` from seeing partial updates
    snapshot_protector: RwLock<()>,
    bits_count: usize,
    hashers: Box<[AHasher]>,
    config: Arc<Config>
}

impl Debug for Bloom {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        struct InnerDebug(u64, usize);
        impl Debug for InnerDebug {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.write_fmt(format_args!("{} ones from {}", self.0, self.1))
            }
        }
        f.debug_struct("Bloom")
            .field(
                "inner",
                &self.inner.as_ref().map(|x| {
                    InnerDebug(x.count_ones(), x.len())
                }),
            )
            .field("bits_count", &self.bits_count)
            .finish()
    }
}

impl Clone for Bloom {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            snapshot_protector: RwLock::new(()),
            bits_count: self.bits_count,
            hashers: self.hashers.clone(),
            config: self.config.clone()
        }
    }
}

#[async_trait::async_trait]
impl<K> FilterTrait<K> for Bloom
where
    K: AsRef<[u8]> + Sync + Send + Debug,
{
    fn add(&self, key: &K) {
        let _ = self.add(key);
    }

    fn contains_fast(&self, key: &K) -> FilterResult {
        self.contains_in_memory(key).unwrap_or_default()
    }

    async fn contains<P: BloomDataProvider>(&self, provider: &P, key: &K) -> FilterResult {
        if let Some(res) = self.contains_in_memory(key) {
            res
        } else {
            let res = self.contains_in_file(provider, key).await;
            res.map_err(|e| {
                error!("Failed to check filter for key {:?}: {}", key, e);
                e
            })
            .unwrap_or_default()
        }
    }

    fn offload_filter(&mut self) -> usize {
        self.offload_from_memory()
    }

    fn checked_add_assign(&mut self, other: &Self) -> bool {
        self.checked_add_assign(other)
    }

    fn memory_allocated(&self) -> usize {
        self.memory_allocated()
    }

    fn clear_filter(&mut self) {
        self.clear();
    }

    fn is_filter_offloaded(&self) -> bool {
        self.is_offloaded()
    }
}

/// Bloom filter configuration parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    /// records count in one blob.
    pub elements: usize,
    /// number of hash functions, the more hash functions.
    /// you have, the slower bloom filter, and the quicker it fills up. If you
    /// have too few, however, you may suffer too many false positives.
    pub hashers_count: usize,
    /// number of bits in the inner buffer.
    pub max_buf_bits_count: usize,
    /// filter buf increase value. Not used anymore
    pub buf_increase_step: usize,
    /// filter incrementally increases buffer
    /// size by step and checks result false positive rate to be less than param.
    /// It stops once buffer reaches size of max_buf_bits_count.
    pub preferred_false_positive_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Save {
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

impl Config {
    /// Config to create empty bloom filter
    pub fn empty() -> Self {
        Self {
            elements: 0,
            hashers_count: 0,
            max_buf_bits_count: 0,
            buf_increase_step: 0,
            preferred_false_positive_rate: 0.0
        }
    }

    /// Returns single shared instance of [`Config`] if the current is equal to it,
    /// otherwise just wraps `Self` with `Arc`
    pub(crate) fn make_shared(self) -> Arc<Self> {
        static SHARED_INSTANCE: OnceLock<Arc<Config>> = OnceLock::new();

        let local_instance = SHARED_INSTANCE.get_or_init(|| Arc::new(self.clone()));
        if self == **local_instance {
            local_instance.clone()
        } else {
            Arc::new(self)
        }
    }
}

fn m_from_fpr(fpr: f64, k: f64, n: f64) -> f64 {
    -k * n / (1_f64 - fpr.powf(1_f64 / k)).ln()
}

fn bits_count_from_formula(config: &Config) -> usize {
    if config.hashers_count == 0 {
        return 0;
    }
    if config.max_buf_bits_count == 0 {
        return 64;
    }

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
    /// Create bloom filter with shared config
    pub(crate) fn new_from_shared_config(config: Arc<Config>) -> Self {
        let bits_count = bits_count_from_formula(&config);
        Self {
            inner: Some(AtomicBitVec::new(bits_count)),
            snapshot_protector: RwLock::new(()),
            bits_count: bits_count,
            hashers: Self::hashers(config.hashers_count),
            config: config
        }
    }
    /// Create new bloom filter
    pub fn new(config: Config) -> Self {
        Self::new_from_shared_config(config.make_shared())
    }

    /// Creates empty bloom filter
    pub fn empty() -> Self {
        Self {
            inner: Some(AtomicBitVec::new(0)),
            snapshot_protector: RwLock::new(()),
            bits_count: 0,
            hashers: Self::hashers(0),
            config: Arc::new(Config::empty())
        }
    }

    /// Acquired write lock on RwLock pair.
    /// Preserve ordering by address comparison. This prevents deadlocks on cycle updates
    fn acquire_snapshot_protection_ordered<'rw>(a: &'rw RwLock<()>, b: &'rw RwLock<()>) -> (std::sync::RwLockWriteGuard<'rw, ()>, Option<std::sync::RwLockWriteGuard<'rw, ()>>) {
        let a_ptr: *const RwLock<()> = a;
        let b_ptr: *const RwLock<()> = b;

        // First lock on RwLock with lower memory address. This preserved order and prevents deadlock on cycle updates
        if a_ptr < b_ptr {
            let g1 = a.write().expect("write lock acquired");
            let g2 = b.write().expect("write lock acquired");
            (g1, Some(g2))
        } else if a_ptr > b_ptr {
            let g2 = b.write().expect("write lock acquired");
            let g1 = a.write().expect("write lock acquired");
            (g1, Some(g2))
        } else {
            // Identical references
            let g1 = a.write().expect("write lock acquired");
            (g1, None)
        }
    }

    /// Merge filters
    #[must_use]
    pub fn checked_add_assign(&mut self, other: &Bloom) -> bool {
        if self.hashers.len() != other.hashers.len() {
            // Hashers should be identical
            return false;
        }

        match (&mut self.inner, &other.inner) {
            (Some(inner), Some(other_inner)) if inner.len() == other_inner.len() => {
                let _guard_pair = Self::acquire_snapshot_protection_ordered(&self.snapshot_protector, &other.snapshot_protector);

                inner.or_with(other_inner).expect("AtomicBitVec::or_with expects to be successful");
                true
            }
            _ => false,
        }
    }

    /// Set in-memory filter buffer to zeroed array
    pub fn clear(&mut self) {
        self.inner = Some(AtomicBitVec::new(self.bits_count));
    }

    /// Check if filter offloaded
    pub fn is_offloaded(&self) -> bool {
        self.inner.is_none()
    }

    /// Clear in-memory filter buffer
    pub fn offload_from_memory(&mut self) -> usize {
        let freed = self.inner.as_ref().map(|x| x.size_in_mem()).unwrap_or(0);
        self.inner = None;
        freed
    }

    fn hashers(k: usize) -> Box<[AHasher]> {
        // TODO create configurable hashers???
        let mut result = Vec::with_capacity(k);
        for i in 0..k {
            result.push(AHasher::new_with_keys((i + 1) as u128, (i + 2) as u128));
        }

        return result.into_boxed_slice();
    }

    fn save(&self) -> Option<Save> {
        if let Some(inner) = &self.inner {
            // Protect from modification while `inner.to_raw_vec()` executing
            let _guard = self.snapshot_protector.write().expect("write lock acquired");

            Some(Save {
                config: self.config.as_ref().clone(),
                buf: inner.to_raw_vec(),
                bits_count: inner.len(),
            })
        } else {
            None
        }
    }

    fn from(save: Save) -> Result<Self> {
        use serde::de::Error;
        
        let inner = 
            AtomicBitVec::from_raw_slice(&save.buf, save.bits_count)
                .map_err(|e| bincode::Error::custom(e))?;

        Ok(Self {
            inner: Some(inner),
            snapshot_protector: RwLock::new(()),
            bits_count: save.bits_count,
            hashers: Self::hashers(save.config.hashers_count),
            config: Arc::new(save.config),
        })
    }

    /// Serialize filter to bytes
    pub fn to_raw(&self) -> Result<Vec<u8>> {
        let save = self
            .save()
            .ok_or_else(|| anyhow::anyhow!("Filter buffer offloaded, can't serialize"))?;
        bincode::serialize(&save).map_err(Into::into)
    }

    /// Deserialize filter from bytes
    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        let save: Save = bincode::deserialize(buf)?;
        Ok(Self::from(save)?)
    }

    /// Add value to filter
    pub fn add(&self, item: impl AsRef<[u8]>) -> Result<()> {
        if let Some(inner) = &self.inner {
            let len = inner.len() as u64;
            if len == 0 {
                return Ok(());
            }

            // snapshot_protector prevents partial modifications to be observable in other functions
            let _guard = self.snapshot_protector.read().expect("read lock acquired");

            for mut hasher in self.hashers.iter().cloned() {
                hasher.write(item.as_ref());
                if let Some(i) = hasher.finish().checked_rem(len) {
                    inner.set(i as usize, true);
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Can't add to in-file filter"))
        }
    }

    /// Check filter in-memory (if not offloaded)
    pub fn contains_in_memory(&self, item: impl AsRef<[u8]>) -> Option<FilterResult> {
        if let Some(inner) = &self.inner {
            let len = inner.len() as u64;
            // Check because NeedAdditionalCheck will be returned is self.hashers is empty
            if len == 0 {
                return None;
            }
            for mut hasher in self.hashers.iter().cloned() {
                hasher.write(item.as_ref());
                if let Some(i) = hasher.finish().checked_rem(len) {
                    if !inner.get(i as usize) {
                        return Some(FilterResult::NotContains);
                    }
                }
            }
            Some(FilterResult::NeedAdditionalCheck)
        } else {
            None
        }
    }

    /// Check filter by reading bits from file
    pub async fn contains_in_file<P: BloomDataProvider>(
        &self,
        provider: &P,
        item: impl AsRef<[u8]>,
    ) -> Result<FilterResult> {
        if self.bits_count == 0 {
            return Ok(FilterResult::NeedAdditionalCheck);
        }
        let mut hashers = self.hashers.clone();
        let start_pos = self.buffer_start_position().unwrap();
        for index in hashers.iter_mut().map(|hasher| {
            hasher.write(item.as_ref());
            hasher.finish() % self.bits_count as u64
        }) {
            let (offset, bit_mask) = OffsetAndMaskCalculator::offset_and_mask_u8(index);
            let byte = provider.read_byte(start_pos + offset).await?;

            if !OffsetAndMaskCalculator::get_bit_u8(byte, bit_mask) {
                return Ok(FilterResult::NotContains);
            }
        }
        Ok(FilterResult::NeedAdditionalCheck)
    }

    // bincode write len as u64 before Vec elements. sizeof(config) + sizeof(u64)
    fn buffer_start_position(&self) -> Result<u64> {
        Ok(bincode::serialized_size(self.config.as_ref())? + std::mem::size_of::<u64>() as u64)
    }

    /// Get amount of memory allocated for filter
    pub fn memory_allocated(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(0, |buf| buf.size_in_mem())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArrayKey;

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

    #[test]
    fn test_simple_operations() {
        let bloom = Bloom::new(Config {
            elements: 1000,
            hashers_count: 2,
            max_buf_bits_count: 1000,
            preferred_false_positive_rate: 0.01,
            buf_increase_step: 1
        });

        assert_eq!(false, bloom.is_offloaded());

        let k1 = ArrayKey::from([1u8; 1]);
        let k2 = ArrayKey::from([50u8; 1]);
        let k3 = ArrayKey::from([255u8; 1]);

        bloom.add(k1.clone()).expect("add");
        bloom.add(k2.clone()).expect("add2");

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k2));
        assert_eq!(FilterResult::NotContains, bloom.contains_fast(&k3));
    }

    #[test]
    fn test_empty_not_fails() {
        let bloom = Bloom::empty();

        assert_eq!(false, bloom.is_offloaded());

        let k1 = ArrayKey::from([1u8; 1]);
        let k2 = ArrayKey::from([50u8; 1]);
        let k3 = ArrayKey::from([255u8; 1]);

        bloom.add(k1.clone()).expect("add");
        bloom.add(k2.clone()).expect("add2");

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k2));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k3));
    }

    #[test]
    fn test_merge() {
        let cfg = Config {
            elements: 1000,
            hashers_count: 2,
            max_buf_bits_count: 1000,
            preferred_false_positive_rate: 0.01,
            buf_increase_step: 1
        };

        let mut bloom_a = Bloom::new(cfg.clone());
        let bloom_b = Bloom::new(cfg);

        let k1 = ArrayKey::from([1u8; 1]);
        let k2 = ArrayKey::from([50u8; 1]);

        bloom_a.add(k1.clone()).expect("add");
        bloom_b.add(k2.clone()).expect("add2");

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_a.contains_fast(&k1));
        assert_eq!(FilterResult::NotContains, bloom_a.contains_fast(&k2));

        assert_eq!(FilterResult::NotContains, bloom_b.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_b.contains_fast(&k2));

        assert_eq!(true, bloom_a.checked_add_assign(&bloom_b));

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_a.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_a.contains_fast(&k2));
    }

    #[test]
    fn test_clone() {
        let cfg = Config {
            elements: 1000,
            hashers_count: 2,
            max_buf_bits_count: 1000,
            preferred_false_positive_rate: 0.01,
            buf_increase_step: 1
        };

        let bloom_a = Bloom::new(cfg.clone());

        let k1 = ArrayKey::from([1u8; 1]);
        let k2 = ArrayKey::from([50u8; 1]);

        bloom_a.add(k1.clone()).expect("add");
        
        let bloom_b = bloom_a.clone();
        bloom_b.add(k2.clone()).expect("add2");

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_a.contains_fast(&k1));
        assert_eq!(FilterResult::NotContains, bloom_a.contains_fast(&k2));

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_b.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom_b.contains_fast(&k2));
    }


    #[test]
    fn test_backward_compat() {
        let raw_buf: Vec<u64> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16384, 0, 0, 0, 0, 0, 0, 524288, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4398046511104, 0, 0, 0, 0, 0, 0, 1073741824, 0];

        let cfg = Config {
            elements: 1000,
            hashers_count: 2,
            max_buf_bits_count: 1000,
            preferred_false_positive_rate: 0.01,
            buf_increase_step: 1
        };

        let sv = Save {
            bits_count: 2885,
            buf: raw_buf.clone(),
            config: cfg
        };

        let bloom = Bloom::from(sv).expect("success");

        let k1 = ArrayKey::from([1u8; 1]);
        let k2 = ArrayKey::from([50u8; 1]);
        let k3 = ArrayKey::from([255u8; 1]);

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k2));
        assert_eq!(FilterResult::NotContains, bloom.contains_fast(&k3));

        let sv2 = bloom.save().unwrap();
        assert_eq!(raw_buf, sv2.buf);
    }

    #[test]
    fn test_backward_compat_serialization() {
        let serialized: Vec<u8> = vec![232, 3, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 123, 20, 174, 71, 225, 122, 132, 63, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 69, 11, 0, 0, 0, 0, 0, 0];

        let bloom = Bloom::from_raw(&serialized[..]).expect("success");

        let k1 = ArrayKey::from([1u8; 1]);
        let k2 = ArrayKey::from([50u8; 1]);
        let k3 = ArrayKey::from([255u8; 1]);

        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k1));
        assert_eq!(FilterResult::NeedAdditionalCheck, bloom.contains_fast(&k2));
        assert_eq!(FilterResult::NotContains, bloom.contains_fast(&k3));

        let serialized2 = bloom.to_raw().unwrap();
        assert_eq!(serialized, serialized2);
    }
}
