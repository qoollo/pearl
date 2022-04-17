use super::*;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct RangeFilter<K>
where
    for<'a> K: Key<'a>,
{
    #[serde(serialize_with = "serialize_key", deserialize_with = "deserialize_key")]
    min: K,
    #[serde(serialize_with = "serialize_key", deserialize_with = "deserialize_key")]
    max: K,
    initialized: bool,
}

#[async_trait::async_trait]
impl<K> FilterTrait<K> for RangeFilter<K>
where
    for<'a> K: Key<'a>,
{
    fn add(&mut self, key: &K) {
        self.add(key);
    }

    fn contains_fast(&self, key: &K) -> FilterResult {
        if self.contains(key) {
            FilterResult::NeedAdditionalCheck
        } else {
            FilterResult::NotContains
        }
    }

    fn checked_add_assign(&mut self, other: &Self) -> bool {
        self.add(&other.min);
        self.add(&other.max);
        true
    }

    fn clear_filter(&mut self) {
        self.clear()
    }
}

impl<K> RangeFilter<K>
where
    for<'a> K: Key<'a>,
{
    /// Create filter
    pub fn new() -> Self {
        Self {
            initialized: false,
            ..Default::default()
        }
    }

    /// Add key to filter
    pub fn add(&mut self, key: &K) {
        if !self.initialized {
            self.min = key.clone();
            self.max = key.clone();
            self.initialized = true;
        } else if key < &self.min {
            self.min = key.clone();
        } else if key > &self.max {
            self.max = key.clone()
        }
    }

    /// Check if key contains in filter
    pub fn contains(&self, key: &K) -> bool {
        self.initialized && &self.min <= key && key <= &self.max
    }

    /// Clear filter
    pub fn clear(&mut self) {
        self.initialized = false;
    }

    /// Create filter from raw bytes
    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        bincode::deserialize(&buf).map_err(|e| e.into())
    }

    /// Convert filter to raw bytes
    pub fn to_raw(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| e.into())
    }
}

fn serialize_key<K, S>(key: &K, serializer: S) -> Result<S::Ok, S::Error>
where
    for<'a> K: Key<'a>,
    S: serde::Serializer,
{
    serializer.serialize_bytes(key.as_ref())
}

fn deserialize_key<'de, K, D>(deserializer: D) -> Result<K, D::Error>
where
    for<'a> K: Key<'a>,
    D: serde::Deserializer<'de>,
{
    struct KeyVisitor<K>(PhantomData<K>);
    impl<'de, K> serde::de::Visitor<'de> for KeyVisitor<K>
    where
        for<'a> K: Key<'a>,
    {
        type Value = K;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bytes")
        }

        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.into())
        }
    }

    deserializer.deserialize_byte_buf(KeyVisitor(PhantomData))
}

#[cfg(test)]
mod tests {
    use crate::{Key, RefKey};

    use super::RangeFilter;

    const LEN: u16 = 8;

    #[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
    struct ProperKey(Vec<u8>);

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    struct RefProperKey<'a>(&'a [u8]);

    impl<'a> From<&'a [u8]> for RefProperKey<'a> {
        fn from(v: &'a [u8]) -> Self {
            Self(v)
        }
    }

    impl AsRef<[u8]> for ProperKey {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    impl From<Vec<u8>> for ProperKey {
        fn from(mut v: Vec<u8>) -> Self {
            v.resize(ProperKey::LEN as usize, 0);
            Self(v)
        }
    }

    impl<'a> RefKey<'a> for RefProperKey<'a> {}

    impl<'a> Key<'a> for ProperKey {
        const LEN: u16 = LEN;

        type Ref = RefProperKey<'a>;
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct WrongKey(Vec<u8>);

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    struct RefWrongKey<'a>(&'a [u8]);

    impl AsRef<[u8]> for WrongKey {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    impl From<Vec<u8>> for WrongKey {
        fn from(mut v: Vec<u8>) -> Self {
            v.resize(WrongKey::LEN as usize, 0);
            Self(v)
        }
    }

    impl PartialOrd for WrongKey {
        fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
            Some(std::cmp::Ordering::Equal)
        }
    }

    impl Ord for WrongKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    impl<'a> From<&'a [u8]> for RefWrongKey<'a> {
        fn from(v: &'a [u8]) -> Self {
            Self(v)
        }
    }

    impl<'a> RefKey<'a> for RefWrongKey<'a> {}

    impl<'a> Key<'a> for WrongKey {
        const LEN: u16 = LEN;

        type Ref = RefWrongKey<'a>;
    }

    fn to_key<K: From<Vec<u8>>>(i: usize) -> K {
        let mut vec = i.to_le_bytes().to_vec();
        vec.resize(LEN as usize, 0);
        vec.into()
    }

    #[test]
    fn test_range_index_key_cmp_proper_key() {
        let mut filter: RangeFilter<ProperKey> = RangeFilter::new();
        for key in [50, 100, 150].iter().map(|&i| to_key(i)) {
            filter.add(&key);
        }
        let less_key = to_key(25);
        let in_key = to_key(75);
        let greater_key = to_key(175);
        assert!(!filter.contains(&less_key));
        assert!(!filter.contains(&greater_key));
        assert!(filter.contains(&in_key));
    }

    #[test]
    fn test_range_index_key_cmp_wrong_key() {
        let mut filter: RangeFilter<ProperKey> = RangeFilter::new();
        for key in [50, 100, 150].iter().map(|&i| to_key(i)) {
            filter.add(&key);
        }
        let buf = bincode::serialize(&filter).unwrap();
        let wrong_key_filter: RangeFilter<WrongKey> = RangeFilter::from_raw(&buf).unwrap();

        let less_key = to_key(25);
        let in_key = to_key(75);
        let greater_key = to_key(175);
        assert!(wrong_key_filter.contains(&less_key));
        assert!(wrong_key_filter.contains(&greater_key));
        assert!(wrong_key_filter.contains(&in_key));
    }
}
