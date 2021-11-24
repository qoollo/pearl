use super::prelude::*;

/// NOTE: le and lt operations are written for big-endian format of keys
#[derive(Debug, Default)]
pub(crate) struct RangeFilter<K> {
    min: K,
    max: K,
    initialized: bool,
}

impl<K: Key> RangeFilter<K> {
    pub(crate) fn new() -> Self {
        Self {
            initialized: false,
            ..Default::default()
        }
    }

    pub(crate) fn add(&mut self, key: &K) {
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

    pub(crate) fn contains(&self, key: &K) -> bool {
        self.initialized && &self.min <= key && key <= &self.max
    }

    pub(crate) fn clear(&mut self) {
        self.initialized = false;
    }

    pub(crate) fn from_raw(buf: &[u8]) -> Result<Self> {
        bincode::deserialize(&buf).map_err(|e| e.into())
    }

    pub(crate) fn to_raw(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| e.into())
    }
}

const STRUCT_NAME: &str = "RangeFilter";
const MIN_FIELD_NAME: &str = "min";
const MAX_FIELD_NAME: &str = "max";
const INIT_FIELD_NAME: &str = "initialized";

impl<K: Key> serde::Serialize for RangeFilter<K> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct(STRUCT_NAME, 3)?;
        state.serialize_field(MIN_FIELD_NAME, &self.min.to_vec())?;
        state.serialize_field(MAX_FIELD_NAME, &self.max.to_vec())?;
        state.serialize_field(INIT_FIELD_NAME, &self.initialized)?;
        state.end()
    }
}

impl<'de, K: Key> serde::Deserialize<'de> for RangeFilter<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RangeFilterVisitor<K>(PhantomData<K>);

        impl<'de, K: Key> serde::de::Visitor<'de> for RangeFilterVisitor<K> {
            type Value = RangeFilter<K>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("range filter struct")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let min = seq.next_element::<Vec<u8>>()?.map(|v| v.into());
                let max = seq.next_element::<Vec<u8>>()?.map(|v| v.into());
                let init = seq.next_element::<bool>()?;
                Ok(RangeFilter::<K> {
                    min: min.expect("min not found"),
                    max: max.expect("max not found"),
                    initialized: init.expect("initialized not found"),
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut min: Option<K> = None;
                let mut max: Option<K> = None;
                let mut initialized: Option<bool> = None;
                while let Some(key) = map.next_key::<&str>()? {
                    match key {
                        MIN_FIELD_NAME => {
                            min = Some(map.next_value::<Vec<u8>>()?.into());
                        }
                        MAX_FIELD_NAME => {
                            max = Some(map.next_value::<Vec<u8>>()?.into());
                        }
                        INIT_FIELD_NAME => initialized = Some(map.next_value::<bool>()?),
                        k => panic!("received wrong field name: {}", k),
                    }
                }
                Ok(RangeFilter::<K> {
                    min: min.expect("min not found"),
                    max: max.expect("max not found"),
                    initialized: initialized.expect("initialized not found"),
                })
            }
        }

        const FIELDS: &'static [&'static str] = &[MIN_FIELD_NAME, MAX_FIELD_NAME, INIT_FIELD_NAME];
        deserializer.deserialize_struct(STRUCT_NAME, FIELDS, RangeFilterVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use crate::Key;

    use super::RangeFilter;

    const LEN: u16 = 8;

    #[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
    struct ProperKey(Vec<u8>);

    impl AsRef<[u8]> for ProperKey {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    impl From<Vec<u8>> for ProperKey {
        fn from(v: Vec<u8>) -> Self {
            Self(v)
        }
    }

    impl Key for ProperKey {
        const LEN: u16 = LEN;
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct WrongKey(Vec<u8>);

    impl AsRef<[u8]> for WrongKey {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    impl From<Vec<u8>> for WrongKey {
        fn from(v: Vec<u8>) -> Self {
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

    impl Key for WrongKey {
        const LEN: u16 = LEN;
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
