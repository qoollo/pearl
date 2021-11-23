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
