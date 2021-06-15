use super::prelude::*;

/// NOTE: le and lt operations are written for big-endian format of keys
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct RangeFilter {
    min: Vec<u8>,
    max: Vec<u8>,
    initialized: bool,
}

impl RangeFilter {
    pub(crate) fn new() -> Self {
        Self {
            initialized: false,
            ..Default::default()
        }
    }

    pub(crate) fn add(&mut self, key: impl AsRef<[u8]>) {
        let key_slice = key.as_ref();
        if !self.initialized {
            self.min = key_slice.to_vec();
            self.max = key_slice.to_vec();
            self.initialized = true;
        } else if Self::lt(key_slice, &self.min) {
            self.min = key_slice.to_vec()
        } else if Self::lt(&self.max, key_slice) {
            self.max = key_slice.to_vec()
        }
    }

    pub(crate) fn lt(lv: &[u8], rv: &[u8]) -> bool {
        // NOTE: if it's keys are serialized in little-endian format you should use other
        // comparison method: Iterator::cmp(lv.iter().rev(), rv.iter().rev())
        lv < rv
    }

    pub(crate) fn le(lv: &[u8], rv: &[u8]) -> bool {
        // NOTE: if it's keys are serialized in little-endian format you should use other
        // comparison method: Iterator::cmp(lv.iter().rev(), rv.iter().rev())
        lv <= rv
    }

    pub(crate) fn contains(&self, key: impl AsRef<[u8]>) -> bool {
        let key_slice = key.as_ref();
        self.initialized && Self::le(&self.min, key_slice) && Self::le(key_slice, &self.max)
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
