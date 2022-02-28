use super::prelude::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct IndexHeader {
    pub records_count: usize,
    // contains serialized size of record headers, which allows to calculate offset in
    // case of `OnDisk` state of indices
    pub record_header_size: usize,
    pub meta_size: usize,
    pub hash: Vec<u8>,
    // this field also contains `written` bit (the first one)
    // to get the version, you should proceed `version >> 1`
    pub(crate) version: u8,
    pub(crate) blob_size: u64,
}

impl IndexHeader {
    pub fn new(
        record_header_size: usize,
        records_count: usize,
        meta_size: usize,
        blob_size: u64,
    ) -> Self {
        Self {
            records_count,
            record_header_size,
            meta_size,
            blob_size,
            ..Self::default()
        }
    }

    pub fn with_hash(
        record_header_size: usize,
        records_count: usize,
        meta_size: usize,
        hash: Vec<u8>,
    ) -> Self {
        Self {
            records_count,
            record_header_size,
            meta_size,
            hash,
            ..Self::default()
        }
    }

    pub(crate) fn set_written(&mut self, state: bool) {
        self.version = self.version & !1;
        self.version |= state as u8;
    }

    pub(crate) fn is_written(&self) -> bool {
        self.version & 1 == 1
    }

    pub(crate) fn version(&self) -> u8 {
        self.version >> 1
    }

    #[allow(dead_code)]
    pub(crate) fn set_version(&mut self, version: u8) {
        let written = self.version & 1;
        self.version = (version << 1) | written;
    }

    pub(crate) fn serialized_size_default() -> bincode::Result<u64> {
        let header = Self::default();
        header.serialized_size()
    }

    #[inline]
    pub fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    #[inline]
    pub(crate) fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(buf)
    }
}

impl Default for IndexHeader {
    fn default() -> Self {
        Self {
            records_count: 0,
            record_header_size: 0,
            meta_size: 0,
            blob_size: 0,
            hash: vec![0; ring::digest::SHA256.output_len],
            version: HEADER_VERSION << 1,
        }
    }
}
