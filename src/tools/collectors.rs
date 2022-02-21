use std::collections::BTreeSet;

use super::prelude::*;

/// Collect data about blob
#[derive(Debug)]
pub struct BlobSummaryCollector {
    header: BlobHeader,
    records: usize,
    keys: BTreeSet<Vec<u8>>,
}

impl BlobSummaryCollector {
    /// Collect data about blob by path
    pub fn from_path(path: &Path) -> Result<Self> {
        let mut reader = BlobReader::from_path(&path)?;
        let header = reader.read_header()?;
        let mut collector = Self::empty(header);
        while !reader.is_eof() {
            let record = reader.read_record(false)?;
            collector.add_record(record);
        }
        Ok(collector)
    }

    fn empty(header: BlobHeader) -> Self {
        Self {
            header,
            records: Default::default(),
            keys: Default::default(),
        }
    }

    fn add_record(&mut self, record: Record) {
        self.records += 1;
        self.keys.insert(record.header().key().to_vec());
    }

    /// Count of records
    pub fn records(&self) -> usize {
        self.records
    }

    /// Count of unique keys
    pub fn unique_keys_count(&self) -> usize {
        self.keys.len()
    }

    /// Blob header magic byte
    pub fn header_magic_byte(&self) -> u64 {
        self.header.magic_byte
    }

    /// Blob header version
    pub fn header_version(&self) -> u32 {
        self.header.version
    }

    /// Blob header flags
    pub fn header_flags(&self) -> u64 {
        self.header.flags
    }
}

/// Collect data about blob
#[derive(Debug)]
pub struct IndexSummaryCollector {
    header: IndexHeader,
    records: usize,
    keys: BTreeSet<Vec<u8>>,
}

impl IndexSummaryCollector {
    /// Collect data about blob by path
    pub fn from_path(path: &Path) -> Result<Self> {
        let header = read_index_header(path)?;
        let headers = read_index(path)?;
        let mut collector = Self::empty(header);
        for headers in headers.values() {
            for header in headers {
                collector.add_record(header.clone());
            }
        }
        Ok(collector)
    }

    fn empty(header: IndexHeader) -> Self {
        Self {
            header,
            records: Default::default(),
            keys: Default::default(),
        }
    }

    fn add_record(&mut self, header: record::Header) {
        self.records += 1;
        self.keys.insert(header.key().to_vec());
    }

    /// Count of records
    pub fn records_readed(&self) -> usize {
        self.records
    }

    /// Count of unique keys
    pub fn unique_keys_count(&self) -> usize {
        self.keys.len()
    }

    /// Index version
    pub fn header_version(&self) -> u8 {
        self.header.version()
    }

    /// Index hash
    pub fn header_hash(&self) -> Vec<u8> {
        self.header.hash.clone()
    }

    /// Meta size
    pub fn header_meta_size(&self) -> usize {
        self.header.meta_size
    }

    /// Records count
    pub fn header_records_count(&self) -> usize {
        self.header.records_count
    }

    /// Is written
    pub fn header_is_written(&self) -> bool {
        self.header.is_written()
    }

    /// Record header size
    pub fn header_record_header_size(&self) -> usize {
        self.header.record_header_size
    }

    /// Key size
    pub fn header_key_size(&self) -> u16 {
        self.header.key_size()
    }
}
