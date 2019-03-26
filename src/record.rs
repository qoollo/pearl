use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

const HEADER_UNALIGNED_SIZE: usize = 49;

/// # Description
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Record<K>
where
    K: AsRef<[u8]>,
{
    header: Header,
    key: K,
    data: Vec<u8>,
}

impl<K> Record<K>
where
    K: Default + AsRef<[u8]>,
{
    /// # Description
    /// Creates new `Record`, temporarily as `Default`
    /// # Examples
    /// ```no-run
    /// use pearl::Record;
    /// let rec = Record::new();
    /// ```
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
/// # Description
    pub fn header_key_len(&self) -> u64 {
        self.header.key_len
    }

    /// # Description
    pub fn header_data_len(&self) -> u64 {
        self.header.size - HEADER_UNALIGNED_SIZE as u64 - self.header.key_len
    }

    /// # Description
    /// Get number of bytes, struct `Record` uses on disk
    pub fn full_size(&self) -> usize {
        // @TODO implement
        self.header.size as usize + Self::header_size()
    }

    /// # Description
    /// Returns data size from header
    pub fn data_len(&self) -> usize {
        self.header.size as usize
    }

    /// # Description
    /// Returns key len from header
    pub fn key_len(&self) -> usize {
        self.header.key_len as usize
    }

    /// # Description
    /// Get  data ref
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// # Description
    /// Get record key reference
    pub fn key(&self) -> &K {
        &self.key
    }

    /// # Description
    /// Set data to Record, replacing if exists
    pub fn set_body(&mut self, key: K, data: Vec<u8>) {
        self.key = key;
        self.data = data;
        self.update_header();
    }

    /// # Description
    pub fn update_header(&mut self) {
        self.header.key_len = self.key.as_ref().len() as u64;
        self.header.size =
            (HEADER_UNALIGNED_SIZE + self.key.as_ref().len() + self.data.len()) as u64;
    }
}

impl<K> Record<K>
where
    K: AsRef<[u8]>,
{
    /// # Description
    /// Returns unaligned record Header size with provided key type
    pub fn header_size() -> usize {
        HEADER_UNALIGNED_SIZE
    }
}

impl<K> Record<K>
where
    K: Serialize + AsRef<[u8]>,
{
    /// # Description
    /// Serialize header to `Vec<u8>` bytes
    pub fn to_raw(&self) -> Vec<u8> {
        let mut buf = self.header.to_raw();
        buf.extend_from_slice(self.key.as_ref());
        buf.extend_from_slice(&self.data);
        buf
    }
}

impl<K> Record<K>
where
    K: for<'de> Deserialize<'de> + AsRef<[u8]> + Default,
{
    /// # Description
    /// Returns Record instant with initialized header from raw buffer
    pub fn with_raw_header(buf: &[u8]) -> Self {
        let header = Header::from_raw(buf);
        Self {
            header,
            key: Default::default(),
            data: Vec::new(),
        }
    }
}

/// # Description
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default)]
#[repr(C)]
struct Header {
    magic_byte: u64,
    key_len: u64,
    size: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    data_checksum: u32,
    header_checksum: u32,
}

impl Header {
    pub fn from_raw(buf: &[u8]) -> Self {
        deserialize(&buf).unwrap()
    }

    pub fn to_raw(&self) -> Vec<u8> {
        serialize(&self).unwrap()
    }
}
