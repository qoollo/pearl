use bincode::{deserialize, serialize};

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromRaw(bincode::ErrorKind),
}

/// # Description
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Record {
    header: Header,
    key: Vec<u8>,
    data: Vec<u8>,
}

impl Record {
    /// # Description
    /// Creates new `Record`
    /// # Examples
    /// ```no-run
    /// use pearl::Record;
    /// let rec = Record::new();
    /// ```
    pub fn new<T, D>(key: T, data: D) -> Self
    where
        T: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        Self {
            key: key.as_ref().to_vec(),
            data: data.as_ref().to_vec(),
            header: Header::new(),
        }
    }

    /// # Description
    /// Get number of bytes, struct `Record` uses on disk
    pub fn full_len(&self) -> u64 {
        // @TODO implement
        self.header.full_len()
    }

    /// # Description
    /// Returns data size from header
    pub fn data_len(&self) -> usize {
        self.data.len()
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
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// # Description
    /// Get record header reference
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// # Description
    /// Set data to Record, replacing if exists
    pub fn set_body<K, D>(&mut self, key: K, data: D) -> &mut Self
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        self.key = key.as_ref().to_vec();
        self.header.key_len = self.key.len() as u64;
        self.data = data.as_ref().to_vec();
        self.header.data_len = self.data.len() as u64;
        self
    }

    /// # Description
    /// Sets blob offset before writing
    pub fn set_blob_offset(&mut self, blob_offset: u64) -> &mut Self {
        self.header.blob_offset = blob_offset;
        self
    }

    /// # Description
    /// Init new `Record` from raw buffer
    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        // @TODO Header validation
        let header = Header::from_raw(buf)?;
        let key_offset = header.serialized_size() as usize;
        let key_len = header.key_len as usize;
        let data_offset = key_offset + key_len;
        let key = buf[key_offset..key_offset + key_len].to_vec();
        let data = buf[data_offset..].to_vec();
        let mut rec = Record::new(key, data);
        rec.header = header;
        Ok(rec)
    }

    /// # Description
    /// Serialize header to `Vec<u8>` bytes
    pub fn to_raw(&self) -> Vec<u8> {
        let raw_header = self.header.to_raw();
        let mut buf = Vec::with_capacity(self.header.full_len() as usize);
        buf.extend(raw_header.iter());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.data);
        buf
    }
}

impl Record {
    /// # Description
    /// Returns Record instant with initialized header from raw buffer
    pub fn with_raw_header(buf: &[u8]) -> Result<Self> {
        let header = Header::from_raw(buf)?;
        Ok(Self {
            header,
            key: Default::default(),
            data: Vec::new(),
        })
    }
}

/// # Description
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Header {
    magic_byte: u64,
    key_len: u64,
    data_len: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    data_checksum: u32,
    header_checksum: u32,
}

impl Header {
    pub fn new() -> Self {
        // @TODO calculate check sums
        Self {
            magic_byte: RECORD_MAGIC_BYTE,
            key_len: 0,
            data_len: 0,
            flags: 0,
            blob_offset: 0,
            created: std::time::UNIX_EPOCH
                .elapsed()
                .map(|d| d.as_secs())
                .unwrap_or_else(|e| {
                    error!("{}", e);
                    0
                }),
            data_checksum: 0,
            header_checksum: 0,
        }
    }

    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        deserialize(&buf).map_err(|e| Error::FromRaw(*e))
    }

    pub fn to_raw(&self) -> Vec<u8> {
        serialize(&self).unwrap()
    }

    pub fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    pub fn full_len(&self) -> u64 {
        self.data_len + self.key_len + self.serialized_size()
    }

    fn serialized_size(&self) -> u64 {
        bincode::serialized_size(&self).unwrap()
    }
}
