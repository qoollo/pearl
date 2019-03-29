use bincode::{deserialize, serialize};

const HEADER_UNALIGNED_SIZE: usize = 49;

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
        self.header.size as usize + Record::header_size()
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
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// # Description
    /// Get record header reference
    pub fn header(&self) -> &Header {
        &self.header
    }

     /// # Description
    /// Get record header mutable reference
    pub fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    /// # Description
    /// Set data to Record, replacing if exists
    pub fn set_body<K, D>(&mut self, key: K, data: D)
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]> ,
    {
        self.key = key.as_ref().to_vec();
        self.data = data.as_ref().to_vec();
        self.update_header();
    }

    /// # Description
    /// Init new `Record` from raw buffer, returns `None` if buf len is less than header
    pub fn from_raw(buf: &[u8]) -> Option<Self> {
        // @TODO Header validation
        let mut record = Self::new();
        if buf.len() < HEADER_UNALIGNED_SIZE {
            None
        } else {
            record.header = Header::from_raw(buf);
            record.key = buf
                [HEADER_UNALIGNED_SIZE..HEADER_UNALIGNED_SIZE + record.header.key_len as usize]
                .to_vec();
            record.data = buf[HEADER_UNALIGNED_SIZE + record.header.key_len as usize..].to_vec();
            Some(record)
        }
    }

    /// # Description
    pub fn update_header(&mut self) {
        self.header.key_len = self.key.len() as u64;
        self.header.size = (HEADER_UNALIGNED_SIZE + self.key.len() + self.data.len()) as u64;
    }

    /// # Description
    /// Returns unaligned record Header size with provided key type
    pub fn header_size() -> usize {
        HEADER_UNALIGNED_SIZE
    }
}

impl Record {
    /// # Description
    /// Serialize header to `Vec<u8>` bytes
    pub fn to_raw(&self) -> Vec<u8> {
        let mut buf = self.header.to_raw();
        buf.extend_from_slice(self.key.as_ref());
        buf.extend_from_slice(&self.data);
        buf
    }
}

impl Record {
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
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Header {
    magic_byte: u64,
    key_len: u64,
    pub size: u64,
    flags: u8,
    pub blob_offset: u64,
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
