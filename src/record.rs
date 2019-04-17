use bincode::{deserialize, serialize};

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromRaw(bincode::ErrorKind),
    WrongMagicByte(u64),
}

/// [`Record`] consists of header, key and data.
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Record {
    header: Header,
    key: Vec<u8>,
    data: Vec<u8>,
}

impl Record {
    /// Creates new `Record` with empty data and key.
    /// Use [`Self::set_body`] method to initialize it.
    ///
    /// [`Self::set_body`]: #method.set_body
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pearl::Record;
    /// let rec = Record::new();
    /// ```
    pub fn new() -> Self {
        Self {
            key: Vec::new(),
            data: Vec::new(),
            header: Header::new(),
        }
    }

    /// Returns the number of bytes, struct `Record` uses on disk.
    /// Includes len of elements: data length, key length and header
    /// serialized size.
    /// # Examples
    /// ```
    /// # use pearl::Record;
    /// let mut rec = Record::new();
    /// rec.set_body("key", "data");
    /// assert_eq!(rec.full_len(), 56);
    /// ```
    pub fn full_len(&self) -> u64 {
        // @TODO implement
        self.header.full_len()
    }

    /// Returns length of owned data buffer.
    /// # Examples
    /// ```
    /// # use pearl::Record;
    /// let data = "test";
    /// let mut record = Record::new();
    /// record.set_body("key", data);
    /// assert_eq!(record.data_len(), data.len());
    /// ```
    pub fn data_len(&self) -> usize {
        self.header.data_len as usize
    }

    /// Returns length of key.
    /// # Examples
    /// ```
    /// # use pearl::Record;
    /// let key = "test-key";
    /// let mut record = Record::new();
    /// record.set_body(key, "data");
    /// assert_eq!(record.key_len(), key.len());
    /// ```
    pub fn key_len(&self) -> usize {
        self.header.key_len as usize
    }

    /// Get immutable reference to owned data buffer.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get immutable reference to owned key buffer.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get immutable reference to header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Sets key and data to record. It clones provided buffers,
    /// converting them to `Vec<u8>`.
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
        // dbg!(&header);
        let key_offset = header.serialized_size() as usize;
        let key_len = header.key_len as usize;
        let data_offset = key_offset + key_len;
        // dbg!(data_offset);
        // dbg!(buf.len());
        let key = buf[key_offset..key_offset + key_len].to_vec();
        let data = buf[data_offset..].to_vec();
        let mut rec = Record::new();
        rec.set_body(key, data);
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
        let header: Self = deserialize(&buf).map_err(|e| Error::FromRaw(*e))?;
        if header.magic_byte != RECORD_MAGIC_BYTE {
            Err(Error::WrongMagicByte(header.magic_byte))
        } else {
            Ok(header)
        }
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
