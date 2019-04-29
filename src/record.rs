use bincode::{deserialize, serialize};

use crate::storage::{Key, Value};

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FromRaw(bincode::ErrorKind),
    WrongMagicByte(u64),
    Serialization(Box<bincode::ErrorKind>),
}

/// [`Record`] consists of header and data.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Record {
    header: Header,
    data: Vec<u8>,
}

impl Record {
    /// Creates new `Record` with provided data and key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pearl::Record;
    /// let rec = Record::new();
    /// ```
    pub fn new(key: impl Key, data: impl Value) -> Self {
        let header = Header::new(key, data.len() as u64);
        Self { header, data }
    }

    /// Get immutable reference to owned key buffer.
    pub fn key(&self) -> &[u8] {
        self.header.key()
    }

    /// Get immutable reference to header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// # Description
    /// Init new `Record` from raw buffer
    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        // @TODO Header validation
        let header = Header::from_raw(buf)?;
        let data_offset = buf.len() - header.data_len as usize;
        let data = buf[data_offset..].to_vec();
        Ok(Self { header, data })
    }

    /// # Description
    /// Serialize record to bytes
    pub fn to_raw(&self) -> Result<Vec<u8>> {
        let raw_header = self.header.to_raw()?;
        let mut buf = Vec::with_capacity(self.header.full_len()? as usize);
        buf.extend(raw_header.iter());
        buf.extend_from_slice(&self.data);
        Ok(buf)
    }

    pub(crate) fn set_offset(&mut self, offset: u64) {
        self.header.blob_offset = offset;
    }

    pub(crate) fn get_data(self) -> Vec<u8> {
        self.data
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Header {
    magic_byte: u64,
    key: Vec<u8>,
    data_len: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    data_checksum: u32,
    header_checksum: u32,
}

impl Header {
    pub fn new(key: Vec<u8>, data_len: u64) -> Self {
        // @TODO calculate check sums
        Self {
            magic_byte: RECORD_MAGIC_BYTE,
            key,
            data_len,
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

    pub fn to_raw(&self) -> Result<Vec<u8>> {
        serialize(&self).map_err(Error::Serialization)
    }

    pub fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    pub fn full_len(&self) -> Result<u64> {
        Ok(self.data_len + self.serialized_size()?)
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    fn serialized_size(&self) -> Result<u64> {
        bincode::serialized_size(&self).map_err(Error::Serialization)
    }
}
