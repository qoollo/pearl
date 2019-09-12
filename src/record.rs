use crate::prelude::*;

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;

/// [`Record`] consists of header and data.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct Record {
    header: Header,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Header {
    magic_byte: u64,
    key: Vec<u8>,
    pub(crate) data_len: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    data_checksum: u32,
    header_checksum: u32,
    meta: Vec<Meta>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
struct Meta {
    name: String,
    value: Vec<u8>,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub(crate) struct Error {
    repr: Repr,
}

#[derive(Debug)]
enum Repr {
    Inner(ErrorKind),
    Other(Box<dyn error::Error + 'static + Send + Sync>),
}

impl Meta {}

impl Error {
    pub(crate) fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Self {
            repr: Repr::Other(error.into()),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.repr {
            Repr::Inner(_) => None,
            Repr::Other(src) => Some(src.as_ref()),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.repr.fmt(f)
    }
}

impl Display for Repr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Repr::Inner(kind) => write!(f, "{:?}", kind),
            Repr::Other(e) => e.fmt(f),
        }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Validation(String),
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Inner(kind),
        }
    }
}

impl Record {
    /// Creates new `Record` with provided data and key.
    pub fn new(key: impl Key, data: Vec<u8>) -> Self {
        let header = Header::new(key.as_ref().to_vec(), data.len() as u64, crc32(&data));
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
        let record = Self { header, data };
        record.validate()
    }

    /// # Description
    /// Serialize record to bytes
    pub fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        let raw_header = self.header.to_raw()?;
        let mut buf = Vec::with_capacity(self.header.full_len()? as usize);
        buf.extend(raw_header.iter());
        buf.extend_from_slice(&self.data);
        Ok(buf)
    }

    pub(crate) fn set_offset(&mut self, offset: u64) -> bincode::Result<()> {
        self.header.blob_offset = offset;
        self.header.update_checksum()
    }

    pub(crate) fn get_data(self) -> Vec<u8> {
        self.data
    }

    fn validate(self) -> Result<Self> {
        self.check_magic_byte()?;
        self.check_data_checksum()?;
        self.check_header_checksum()?;
        Ok(self)
    }

    fn check_magic_byte(&self) -> Result<()> {
        if self.header().magic_byte == RECORD_MAGIC_BYTE {
            Ok(())
        } else {
            Err(ErrorKind::Validation("wrong magic byte".to_owned()).into())
        }
    }

    fn check_data_checksum(&self) -> Result<()> {
        let calc_crc = crc32(&self.data);
        if calc_crc == self.header.data_checksum {
            Ok(())
        } else {
            let e = ErrorKind::Validation(format!(
                "wrong data checksum {} vs {}",
                calc_crc, self.header.data_checksum
            ));
            error!("{:?}", e);
            Err(e.into())
        }
    }

    fn check_header_checksum(&self) -> Result<()> {
        let mut header = self.header.clone();
        header.header_checksum = 0;
        let calc_crc = header.crc32().map_err(Error::new)?;
        if calc_crc == self.header.header_checksum {
            Ok(())
        } else {
            let e = ErrorKind::Validation(format!(
                "wrong header checksum {} vs {}",
                calc_crc, self.header.header_checksum
            ));
            error!("{:?}", e);
            Err(e.into())
        }
    }
}

impl Header {
    pub fn new(key: Vec<u8>, data_len: u64, data_checksum: u32) -> Self {
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
            data_checksum,
            header_checksum: 0,
            meta: Vec::new(),
        }
    }

    pub(crate) fn from_raw(buf: &[u8]) -> Result<Self> {
        deserialize(&buf).map_err(Error::new)
    }

    pub(crate) fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        serialize(&self)
    }

    pub fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    pub(crate) fn full_len(&self) -> bincode::Result<u64> {
        self.serialized_size().map(|size| self.data_len + size)
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    pub(crate) fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    fn update_checksum(&mut self) -> bincode::Result<()> {
        self.header_checksum = 0;
        self.header_checksum = self.crc32()?;
        Ok(())
    }

    fn crc32(&self) -> bincode::Result<u32> {
        self.to_raw().map(|raw| crc32(&raw))
    }
}
