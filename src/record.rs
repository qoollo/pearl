use crate::prelude::*;

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;

/// [`Record`] consists of header and data.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct Record {
    header: Header,
    meta: Meta,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Header {
    magic_byte: u64,
    key: Vec<u8>,
    meta_size: u64,
    data_size: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    data_checksum: u32,
    header_checksum: u32,
}

/// Struct representing additional meta information. Helps to distinguish different
/// version of the records with the same key.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Meta(HashMap<String, Vec<u8>>);

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

impl Meta {
    /// Create new Meta with name and bytes body.
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts new option into meta
    #[inline]
    pub fn insert(
        &mut self,
        name: String,
        value: impl Into<Vec<u8>>,
    ) -> Option<impl Into<Vec<u8>>> {
        self.0.insert(name, value.into())
    }

    #[inline]
    pub(crate) fn from_raw(buf: &[u8]) -> Result<Self> {
        let res = deserialize(&buf);
        trace!("meta deserialized: {:?}", res);
        res.map_err(Into::into)
    }

    #[inline]
    fn serialized_size(&self) -> Result<u64> {
        serialized_size(&self).map_err(Error::new)
    }

    #[inline]
    pub(crate) fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        let buf = serialize(&self)?;
        Ok(buf)
    }

    pub(crate) async fn load(file: &File, location: Location) -> Result<Self> {
        let buf = file.read_at(location.size(), location.offset()).await?;
        Self::from_raw(&buf)
    }
}

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
        Debug::fmt(&self.repr, f)
    }
}

impl Display for Repr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Inner(kind) => write!(f, "{:?}", kind),
            Self::Other(e) => Debug::fmt(e, f),
        }
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        ErrorKind::Bincode(e.to_string()).into()
    }
}

impl From<IOError> for Error {
    fn from(e: IOError) -> Self {
        ErrorKind::IO(e.to_string()).into()
    }
}

impl From<TryFromIntError> for Error {
    #[must_use]
    fn from(e: TryFromIntError) -> Self {
        ErrorKind::Conversion(e.to_string()).into()
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Validation(String),
    Bincode(String),
    IO(String),
    Conversion(String),
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
    pub fn create(key: impl Key, data: Vec<u8>, meta: Meta) -> Result<Self> {
        meta.serialized_size().map(|meta_size| {
            let header = Header::new(
                key.as_ref().to_vec(),
                meta_size,
                data.len() as u64,
                crc32(&data),
            );
            Self { header, meta, data }
        })
    }

    /// Get immutable reference to header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// # Description
    /// Init new `Record` from raw buffer
    pub fn from_raw(buf: &[u8]) -> Result<Self> {
        trace!("len: {}, buf: {:?}", buf.len(), buf);
        // @TODO Header validation
        let header = Header::from_raw(buf)?;
        trace!("header from raw created");
        let meta_offset = header.serialized_size().try_into()?;
        let meta_size: usize = header.meta_size.try_into()?;
        trace!("meta offset {} len {}", meta_offset, meta_size);
        let meta = Meta::from_raw(&buf[meta_offset..])?;
        trace!("meta from raw created: {:?}", meta);
        let data_offset = meta_offset + meta_size;
        trace!("data offset {}", data_offset);
        let data = buf[data_offset..].to_vec();
        let record = Self { header, meta, data };
        record.validate()
    }

    /// # Description
    /// Serialize record to bytes
    pub fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        let mut buf = self.header.to_raw()?;
        trace!("raw header: len: {}", buf.len());
        let raw_meta = self.meta.to_raw()?;
        buf.extend(&raw_meta);
        buf.extend(&self.data);
        trace!("len: {}", buf.len());
        Ok(buf)
    }

    pub(crate) fn set_offset(&mut self, offset: u64) -> bincode::Result<()> {
        self.header.blob_offset = offset;
        self.header.update_checksum()
    }

    pub(crate) fn into_data(self) -> Vec<u8> {
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
    pub fn new(key: Vec<u8>, meta_size: u64, data_size: u64, data_checksum: u32) -> Self {
        Self {
            magic_byte: RECORD_MAGIC_BYTE,
            key,
            meta_size,
            data_size,
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
        }
    }

    #[inline]
    pub(crate) fn meta_location(&self) -> Location {
        Location::new(
            self.blob_offset + self.serialized_size(),
            self.meta_size.try_into().expect("convert to usize"),
        )
    }

    #[inline]
    pub(crate) fn data_size(&self) -> u64 {
        self.data_size
    }

    #[inline]
    pub(crate) fn meta_size(&self) -> u64 {
        self.meta_size
    }

    #[inline]
    pub(crate) fn full_size(&self) -> u64 {
        self.serialized_size() + self.meta_size + self.data_size
    }

    #[inline]
    pub fn meta_offset(&self) -> u64 {
        self.blob_offset + self.serialized_size()
    }

    #[inline]
    pub fn data_offset(&self) -> u64 {
        self.meta_offset() + self.meta_size
    }

    #[inline]
    pub(crate) fn from_raw(buf: &[u8]) -> Result<Self> {
        deserialize(&buf).map_err(Error::new)
    }

    #[inline]
    pub(crate) fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        serialize(&self)
    }

    #[inline]
    pub fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    pub fn has_key(&self, key: &[u8]) -> bool {
        self.key.as_slice() == key
    }

    #[inline]
    pub(crate) fn serialized_size(&self) -> u64 {
        bincode::serialized_size(&self).expect("calc record serialized size")
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
