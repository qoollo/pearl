use crate::prelude::*;

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;

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
    kind: ErrorKind,
}

impl Meta {
    /// Create new empty `Meta`.
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts new pair of name-value into meta.
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
        let res = deserialize(buf);
        trace!("meta deserialized: {:?}", res);
        res.map_err(Into::into)
    }

    #[inline]
    fn serialized_size(&self) -> bincode::Result<u64> {
        serialized_size(&self)
    }

    #[inline]
    pub(crate) fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        serialize(&self)
    }

    pub(crate) async fn load(file: &File, location: Location) -> AnyResult<Self> {
        trace!("meta load");
        let mut buf = vec![0; location.size()];
        trace!(
            "file read at: {} bytes, offset: {}",
            location.size(),
            location.offset()
        );
        let n = file
            .read_at(&mut buf, location.offset())
            .await
            .context("failed to load record from file")?;
        trace!("read {} bytes", n);
        Ok(Self::from_raw(&buf)?)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self, f)
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
        Self { kind }
    }
}

impl Record {
    /// Creates new `Record` with provided data, key and meta.
    pub fn create(key: impl Key, data: Vec<u8>, meta: Meta) -> bincode::Result<Self> {
        let key = key.as_ref().to_vec();
        let meta_size = meta.serialized_size()?;
        let header = Header::new(key, meta_size, data.len() as u64, crc32(&data));
        Ok(Self { header, meta, data })
    }

    /// Get immutable reference to header.
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// # Description
    /// Init new `Record` from raw buffer
    pub fn from_raw(buf: &[u8]) -> AnyResult<Self> {
        debug!("record from raw");
        // @TODO Header validation
        let header = Header::from_raw(buf)?;
        debug!("record from raw header from raw created");
        let meta_offset = header.serialized_size().try_into()?;
        let meta_size: usize = header.meta_size.try_into()?;
        trace!("meta offset {} len {}", meta_offset, meta_size);
        let meta = Meta::from_raw(&buf[meta_offset..])?;
        trace!("meta from raw created: {:?}", meta);
        let data_offset = meta_offset + meta_size;
        trace!("data offset {}", data_offset);
        let data = buf[data_offset..].to_vec();
        let record = Self { header, meta, data };
        record
            .validate()
            .with_context(|| "record validation failed")
    }

    /// # Description
    /// Serialize record to bytes
    pub fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        let mut buf = self.header.to_raw()?;
        trace!("raw header: len: {}", buf.len());
        let raw_meta = self.meta.to_raw()?;
        buf.extend(&raw_meta);
        buf.extend(&self.data);
        Ok(buf)
    }

    pub(crate) fn set_offset(&mut self, offset: u64) -> bincode::Result<()> {
        self.header.blob_offset = offset;
        self.header.update_checksum()
    }

    pub(crate) fn into_data(self) -> Vec<u8> {
        self.data
    }

    fn validate(self) -> AnyResult<Self> {
        self.check_magic_byte()?;
        self.check_data_checksum()?;
        self.check_header_checksum()
            .with_context(|| "check header checksum failed")?;
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
            let msg = format!(
                "wrong data checksum {} vs {}",
                calc_crc, self.header.data_checksum
            );
            let e = ErrorKind::Validation(msg);
            error!("{:?}", e);
            Err(e.into())
        }
    }

    fn check_header_checksum(&self) -> AnyResult<()> {
        let mut header = self.header.clone();
        header.header_checksum = 0;
        let calc_crc = header
            .crc32()
            .with_context(|| "header checksum calculation failed")?;
        if calc_crc == self.header.header_checksum {
            Ok(())
        } else {
            let e = ErrorKind::Validation(format!(
                "wrong header checksum {} vs {}",
                calc_crc, self.header.header_checksum
            ));
            error!("{:?}", e);
            Err(Error::from(e).into())
        }
    }
}

impl Header {
    pub fn new(key: Vec<u8>, meta_size: u64, data_size: u64, data_checksum: u32) -> Self {
        let created = std::time::UNIX_EPOCH.elapsed().map_or_else(
            |e| {
                error!("{}", e);
                0
            },
            |d| d.as_secs(),
        );
        Self {
            magic_byte: RECORD_MAGIC_BYTE,
            key,
            meta_size,
            data_size,
            flags: 0,
            blob_offset: 0,
            created,
            data_checksum,
            header_checksum: 0,
        }
    }

    #[inline]
    pub(crate) fn meta_location(&self) -> Location {
        let offset = self.blob_offset + self.serialized_size();
        let size = self.meta_size.try_into().expect("convert to usize");
        Location::new(offset, size)
    }

    #[inline]
    pub(crate) const fn data_size(&self) -> u64 {
        self.data_size
    }

    #[inline]
    pub(crate) const fn meta_size(&self) -> u64 {
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
    pub(crate) fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        deserialize(buf)
    }

    #[inline]
    pub(crate) fn to_raw(&self) -> bincode::Result<Vec<u8>> {
        serialize(&self)
    }

    #[inline]
    pub const fn blob_offset(&self) -> u64 {
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
