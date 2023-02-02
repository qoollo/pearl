use bytes::{BufMut, Bytes, BytesMut};

use crate::{blob::File, error::ValidationErrorKind, prelude::*};

pub(crate) const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;
const DELETE_FLAG: u8 = 0x01;
const MAX_SINGLE_PASS_DATA_SIZE: usize = 4 * 1024;

#[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct Record {
    pub header: Header,
    pub meta: Meta,
    #[serde(
        serialize_with = "serialize_data",
        deserialize_with = "deserialize_data"
    )]
    pub data: Bytes,
}

fn serialize_data<S>(data: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_bytes(data.as_ref())
}

fn deserialize_data<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct DataVisitor();
    impl<'de> serde::de::Visitor<'de> for DataVisitor {
        type Value = Bytes;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bytes")
        }

        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.into())
        }
    }

    deserializer.deserialize_byte_buf(DataVisitor())
}

impl Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "Record(header={:?}, meta={:?}, data_size={})",
            self.header,
            self.meta.0,
            self.data.len()
        )
    }
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
    fn serialized_size(&self) -> u64 {
        serialized_size(&self).expect("serialized_size")
    }

    #[inline]
    pub(crate) fn to_raw_into(&self, writer: impl std::io::Write) -> bincode::Result<()> {
        serialize_into(writer, &self)
    }

    /// Get attribute.
    pub fn get(&self, k: impl AsRef<str>) -> Option<&Vec<u8>> {
        self.0.get(k.as_ref())
    }
}

pub(crate) struct PartiallySerializedRecord {
    buf: Vec<u8>,
    offset_pos: usize,
    header_checksum_pos: usize,
    header_start: usize,
    header_len: usize,
}

impl PartiallySerializedRecord {
    fn new(
        buf: Vec<u8>,
        offset_pos: usize,
        header_checksum_pos: usize,
        header_start: usize,
        header_len: usize,
    ) -> Self {
        Self {
            buf,
            offset_pos,
            header_checksum_pos,
            header_start,
            header_len,
        }
    }

    pub(crate) fn into_serialized_with_header_checksum(
        self,
        blob_offset: u64,
    ) -> Result<(Vec<u8>, u32)> {
        let Self {
            mut buf,
            offset_pos,
            header_checksum_pos,
            header_start,
            header_len,
        } = self;
        let offset_slice = &mut buf[offset_pos..];
        bincode::serialize_into(offset_slice, &blob_offset)?;
        let header_slice = &buf[header_start..(header_start + header_len)];
        let checksum = CRC32C.checksum(header_slice);
        let checksum_slice = &mut buf[header_checksum_pos..];
        bincode::serialize_into(checksum_slice, &checksum)?;
        Ok((buf, checksum))
    }

    pub(crate) async fn write_to_file(&self, file: &File) -> Result<u64> {
        if self.data.len() <= MAX_SINGLE_PASS_DATA_SIZE {
            self.write_single_pass(file).await
        } else {
            self.write_double_pass(file).await
        }
    }

    fn create_header_meta_buffer(&self, data_size: Option<u64>) -> Result<BytesMut> {
        let size = self.header.serialized_size()
            + self.meta.serialized_size()
            + data_size.unwrap_or_default();
        let mut result = BytesMut::with_capacity(size as usize);
        self.header.to_raw_into((&mut result).writer())?;
        self.meta.to_raw_into((&mut result).writer())?;
        Ok(result)
    }

    async fn write_single_pass(&self, file: &File) -> Result<u64> {
        let mut buf = self.create_header_meta_buffer(Some(self.data.len() as u64))?;
        buf.extend(&self.data);
        let len = buf.len() as u64;
        Self::process_file_result(file.write_append_all(buf.freeze()).await).map(|_| len)
    }

    async fn write_double_pass(&self, file: &File) -> Result<u64> {
        let buf = self.create_header_meta_buffer(None)?;
        let data = self.data.clone();
        let len = (buf.len() + data.len()) as u64;
        Self::process_file_result(file.write_append_all_buffers(buf.freeze(), data).await)
            .map(|_| len)
    }

    fn process_file_result(result: std::result::Result<(), std::io::Error>) -> Result<()> {
        result.map_err(|e| -> anyhow::Error {
            match e.kind() {
                kind if kind == IOErrorKind::Other || kind == IOErrorKind::NotFound => {
                    Error::file_unavailable(kind).into()
                }
                _ => e.into(),
            }
        })
    }
}

impl Record {
    pub(crate) fn new(header: Header, meta: Meta, data: Bytes) -> Self {
        Self { header, meta, data }
    }

    pub fn into_data(self) -> Bytes {
        self.data
    }

    pub fn into_header(self) -> Header {
        self.header
    }

    /// Creates new `Record` with provided data, key and meta.
    pub fn create<K>(key: &K, data: Bytes, meta: Meta) -> bincode::Result<Self>
    where
        for<'a> K: Key<'a>,
    {
        let key = key.as_ref().to_vec();
        let meta_size = meta.serialized_size();
        let data_checksum = CRC32C.checksum(&data);
        let header = Header::new(key, meta_size, data.len() as u64, data_checksum);
        Ok(Self { header, meta, data })
    }

    /// Get immutable reference to header.
    pub const fn header(&self) -> &Header {
        &self.header
    }

    pub(crate) fn to_partially_serialized(&self) -> Result<PartiallySerializedRecord> {
        let mut buf = self.header.to_raw()?;
        let header_len = buf.len();
        let raw_meta = self.meta.to_raw()?;
        buf.extend(&raw_meta);
        buf.extend(&self.data);
        Ok(PartiallySerializedRecord::new(
            buf,
            header_len - 24,
            header_len - 4,
            0,
            header_len,
        ))
    }

    pub(crate) fn deleted<K>(key: &K) -> bincode::Result<Self>
    where
        for<'a> K: Key<'a> + 'static,
    {
        let mut record = Record::create(key, Bytes::new(), Meta::default())?;
        record.header.mark_as_deleted()?;
        Ok(record)
    }

    pub(crate) fn validate(self) -> Result<Self> {
        self.header().validate()?;
        self.check_data_checksum()?;
        Ok(self)
    }

    fn check_data_checksum(&self) -> Result<()> {
        let calc_crc = CRC32C.checksum(&self.data);
        if calc_crc == self.header.data_checksum {
            Ok(())
        } else {
            let cause = format!(
                "wrong data checksum {} vs {}",
                calc_crc, self.header.data_checksum
            );
            let param = ValidationErrorKind::RecordDataChecksum;
            let e = Error::validation(param, cause);
            error!("{:#?}", e);
            Err(e.into())
        }
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    pub fn into_header(self) -> Header {
        self.header
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
    pub(crate) const fn data_size(&self) -> u64 {
        self.data_size
    }

    #[inline]
    pub(crate) const fn meta_size(&self) -> u64 {
        self.meta_size
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
    pub(crate) fn to_raw_into(&self, writer: impl std::io::Write) -> bincode::Result<()> {
        serialize_into(writer, &self)
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

    pub(crate) fn set_offset_checksum(&mut self, blob_offset: u64, header_checksum: u32) {
        self.blob_offset = blob_offset;
        self.header_checksum = header_checksum;
    }

    fn update_checksum(&mut self) -> bincode::Result<()> {
        self.header_checksum = 0;
        self.header_checksum = self.crc32()?;
        Ok(())
    }

    fn crc32(&self) -> bincode::Result<u32> {
        self.to_raw().map(|raw| CRC32C.checksum(&raw))
    }

    /// Used for migration
    pub(crate) fn with_reversed_key_bytes(mut self) -> bincode::Result<Self> {
        self.key.reverse();
        self.update_checksum()?;
        Ok(self)
    }

    fn check_magic_byte(&self) -> Result<()> {
        if self.magic_byte == RECORD_MAGIC_BYTE {
            Ok(())
        } else {
            let param = ValidationErrorKind::RecordMagicByte;
            Err(Error::validation(param, "wrong magic byte").into())
        }
    }

    fn check_header_checksum(&self) -> Result<()> {
        let mut header = self.clone();
        header.header_checksum = 0;
        let calc_crc = header
            .crc32()
            .with_context(|| "header checksum calculation failed")?;
        if calc_crc == self.header_checksum {
            Ok(())
        } else {
            let cause = format!(
                "wrong header checksum {} vs {}",
                calc_crc, self.header_checksum
            );
            let param = ValidationErrorKind::RecordHeaderChecksum;
            let e = Error::validation(param, cause);
            error!("{:#?}", e);
            Err(Error::from(e).into())
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.check_magic_byte()
            .with_context(|| "check header magic byte failed")?;
        self.check_header_checksum()
            .with_context(|| "check header checksum failed")?;
        Ok(())
    }

    pub(crate) fn mark_as_deleted(&mut self) -> bincode::Result<()> {
        self.flags |= DELETE_FLAG;
        self.update_checksum()
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.flags & DELETE_FLAG == DELETE_FLAG
    }

    pub(crate) fn created(&self) -> u64 {
        self.created
    }
}
