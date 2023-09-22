use crate::{error::ValidationErrorKind, prelude::*};
use bytes::{BufMut, Bytes, BytesMut};

use super::partially_serialized::PartiallySerializedRecord;

pub(crate) const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;
const DELETE_FLAG: u8 = 0x01;

const MAX_SINGLE_PASS_DATA_SIZE: usize = 4 * 1024;

#[derive(Default, Clone, PartialEq)]
pub struct Record {
    pub header: Header,
    pub meta: Meta,
    pub data: Bytes,
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
    timestamp: u64, // This was previously a 'created' field. Adding new field is a breaking change. TODO: add back 'created' in the future releases
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
    pub(crate) fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        let res = deserialize(buf);
        trace!("meta deserialized: {:?}", res);
        res
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
    pub fn create<K>(key: &K, timestamp: u64, data: Bytes, meta: Option<Meta>) -> bincode::Result<Self>
    where
        for<'a> K: Key<'a>,
    {
        let key = key.as_ref().to_vec();
        let meta = meta.unwrap_or_default();
        let meta_size = meta.serialized_size();
        let data_checksum = CRC32C.checksum(&data);
        let header = Header::new(key, timestamp, meta_size, data.len() as u64, data_checksum);
        Ok(Self { header, meta, data })
    }

    /// Get immutable reference to header.
    pub const fn header(&self) -> &Header {
        &self.header
    }

    pub(crate) fn to_partially_serialized_and_header(
        self,
    ) -> Result<(PartiallySerializedRecord, Header)> {
        let Self {
            data, header, meta, ..
        } = self;
        let head_size = (header.serialized_size() + meta.serialized_size()) as usize;
        let include_data = head_size + data.len() <= MAX_SINGLE_PASS_DATA_SIZE;
        let buf_len = head_size + if include_data { data.len() } else { 0 };
        let mut buf = BytesMut::with_capacity(buf_len);
        header.to_raw_into((&mut buf).writer())?;
        let header_len = buf.len();
        meta.to_raw_into((&mut buf).writer())?;
        let data = if include_data {
            buf.extend(&data);
            None
        } else {
            Some(data)
        };

        Ok((
            PartiallySerializedRecord::new(buf, header_len, data),
            header,
        ))
    }

    pub(crate) fn deleted<K>(key: &K, timestamp: u64, meta: Option<Meta>) -> bincode::Result<Self>
    where
        for<'a> K: Key<'a> + 'static,
    {
        let mut record = Record::create(key, timestamp, Bytes::new(), meta)?;
        record.header.mark_as_deleted()?;
        Ok(record)
    }

    pub(crate) fn validate(self) -> Result<Self> {
        self.header().validate()?;
        self.check_data_checksum()?;
        Ok(self)
    }

    fn check_data_checksum(&self) -> Result<()> {
        self.header.data_checksum_audit(&self.data)
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }
}

impl Header {
    pub fn new(key: Vec<u8>, timestamp: u64, meta_size: u64, data_size: u64, data_checksum: u32) -> Self {
        Self {
            magic_byte: RECORD_MAGIC_BYTE,
            key,
            meta_size,
            data_size,
            flags: 0,
            blob_offset: 0,
            timestamp: timestamp,
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
        self.header_checksum = 0;
        debug_assert_eq!(self.crc32().expect("debug assert crc32"), header_checksum);
        self.header_checksum = header_checksum;
    }

    /// Calculates offset for the `blob_offset` field in serialized header
    /// len is length of serialized header in bytes
    pub(super) const fn blob_offset_offset(len: usize) -> usize {
        len - 24
    }

    /// Calculates offset for the `checksum` field in serialized header
    /// len is length of serialized header in bytes
    pub(super) const fn checksum_offset(len: usize) -> usize {
        len - 4
    }

    fn update_checksum(&mut self) -> bincode::Result<u32> {
        self.header_checksum = 0;
        self.header_checksum = self.crc32()?;
        Ok(self.header_checksum)
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

    pub(crate) fn data_checksum_audit(&self, data: &[u8]) -> Result<()> {
        let calc_crc = CRC32C.checksum(data);
        if calc_crc == self.data_checksum {
            Ok(())
        } else {
            let cause = format!(
                "wrong data checksum {} vs {}",
                calc_crc, self.data_checksum
            );
            let param = ValidationErrorKind::RecordDataChecksum;
            let e = Error::validation(param, cause);
            error!("{:#?}", e);
            Err(e.into())
        }
    }

    pub(crate) fn mark_as_deleted(&mut self) -> bincode::Result<()> {
        self.flags |= DELETE_FLAG;
        self.update_checksum()?;
        Ok(())
    }

    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.flags & DELETE_FLAG == DELETE_FLAG
    }

    #[inline]
    pub(crate) fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use crate::prelude::*;

    #[test]
    pub fn partial_serialization_preserves_checksum_and_offset() -> Result<()> {
        let data = Bytes::from((0..16).map(|i| i * i).collect::<Vec<u8>>());

        for i in 0..8 {
            let key = vec![0, 0, i];
            let data = data.clone();
            let checksum: u32 = CRC32C.checksum(&data);
            let meta = Meta::new();
            let header =
                RecordHeader::new(key, 101, meta.serialized_size(), data.len() as u64, checksum);
            let header_size = header.serialized_size();
            let record = Record::new(header, meta, data);
            let offset: u64 = 101 * i as u64;

            let (partially_serialized, _) = record.to_partially_serialized_and_header()?;
            let (head, _, checksum) = partially_serialized.serialize_with_checksum(offset)?;

            let from_raw = RecordHeader::from_raw(&head[..(header_size as usize)])?;

            assert_eq!(offset, from_raw.blob_offset);
            assert_eq!(checksum, from_raw.header_checksum);
        }
        Ok(())
    }

    #[test]
    pub fn partial_serialization_is_equal_to_normal_serialization() -> Result<()> {
        let data = Bytes::from((0..16).map(|i| i * i).collect::<Vec<u8>>());
        for i in 0..8 {
            let key = vec![0, 0, i];
            let data = data.clone();
            let checksum: u32 = CRC32C.checksum(&data);
            let meta = Meta::new();
            let mut header =
                RecordHeader::new(key, 101, meta.serialized_size(), data.len() as u64, checksum);
            let header_size = header.serialized_size() as usize;
            let record = Record::new(header.clone(), meta.clone(), data.clone());
            let offset: u64 = 101 * i as u64;

            let (partially_serialized, _) = record.to_partially_serialized_and_header()?;
            let (head, body, _) = partially_serialized.serialize_with_checksum(offset)?;
            let mut new_method_buf = BytesMut::with_capacity(header_size + data.len());
            new_method_buf.extend(&head);
            if let Some(body) = body {
                new_method_buf.extend(&body);
            }

            let mut old_method_buf = BytesMut::with_capacity(header_size + data.len());
            header.blob_offset = offset;
            header.update_checksum()?;
            serialize_into((&mut old_method_buf).writer(), &header)?;
            serialize_into((&mut old_method_buf).writer(), &meta)?;
            old_method_buf.extend(data);

            assert_eq!(old_method_buf, new_method_buf);
        }
        Ok(())
    }
}
