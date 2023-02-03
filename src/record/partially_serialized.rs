use crate::blob::File;
use crate::prelude::*;
use bytes::{Bytes, BytesMut};

const MAX_SINGLE_PASS_DATA_SIZE: usize = 4 * 1024;

enum PartiallySerializedData {
    SinglePass(Bytes),
    DoublePass(Bytes, Bytes),
}

impl PartiallySerializedData {
    pub(crate) async fn write_to_file(self, file: &File) -> Result<u64> {
        match self {
            PartiallySerializedData::SinglePass(b) => Self::write_single_pass(b, file).await,
            PartiallySerializedData::DoublePass(h, d) => Self::write_double_pass(h, d, file).await,
        }
    }

    async fn write_single_pass(buf: Bytes, file: &File) -> Result<u64> {
        let len = buf.len() as u64;
        Self::process_file_result(file.write_append_all(buf).await).map(|_| len)
    }

    async fn write_double_pass(head: Bytes, data: Bytes, file: &File) -> Result<u64> {
        let len = (head.len() + data.len()) as u64;
        Self::process_file_result(file.write_append_all_buffers(head, data).await).map(|_| len)
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

pub(crate) struct PartiallySerializedHeader {
    buf: BytesMut,
    header_len: usize,
    offset_pos: usize,
    checksum_pos: usize,
}

impl PartiallySerializedHeader {
    pub(crate) fn new(
        buf: BytesMut,
        header_len: usize,
        offset_pos: usize,
        checksum_pos: usize,
    ) -> Self {
        Self {
            buf,
            header_len,
            offset_pos,
            checksum_pos,
        }
    }

    pub(crate) fn finalize_with_checksum(self, bytes_offset: u64) -> Result<(BytesMut, u32)> {
        let Self {
            mut buf,
            header_len,
            offset_pos,
            checksum_pos,
        } = self;
        let offset_slice = &mut buf[offset_pos..];
        bincode::serialize_into(offset_slice, &bytes_offset)?;
        let header_slice = &buf[..header_len];
        let checksum: u32 = CRC32C.checksum(header_slice);
        let checksum_slice = &mut buf[checksum_pos..];
        bincode::serialize_into(checksum_slice, &checksum)?;
        Ok((buf, checksum))
    }
}

pub(crate) struct PartiallySerializedRecord {
    head: PartiallySerializedHeader,
    data: Bytes,
}

impl PartiallySerializedRecord {
    pub(crate) fn new(head: PartiallySerializedHeader, data: Bytes) -> Self {
        Self { head, data }
    }

    fn into_serialized_with_header_checksum(
        self,
        blob_offset: u64,
    ) -> Result<(PartiallySerializedData, u32)> {
        let Self { head, data } = self;
        let (mut head, checksum) = head.finalize_with_checksum(blob_offset)?;
        let data = if head.len() + data.len() > MAX_SINGLE_PASS_DATA_SIZE {
            PartiallySerializedData::DoublePass(head.freeze(), data)
        } else {
            head.extend_from_slice(&data);
            PartiallySerializedData::SinglePass(head.freeze())
        };
        Ok((data, checksum))
    }

    pub async fn write_to_file(self, file: &File, offset: u64) -> Result<(u64, u32)> {
        let (data, checksum) = self.into_serialized_with_header_checksum(offset)?;
        data.write_to_file(file).await.map(|l| (l, checksum))
    }
}
