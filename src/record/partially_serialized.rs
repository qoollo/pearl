use crate::blob::File;
use crate::prelude::*;
use bytes::{Bytes, BytesMut};

const MAX_SINGLE_PASS_DATA_SIZE: usize = 4 * 1024;

enum PartiallySerializedData {
    SinglePass(BytesMut),
    DoublePass(BytesMut, Bytes),
}

impl PartiallySerializedData {
    pub(crate) async fn write_to_file(self, file: &File) -> Result<u64> {
        match self {
            PartiallySerializedData::SinglePass(b) => {
                Self::write_single_pass(b.freeze(), file).await
            }
            PartiallySerializedData::DoublePass(h, d) => {
                Self::write_double_pass(h.freeze(), d, file).await
            }
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

pub(crate) struct PartiallySerializedRecord {
    data: PartiallySerializedData,
    offset_pos: usize,
    header_checksum_pos: usize,
    header_start: usize,
    header_len: usize,
}

impl PartiallySerializedRecord {
    pub(crate) fn new(
        mut head: BytesMut,
        data: Bytes,
        offset_pos: usize,
        header_checksum_pos: usize,
        header_start: usize,
        header_len: usize,
    ) -> Self {
        let data = if head.len() + data.len() > MAX_SINGLE_PASS_DATA_SIZE {
            PartiallySerializedData::DoublePass(head, data)
        } else {
            head.extend_from_slice(&data);
            PartiallySerializedData::SinglePass(head)
        };
        Self {
            data,
            offset_pos,
            header_checksum_pos,
            header_start,
            header_len,
        }
    }

    fn into_serialized_with_header_checksum(
        self,
        blob_offset: u64,
    ) -> Result<(PartiallySerializedData, u32)> {
        let Self {
            mut data,
            offset_pos,
            header_checksum_pos,
            header_start,
            header_len,
        } = self;
        let checksum: u32;
        match data {
            PartiallySerializedData::SinglePass(ref mut buf) => {
                let offset_slice = &mut buf[offset_pos..];
                bincode::serialize_into(offset_slice, &blob_offset)?;
                let header_slice = &buf[header_start..(header_start + header_len)];
                checksum = CRC32C.checksum(header_slice);
                let checksum_slice = &mut buf[header_checksum_pos..];
                bincode::serialize_into(checksum_slice, &checksum)?;
            }
            PartiallySerializedData::DoublePass(ref mut buf, _) => {
                let offset_slice = &mut buf[offset_pos..];
                bincode::serialize_into(offset_slice, &blob_offset)?;
                let header_slice = &buf[header_start..(header_start + header_len)];
                checksum = CRC32C.checksum(header_slice);
                let checksum_slice = &mut buf[header_checksum_pos..];
                bincode::serialize_into(checksum_slice, &checksum)?;
            }
        }
        Ok((data, checksum))
    }

    pub async fn write_to_file(self, file: &File, offset: u64) -> Result<(u64, u32)> {
        let (data, checksum) = self.into_serialized_with_header_checksum(offset)?;
        data.write_to_file(file).await.map(|l| (l, checksum))
    }
}
