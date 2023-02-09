use crate::blob::File;
use crate::prelude::*;
use bytes::{Bytes, BytesMut};

pub(crate) struct PartiallySerializedWriteResult {
    bytes_written: u64,
    header_checksum: u32,
}

impl PartiallySerializedWriteResult {
    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub(crate) fn header_checksum(&self) -> u32 {
        self.header_checksum
    }
}

pub(crate) struct PartiallySerializedRecord {
    head_with_data: BytesMut,
    header_len: usize,
    data: Option<Bytes>,
}

impl PartiallySerializedRecord {
    pub(crate) fn new(head_with_data: BytesMut, header_len: usize, data: Option<Bytes>) -> Self {
        Self {
            head_with_data,
            header_len,
            data,
        }
    }

    pub(super) fn serialize_with_checksum(
        self,
        blob_offset: u64,
    ) -> Result<(Bytes, Option<Bytes>, u32)> {
        let Self {
            head_with_data,
            header_len,
            data,
        } = self;
        let (head, checksum) =
            Self::finalize_with_checksum(head_with_data, header_len, blob_offset)?;
        Ok((head.freeze(), data, checksum))
    }

    pub(crate) async fn write_to_file(
        self,
        file: &File,
        offset: u64,
    ) -> Result<PartiallySerializedWriteResult> {
        let (head, data, checksum) = self.serialize_with_checksum(offset)?;

        let res = if let Some(data) = data {
            Self::write_double_pass(head, data, file).await
        } else {
            Self::write_single_pass(head, file).await
        };

        res.map(|l| PartiallySerializedWriteResult {
            bytes_written: l,
            header_checksum: checksum,
        })
    }

    fn finalize_with_checksum(
        mut buf: BytesMut,
        header_len: usize,
        bytes_offset: u64,
    ) -> Result<(BytesMut, u32)> {
        use std::mem::size_of;

        let offset_pos = RecordHeader::blob_offset_offset(header_len);
        let checksum_pos = RecordHeader::checksum_offset(header_len);
        let offset_slice = &mut buf[offset_pos..];
        offset_slice[..size_of::<u64>()].copy_from_slice(&bytes_offset.to_le_bytes());

        let checksum_slice = &mut buf[checksum_pos..];
        checksum_slice[..size_of::<u32>()].copy_from_slice(&0u32.to_le_bytes());

        let header_slice = &buf[..header_len];
        let checksum: u32 = CRC32C.checksum(header_slice);
        let checksum_slice = &mut buf[checksum_pos..];
        checksum_slice[..size_of::<u32>()].copy_from_slice(&checksum.to_le_bytes());

        Ok((buf, checksum))
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
