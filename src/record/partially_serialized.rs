use crate::blob::File;
use crate::prelude::*;
use bytes::{Bytes, BytesMut};

pub(crate) struct PartiallySerializedWriteResult {
    blob_offset: u64,
    header_checksum: u32,
}

impl PartiallySerializedWriteResult {
    pub(crate) fn blob_offset(&self) -> u64 {
        self.blob_offset
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

    // Only needed for tests
    #[allow(dead_code)]
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
            Self::finalize_with_checksum(head_with_data, header_len, blob_offset);
        Ok((head.freeze(), data, checksum))
    }

    pub(crate) async fn write_to_file(self, file: &File) -> Result<PartiallySerializedWriteResult> {
        file.write_append_writable_data(self)
            .await
            .map_err(|e| match e.kind() {
                kind if kind == IOErrorKind::Other || kind == IOErrorKind::NotFound => {
                    Error::file_unavailable(kind).into()
                }
                _ => e.into(),
            })
    }

    fn finalize_with_checksum(
        mut buf: BytesMut,
        header_len: usize,
        blob_offset: u64,
    ) -> (BytesMut, u32) {
        use std::mem::size_of;

        let offset_pos = RecordHeader::blob_offset_offset(header_len);
        let checksum_pos = RecordHeader::checksum_offset(header_len);
        let offset_slice = &mut buf[offset_pos..(offset_pos + size_of::<u64>())];
        offset_slice.copy_from_slice(&blob_offset.to_le_bytes());

        let checksum_slice = &mut buf[checksum_pos..(checksum_pos + size_of::<u32>())];
        checksum_slice.copy_from_slice(&0u32.to_le_bytes());

        let header_slice = &buf[..header_len];
        let checksum: u32 = CRC32C.checksum(header_slice);
        let checksum_slice = &mut buf[checksum_pos..(checksum_pos + size_of::<u32>())];
        checksum_slice.copy_from_slice(&checksum.to_le_bytes());

        (buf, checksum)
    }
}

impl WritableDataCreator<PartiallySerializedWriteResult> for PartiallySerializedRecord {
    fn create(self, blob_offset: u64) -> (WritableData, PartiallySerializedWriteResult) {
        let PartiallySerializedRecord {
            head_with_data,
            header_len,
            data,
        } = self;
        let (head, header_checksum) = PartiallySerializedRecord::finalize_with_checksum(
            head_with_data,
            header_len,
            blob_offset,
        );
        let bytes_data = if let Some(data) = data {
            WritableData::Double(head.freeze(), data)
        } else {
            WritableData::Single(head.freeze())
        };
        let ret_data = PartiallySerializedWriteResult {
            blob_offset,
            header_checksum,
        };
        (bytes_data, ret_data)
    }

    fn len(&self) -> u64 {
        self.head_with_data.len() as u64 + self.data.as_ref().map_or(0, |v| v.len()) as u64
    }
}
