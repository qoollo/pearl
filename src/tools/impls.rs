use super::prelude::*;
pub(crate) use crate::blob::header::{Header as BlobHeader, BLOB_MAGIC_BYTE};
pub(crate) use crate::blob::index::{header::IndexHeader, BPTreeFileIndex, SimpleFileIndex};
pub(crate) use crate::record::RECORD_MAGIC_BYTE;
pub(crate) use crate::record::{Header, Record};
pub use crate::BloomConfig;

impl IndexHeader {
    pub fn validate_without_version(&self) -> AnyResult<()> {
        if !self.is_written() {
            return Err(
                Error::index_header_validation_error("Header is corrupt".to_string()).into(),
            );
        }
        Ok(())
    }
}

impl BlobHeader {
    pub fn validate_without_version(&self) -> AnyResult<()> {
        if self.magic_byte != BLOB_MAGIC_BYTE {
            return Err(Error::blob_header_validation_error(
                "blob header magic byte is invalid".to_string(),
            )
            .into());
        }
        Ok(())
    }
}

impl Header {
    pub fn validate(&self) -> AnyResult<()> {
        if self.magic_byte != RECORD_MAGIC_BYTE {
            return Err(Error::record_header_validation_error(
                "record header magic byte is invalid".to_string(),
            )
            .into());
        }
        let mut header = self.clone();
        header.header_checksum = 0;
        let serialized = bincode::serialize(&header)?;
        if !validate_bytes(&serialized, self.header_checksum) {
            return Err(Error::index_header_validation_error("invalid header checksum").into());
        };
        Ok(())
    }
}

impl Record {
    pub(crate) fn migrate(self, source: u32, target: u32) -> AnyResult<Self> {
        match (source, target) {
            (source, target) if source >= target => Ok(self),
            (0, 1) => self.mirgate_v0_to_v1(),
            (source, target) => Err(Error::unsupported_migration(source, target).into()),
        }
    }

    pub(crate) fn mirgate_v0_to_v1(mut self) -> AnyResult<Self> {
        self.header = self.header.with_reversed_key_bytes()?;
        Ok(self)
    }
}
