use crate::prelude::*;
use anyhow::{Context, Result};
use bincode::{deserialize, serialized_size};

use crate::{error::ValidationErrorKind, Error};

pub(crate) const BLOB_VERSION: u32 = 1;
pub(crate) const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Header {
    pub(crate) magic_byte: u64,
    pub(crate) version: u32,
    pub(crate) flags: u64,
}

impl Header {
    pub(crate) const fn new() -> Self {
        Self {
            magic_byte: BLOB_MAGIC_BYTE,
            version: BLOB_VERSION,
            flags: 0,
        }
    }

    pub(crate) async fn from_file(file: &File, file_name: &Path) -> Result<Self> {
        let size = serialized_size(&Header::new()).expect("failed to serialize default header");
        let buf = file
            .read_exact_at_allocate(size as usize, 0)
            .await
            .with_context(|| format!("failed to read from file: {:?}", file_name))?;
        let header: Self = deserialize(&buf)
            .with_context(|| format!("failed to deserialize header from file: {:?}", file_name))?;
        header.validate().context("header validation failed")?;
        Ok(header)
    }

    fn validate(&self) -> Result<()> {
        self.validate_without_version()?;
        if self.version != BLOB_VERSION {
            let cause = format!(
                "old blob version: {}, expected: {}",
                self.version, BLOB_VERSION
            );
            return Err(Error::validation(ValidationErrorKind::BlobVersion, cause).into());
        }

        Ok(())
    }

    pub fn validate_without_version(&self) -> Result<()> {
        if self.magic_byte != BLOB_MAGIC_BYTE {
            let param = ValidationErrorKind::BlobMagicByte;
            return Err(Error::validation(param, "blob header magic byte mismatch").into());
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn serialized_size(&self) -> u64 {
        serialized_size(&self).expect("blob header size")
    }
}
