use anyhow::{Context, Result};
use bincode::{deserialize, serialized_size};
use rio::Rio;

use crate::{blob::File, Error};

use super::FileName;

crate const BLOB_VERSION: u32 = 1;
crate const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;

#[derive(Debug, Clone, Serialize, Deserialize)]
crate struct Header {
    magic_byte: u64,
    version: u32,
    flags: u64,
}

impl Header {
    crate const fn new() -> Self {
        Self {
            magic_byte: BLOB_MAGIC_BYTE,
            version: BLOB_VERSION,
            flags: 0,
        }
    }

    crate async fn from_file(name: &FileName, ioring: Option<Rio>) -> Result<Self> {
        let file = File::open(name.to_path(), ioring)
            .await
            .with_context(|| format!("failed to open blob file: {}", name))?;
        let size = serialized_size(&Header::new()).expect("failed to serialize default header");
        let mut buf = vec![0; size as usize];
        file.read_at(&mut buf, 0)
            .await
            .with_context(|| format!("failed to read from file: {}", name))?;
        let header: Self = deserialize(&buf)
            .with_context(|| format!("failed to deserialize header from file: {}", name))?;
        header.validate().context("header validation failed")?;
        Ok(header)
    }

    fn validate(&self) -> Result<()> {
        if self.version != BLOB_VERSION {
            let cause = format!(
                "old blob version: {}, expected: {}",
                self.version, BLOB_VERSION
            );
            return Err(Error::blob_validation(cause).into());
        }
        if self.magic_byte != BLOB_MAGIC_BYTE {
            return Err(Error::blob_validation("blob header magic byte is wrong").into());
        }

        Ok(())
    }
}
