use super::prelude::*;
pub(crate) use crate::blob::header::Header as BlobHeader;
pub(crate) use crate::blob::index::{header::IndexHeader, BPTreeFileIndex};
pub(crate) use crate::record::{Header, Record};
pub use crate::BloomConfig;

/// Validate index file
pub fn validate_index(path: &Path) -> AnyResult<()> {
    read_index(path)?;
    Ok(())
}

/// Validate blob file
pub fn validate_blob(path: &Path) -> AnyResult<()> {
    let mut reader = BlobReader::from_path(&path)?;
    reader.read_header()?;
    while !reader.is_eof() {
        reader.read_record(false)?;
    }
    Ok(())
}
