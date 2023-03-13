use super::prelude::*;
use super::utils::block_on;
pub(crate) use crate::blob::header::Header as BlobHeader;
use crate::blob::index::FileIndexTrait;
pub(crate) use crate::blob::index::{header::IndexHeader, BPTreeFileIndex};
use crate::blob::FileName;
pub(crate) use crate::record::{Header, Record};
pub use crate::BloomConfig;

/// Validate index file
pub fn validate_index<K>(path: &Path) -> AnyResult<()>
where
    for<'a> K: Key<'a> + 'static,
{
    use blob::index::HEADER_VERSION;

    static_assertions::const_assert_eq!(HEADER_VERSION, 6);
    let header = read_index_header(path)?;
    let blob_path = path.with_extension("blob");
    let blob_size = if blob_path.exists() {
        blob_path.metadata()?.len()
    } else {
        if let Some(path) = path.to_str() {
            warn!("blob file doesn't exist for {}", path);
        } else {
            warn!("blob file doesn't exist for index");
        }
        header.blob_size()
    };
    let headers = if header.version() < HEADER_VERSION {
        return Err(Error::index_header_validation_error(format!(
            "Index version is outdated. Passed version: {}, latest version: {}",
            header.version(),
            HEADER_VERSION
        ))
        .into());
    } else if header.version() == HEADER_VERSION {
        block_on(async {
            let index =
                BPTreeFileIndex::<K>::from_file(FileName::from_path(path)?, IODriver::default())
                    .await?;
            let res = index.get_records_headers(blob_size).await?;
            AnyResult::<_>::Ok(res.0)
        })??
    } else {
        return Err(Error::index_header_validation_error("unknown header version").into());
    };
    for (_, headers) in headers {
        for header in headers {
            header.validate()?;
        }
    }
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
