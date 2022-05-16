use super::prelude::*;
pub(crate) use crate::blob::header::Header as BlobHeader;
use crate::blob::index::FileIndexTrait;
pub(crate) use crate::blob::index::{header::IndexHeader, BPTreeFileIndex};
use crate::blob::FileName;
pub(crate) use crate::record::{Header, Record};
pub use crate::BloomConfig;
use futures::Future;

fn block_on<T, F: Future<Output = T>>(f: F) -> Result<T> {
    Ok(tokio::runtime::Runtime::new()?.block_on(f))
}

/// Validate index file
pub fn validate_index<K>(path: &Path) -> AnyResult<()>
where
    for<'a> K: Key<'a> + 'static,
{
    static_assertions::const_assert_eq!(blob::index::HEADER_VERSION, 5);
    let header = read_index_header(path)?;
    let blob_size = path.metadata()?.len();
    let headers = match header.version() {
        2..=4 => return Err(Error::index_header_validation_error("too low header version").into()),
        5 => block_on(async {
            let index = BPTreeFileIndex::<K>::from_file(FileName::from_path(path)?, None).await?;
            let res = index.get_records_headers(blob_size).await?;
            AnyResult::<_>::Ok(res.0)
        })?,
        _ => return Err(Error::index_header_validation_error("unknown header version").into()),
    }?;
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
