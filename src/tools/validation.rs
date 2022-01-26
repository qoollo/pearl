use super::prelude::*;
pub(crate) use crate::blob::header::Header as BlobHeader;
use crate::blob::index::FileIndexTrait;
pub(crate) use crate::blob::index::{header::IndexHeader, BPTreeFileIndex, SimpleFileIndex};
use crate::blob::FileName;
pub(crate) use crate::record::{Header, Record};
pub use crate::BloomConfig;
use futures::Future;

fn block_on<T, F: Future<Output = T>>(f: F) -> Result<T> {
    Ok(tokio::runtime::Runtime::new()?.block_on(f))
}

/// Validate index file
pub fn validate_index<K: Key + 'static>(path: &Path) -> AnyResult<()> {
    let header = read_index_header(path)?;
    let headers = match header.version() {
        2 => block_on(async {
            let index =
                <SimpleFileIndex as FileIndexTrait<K>>::from_file(FileName::from_path(path)?, None)
                    .await?;
            let res = index.get_records_headers().await?;
            AnyResult::<_>::Ok(res.0)
        })?,
        3..=4 => block_on(async {
            let index = BPTreeFileIndex::<K>::from_file(FileName::from_path(path)?, None).await?;
            let res = index.get_records_headers().await?;
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
