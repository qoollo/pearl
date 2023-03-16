use super::prelude::*;
use crate::blob::index::FileIndexTrait;
use crate::blob::index::HEADER_VERSION;
use crate::blob::FileName;
use crate::ArrayKey;
use futures::Future;

/// Read index header from file
pub(crate) fn read_index_header(path: &Path) -> Result<IndexHeader> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let header_size = IndexHeader::serialized_size_default() as usize;
    let mut buf = vec![0; header_size];
    file.read_exact(&mut buf)?;
    let header = IndexHeader::from_raw(&buf)?;
    header.validate_without_version()?;
    Ok(header)
}

/// Recover blob in-place, corrupted blob will be moved to `backup_path`
pub fn move_and_recover_blob<P, Q>(
    invalid_path: P,
    backup_path: Q,
    validate_every: usize,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    std::fs::rename(&invalid_path, &backup_path).with_context(|| "move invalid blob")?;

    info!("backup saved to {:?}", backup_path.as_ref());
    recovery_blob(&backup_path, &invalid_path, validate_every, true)?;
    Ok(())
}

/// Recover blob
pub fn recovery_blob<P, Q>(
    input: &P,
    output: &Q,
    validate_every: usize,
    skip_wrong_record: bool,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    process_blob_with(
        input,
        output,
        validate_every,
        |record, _| Ok(record),
        |header, _| Ok(header),
        skip_wrong_record,
    )
}

pub(crate) fn process_blob_with<P, Q, F, H>(
    input: &P,
    output: &Q,
    validate_every: usize,
    preprocess_record: F,
    preprocess_header: H,
    skip_wrong_record: bool,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
    H: Fn(BlobHeader, u32) -> AnyResult<BlobHeader>,
    F: Fn(Record, u32) -> AnyResult<Record>,
{
    if input.as_ref() == output.as_ref() {
        return Err(anyhow::anyhow!(
            "Recovering into same file is not supported"
        ));
    }
    let validate_written_records = validate_every != 0;
    let mut reader = BlobReader::from_path(&input)?;
    info!("Blob reader created");
    let header = reader.read_header()?;
    let source_version = header.version;
    let header = preprocess_header(header, source_version)?;
    // Create writer after read blob header to prevent empty blob creation
    let mut writer = BlobWriter::from_path(&output, validate_written_records)?;
    info!("Blob writer created");
    writer.write_header(&header)?;
    info!("Input blob header version: {}", source_version);
    let mut count = 0;
    while !reader.is_eof() {
        match reader
            .read_record(skip_wrong_record)
            .and_then(|record| preprocess_record(record, source_version))
        {
            Ok(record) => {
                writer.write_record(record)?;
                count += 1;
            }
            Err(error) => {
                error!("Record read error from {:?}: {}", input.as_ref(), error);
                break;
            }
        }
        if validate_written_records && count % validate_every == 0 {
            writer.validate_written_records()?;
            writer.clear_cache();
        }
    }
    if validate_written_records {
        writer.validate_written_records()?;
        writer.clear_cache();
    }
    info!(
        "Blob from '{:?}' recovered to '{:?}'",
        input.as_ref(),
        output.as_ref()
    );
    info!(
        "{} records written, totally {} bytes",
        count,
        writer.written()
    );
    Ok(())
}

/// block on function
pub(super) fn block_on<T, F: Future<Output = T>>(f: F) -> Result<T> {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => Ok(runtime.block_on(f)),
        Err(_) => Ok(tokio::runtime::Runtime::new()?.block_on(f)),
    }
}

async fn index_from_file<K>(
    header: &IndexHeader,
    path: &Path,
) -> AnyResult<BTreeMap<Vec<u8>, Vec<record::Header>>>
where
    for<'a> K: Key<'a> + 'static,
{
    let headers = match header.version() {
        HEADER_VERSION => {
            let index =
                BPTreeFileIndex::<K>::from_file(FileName::from_path(path)?, IoDriver::default())
                    .await?;
            let res = index.get_records_headers(index.blob_size()).await?;
            AnyResult::<_>::Ok(res.0)
        }
        _ => return Err(Error::index_header_validation_error("unsupported header version").into()),
    }?;
    let headers = headers
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value))
        .collect();
    Ok(headers)
}

/// Read index file, async
pub async fn read_index(path: &Path) -> AnyResult<BTreeMap<Vec<u8>, Vec<record::Header>>> {
    let header = read_index_header(path)?;
    let headers = match header.key_size() {
        4 => index_from_file::<ArrayKey<4>>(&header, path).await,
        8 => index_from_file::<ArrayKey<8>>(&header, path).await,
        16 => index_from_file::<ArrayKey<16>>(&header, path).await,
        32 => index_from_file::<ArrayKey<32>>(&header, path).await,
        64 => index_from_file::<ArrayKey<64>>(&header, path).await,
        128 => index_from_file::<ArrayKey<128>>(&header, path).await,
        size => return Err(Error::unsupported_key_size(size).into()),
    }?;
    for (_, headers) in headers.iter() {
        for header in headers {
            header.validate()?;
        }
    }
    Ok(headers)
}

/// Read index file, sync
pub fn read_index_sync(path: &Path) -> AnyResult<BTreeMap<Vec<u8>, Vec<record::Header>>> {
    block_on(read_index(path))?
}
