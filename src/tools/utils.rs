use futures::Future;

use crate::blob::{index::FileIndexTrait, FileName};

use super::prelude::*;

pub(crate) fn validate_bytes(a: &[u8], checksum: u32) -> bool {
    let actual_checksum = CRC32C.checksum(a);
    actual_checksum == checksum
}

/// Read index header from file
pub(crate) fn read_index_header(path: &Path) -> Result<IndexHeader> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let header_size = IndexHeader::serialized_size_default()? as usize;
    let mut buf = vec![0; header_size];
    file.read_exact(&mut buf)?;
    let header = IndexHeader::from_raw(&buf)?;
    header.validate_without_version()?;
    Ok(header)
}

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
        skip_wrong_record,
    )
}

/// Migrate blob version
pub fn migrate_blob(
    input: &Path,
    output: &Path,
    validate_every: usize,
    target_version: u32,
) -> AnyResult<()> {
    process_blob_with(
        &input,
        &output,
        validate_every,
        |record, version| record.migrate(version, target_version),
        false,
    )?;
    Ok(())
}

pub(crate) fn process_blob_with<P, Q, F>(
    input: &P,
    output: &Q,
    validate_every: usize,
    preprocess_record: F,
    skip_wrong_record: bool,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
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
    // Create writer after read blob header to prevent empty blob creation
    let mut writer = BlobWriter::from_path(&output, validate_written_records)?;
    info!("Blob writer created");
    writer.write_header(&header)?;
    info!("Input blob header version: {}", header.version);
    let mut count = 0;
    while !reader.is_eof() {
        match reader
            .read_record(skip_wrong_record)
            .and_then(|record| preprocess_record(record, header.version))
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
