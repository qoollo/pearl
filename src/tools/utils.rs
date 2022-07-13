use super::prelude::*;

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