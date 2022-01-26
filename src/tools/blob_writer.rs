use super::prelude::*;

pub(crate) struct BlobWriter {
    file: File,
    cache: Option<Vec<Record>>,
    written_cached: u64,
    written: u64,
}

impl BlobWriter {
    pub(crate) fn from_path<P: AsRef<Path>>(path: P, cache_written: bool) -> AnyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let cache = if cache_written { Some(vec![]) } else { None };
        Ok(BlobWriter {
            file,
            written: 0,
            cache,
            written_cached: 0,
        })
    }

    pub(crate) fn written(&self) -> u64 {
        self.written
    }

    pub(crate) fn write_header(&mut self, header: &BlobHeader) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, header)?;
        self.written += bincode::serialized_size(&header).with_context(|| "write header")?;
        self.validate_written_header(header)?;
        Ok(())
    }

    pub(crate) fn write_record(&mut self, record: Record) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, &record.header).with_context(|| "write header")?;
        let mut written = 0;
        written += bincode::serialized_size(&record.header)?;
        let meta = bincode::serialize(&record.meta)?;
        self.file.write_all(&meta)?;
        written += meta.len() as u64;
        self.file.write_all(&record.data)?;
        written += record.data.len() as u64;
        debug!("Record written: {:?}", record);
        if let Some(cache) = &mut self.cache {
            cache.push(record);
            self.written_cached += written;
        }
        self.written += written;
        Ok(())
    }

    pub(crate) fn clear_cache(&mut self) {
        if let Some(cache) = &mut self.cache {
            cache.clear();
            self.written_cached = 0;
        }
    }

    pub(crate) fn validate_written_header(&mut self, written_header: &BlobHeader) -> AnyResult<()> {
        let current_position = self.written;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BlobReader::from_file(file)?;
        let header = reader.read_header()?;
        if header != *written_header {
            return Err(Error::blob_header_validation_error(
                "validation of written blob header failed",
            )
            .into());
        }
        self.file.seek(SeekFrom::Start(current_position))?;
        debug!("written header validated");
        Ok(())
    }

    pub(crate) fn validate_written_records(&mut self) -> AnyResult<()> {
        let cache = if let Some(cache) = &mut self.cache {
            cache
        } else {
            return Ok(());
        };
        if cache.is_empty() {
            return Ok(());
        }
        debug!("Start validation of written records");
        let current_position = self.written;
        let start_position = current_position
            .checked_sub(self.written_cached)
            .expect("Should be correct");
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start_position))?;
        let mut reader = BlobReader::from_file(file)?;
        for record in cache.iter() {
            let written_record = reader.read_single_record()?;
            if record != &written_record {
                return Err(Error::record_validation_error(
                    "Written and cached records is not equal",
                )
                .into());
            }
        }
        self.file.seek(SeekFrom::Start(current_position))?;
        debug!("{} written records validated", cache.len());
        Ok(())
    }
}
