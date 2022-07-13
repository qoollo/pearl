use super::prelude::*;

pub(crate) struct BlobReader {
    file: File,
    position: u64,
    len: u64,
    latest_wrong_header: Option<Header>,
}

impl BlobReader {
    pub(crate) fn from_path<P: AsRef<Path>>(path: P) -> AnyResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(BlobReader {
            len: file.metadata()?.len(),
            file,
            position: 0,
            latest_wrong_header: None,
        })
    }

    pub(crate) fn from_file(mut file: File) -> AnyResult<BlobReader> {
        let position = file.stream_position()?;
        let len = file.metadata()?.len();
        Ok(BlobReader {
            file,
            position,
            len,
            latest_wrong_header: None,
        })
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.position >= self.len
    }

    fn read_data<T>(&mut self) -> AnyResult<T>
    where
        T: serde::de::DeserializeOwned + serde::Serialize,
    {
        let data = bincode::deserialize_from(&mut self.file)?;
        self.position += bincode::serialized_size(&data)?;
        Ok(data)
    }

    fn read_bytes(&mut self, count: usize) -> AnyResult<Vec<u8>> {
        let mut data = vec![0; count as usize];
        self.file
            .read_exact(&mut data)
            .with_context(|| "read record meta")?;
        self.position += count as u64;
        Ok(data)
    }

    pub(crate) fn read_header(&mut self) -> AnyResult<BlobHeader> {
        let header: BlobHeader = self.read_data().with_context(|| "read blob header")?;
        header
            .validate_without_version()
            .with_context(|| "validate blob header")?;
        Ok(header)
    }

    pub(crate) fn read_single_record(&mut self) -> AnyResult<Record> {
        let header: Header =
            bincode::deserialize_from(&mut self.file).with_context(|| "read record header")?;
        self.position += bincode::serialized_size(&header)?;
        header.validate().map_err(|err| {
            self.latest_wrong_header = Some(header.clone());
            Error::record_header_validation_error(err.to_string())
        })?;
        self.latest_wrong_header = None;

        let meta = self
            .read_bytes(header.meta_size() as usize)
            .with_context(|| "read record meta")?;
        let meta = bincode::deserialize(&meta)?;

        let data = self
            .read_bytes(header.data_size() as usize)
            .with_context(|| "read record data")?;

        let record = Record { header, meta, data };
        let record = record
            .validate()
            .map_err(|err| Error::record_validation_error(err.to_string()))?;
        Ok(record)
    }

    pub(crate) fn skip_wrong_record_data(&mut self) -> AnyResult<()> {
        debug!("Trying to skip wrong record data");
        let header = self
            .latest_wrong_header
            .as_ref()
            .ok_or_else(|| Error::skip_record_data_error("wrong header not found"))?;
        let position = self
            .position
            .checked_add(header.data_size())
            .and_then(|x| x.checked_add(header.meta_size()))
            .ok_or_else(|| Error::skip_record_data_error("position overflow"))?;
        if position >= self.len {
            return Err(Error::skip_record_data_error("position is bigger than file size").into());
        }
        self.file.seek(SeekFrom::Start(position))?;
        debug!("Skipped {} bytes", position - self.position);
        self.position = position;
        Ok(())
    }

    pub(crate) fn read_record(&mut self, skip_wrong: bool) -> AnyResult<Record> {
        if skip_wrong {
            return self.read_single_record();
        }
        match self.read_single_record() {
            Ok(record) => Ok(record),
            Err(error) => {
                match error.downcast_ref::<Error>() {
                    Some(Error::RecordValidation(_)) => {}
                    Some(Error::RecordHeaderValidation(_)) => self.skip_wrong_record_data()?,
                    _ => return Err(error),
                }
                warn!("Record read error, trying read next record: {}", error);
                self.read_single_record()
            }
        }
    }
}
