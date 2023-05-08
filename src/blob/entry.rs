use bytes::BytesMut;

use super::prelude::*;

/// [`Entry`] is a [`Future`], which contains header and metadata of the record,
/// but does not contain all of the data in memory.
///
/// If you searching for the records with particular meta, you don't need to load
/// full record. When you've found entry with required meta, call [`load`] to get
/// body.
///
/// [`Entry`]: struct.Entry.html
/// [`load`]: struct.Entry.html#method.load
#[derive(Debug)]
pub struct Entry {
    header: RecordHeader,
    meta: Option<Meta>,
    blob_file: File,
}

impl Entry {
    /// Consumes Entry and returns whole loaded record.
    /// # Errors
    /// Returns the error type for I/O operations, see [`std::io::Error`]
    pub async fn load(self) -> Result<Record> {
        let meta_size = self.header.meta_size().try_into()?;
        let data_size: usize = self.header.data_size().try_into()?;
        // The number of bytes read is checked by File internally.
        let buf = self
            .blob_file
            .read_exact_at_allocate(data_size + meta_size, self.header.meta_offset())
            .await
            .map_err(|err| err.into_bincode_if_unexpected_eof())
            .context("Record load failed")?;
        let mut buf = buf.freeze();
        let data_buf = buf.split_off(meta_size);
        let meta = Meta::from_raw(&buf).map_err(|err| Error::from(err))?;
        let record = Record::new(self.header.clone(), meta, data_buf);
        record.validate()
    }

    /// Returns only data.
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn load_data(&self) -> Result<BytesMut> {
        let data_offset = self.header.data_offset();
        self.blob_file
            .read_exact_at_allocate(self.header.data_size().try_into()?, data_offset)
            .await
            .map_err(|err| err.into_bincode_if_unexpected_eof())
            .context("Error loading Record data")
    }

    /// Loads meta data from fisk, and returns reference to it.
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn load_meta(&mut self) -> Result<Option<&Meta>> {
        let meta_offset = self.header.meta_offset();
        let buf = self
            .blob_file
            .read_exact_at_allocate(self.header.meta_size().try_into()?, meta_offset)
            .await
            .map_err(|err| err.into_bincode_if_unexpected_eof())
            .with_context(|| format!("failed to read Record metadata, offset: {}", meta_offset))?;
        self.meta = Some(Meta::from_raw(&buf).map_err(|err| Error::from(err))?);
        Ok(self.meta.as_ref())
    }

    /// Entry marked as deleted
    pub fn is_deleted(&self) -> bool {
        self.header.is_deleted()
    }

    /// Timestamp when entry was created
    pub fn created(&self) -> u64 {
        self.header.created()
    }

    pub(crate) fn new(header: RecordHeader, blob_file: File) -> Self {
        Self {
            meta: None,
            header,
            blob_file,
        }
    }
}
