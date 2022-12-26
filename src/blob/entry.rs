use bytes::Bytes;

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
        let mut buf = vec![0; data_size + meta_size];
        // The number of bytes read is checked by File internally.
        self.blob_file
            .read_at(&mut buf, self.header.meta_offset())
            .await
            .with_context(|| "blob load failed")?;
        let mut buf = Bytes::from(buf);
        let data_buf = buf.split_off(meta_size);
        let meta = Meta::from_raw(&buf)?;
        let record = Record::new(self.header.clone(), meta, data_buf);
        record.validate()
    }

    /// Returns only data.
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn load_data(&self) -> Result<Vec<u8>> {
        let data_offset = self.header.data_offset();
        let mut buf = vec![0; self.header.data_size().try_into()?];
        self.blob_file.read_at(&mut buf, data_offset).await?;
        Ok(buf)
    }

    /// Loads meta data from fisk, and returns reference to it.
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn load_meta(&mut self) -> Result<Option<&Meta>> {
        let meta_offset = self.header.meta_offset();
        let mut buf = vec![0; self.header.meta_size().try_into()?];
        self.blob_file.read_at(&mut buf, meta_offset).await?;
        self.meta = Some(Meta::from_raw(&buf)?);
        Ok(self.meta.as_ref())
    }

    pub(crate) fn new(header: RecordHeader, blob_file: File) -> Self {
        Self {
            meta: None,
            header,
            blob_file,
        }
    }
}
