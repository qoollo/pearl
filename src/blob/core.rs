use std::time::SystemTime;

use bytes::{BufMut, BytesMut};
use tokio::time::Instant;

use crate::error::ValidationErrorKind;
use crate::filter::{CombinedFilter, FilterTrait};
use crate::storage::{BlobRecordTimestamp, ReadResult};

use super::prelude::*;

use super::{header::Header, index::IndexTrait};

pub(crate) const BLOB_INDEX_FILE_EXTENSION: &str = "index";

/// A [`Blob`] struct representing file with records,
/// provides methods for read/write access by key
///
/// [`Blob`]: struct.Blob.html
#[derive(Debug)]
pub struct Blob<K>
where
    for<'a> K: Key<'a>,
{
    header: Header,
    index: Index<K>,
    name: Arc<FileName>,
    file: File,
    created_at: SystemTime,
    validate_data_during_index_regen: bool,
}

pub(crate) struct WriteResult {
    pub dirty_bytes: u64
}

pub(crate) struct DeleteResult {
    pub dirty_bytes: u64,
    pub deleted: bool
}

impl<K> Blob<K>
where
    for<'a> K: Key<'a> + 'static,
{
    /// # Description
    /// Creates new blob file with given [`FileName`].
    /// And creates index from existing `.index` file or scans corresponding blob.
    /// # Panic
    /// Panics if file with same path already exists
    ///
    /// [`FileName`]: struct.FileName.html
    pub(crate) async fn open_new(
        name: FileName,
        iodriver: IoDriver,
        config: BlobConfig,
    ) -> Result<Self> {
        let BlobConfig {
            index: index_config,
            validate_data_during_index_regen,
        } = config;
        let file = iodriver.create(name.as_path()).await?;
        let index = Self::create_index(&name, iodriver, index_config);
        let header = Header::new();
        let mut blob = Self {
            header,
            index,
            name: Arc::new(name),
            file,
            created_at: SystemTime::now(),
            validate_data_during_index_regen,
        };
        blob.write_header().await?;
        Ok(blob)
    }

    pub fn name(&self) -> &FileName {
        &self.name
    }

    pub(crate) fn created_at(&self) -> SystemTime {
        self.created_at
    }

    async fn write_header(&mut self) -> Result<()> {
        let size = self.header.serialized_size();
        let mut buf = BytesMut::with_capacity(size as usize);
        serialize_into((&mut buf).writer(), &self.header)?;
        self.file.write_append_all(buf.freeze()).await?;
        self.file.fsyncdata().await?;
        Ok(())
    }

    #[inline]
    fn create_index(name: &FileName, iodriver: IoDriver, index_config: IndexConfig) -> Index<K> {
        Index::new(name.with_extension(BLOB_INDEX_FILE_EXTENSION), iodriver, index_config)
    }

    pub(crate) async fn dump(&mut self) -> Result<usize> {
        if self.index.on_disk() {
            Ok(0) // 0 bytes dumped
        } else {
            self.fsyncdata()
                .await
                .with_context(|| format!("blob file dump failed: {:?}", self.name.as_path()))?;

            self.index.dump(self.file_size()).await.with_context(|| {
                format!(
                    "index file dump failed, associated blob file: {:?}",
                    self.name.as_path()
                )
            })
        }
    }

    pub(crate) async fn load_index(&mut self) -> Result<()> {
        if let Err(e) = self.index.load(self.file_size()).await {
            warn!("error loading index: {}, regenerating", e);
            self.index.clear();
            self.try_regenerate_index().await?;
        }
        Ok(())
    }

    pub(crate) async fn from_file(
        path: PathBuf,
        iodriver: IoDriver,
        config: BlobConfig,
    ) -> Result<Self> {
        let now = Instant::now();
        let file = iodriver.open(&path).await?;
        let name = FileName::from_path(&path)?;
        info!("{} blob init started", name);
        let size = file.size();

        let header = Header::from_file(&file, &path)
            .await
            .with_context(|| format!("failed to read blob header. Blob file: {:?}", path))?;

        let index_name = name.with_extension(BLOB_INDEX_FILE_EXTENSION);
        let BlobConfig {
            index: index_config,
            validate_data_during_index_regen,
        } = config;
        trace!("looking for index file: [{}]", index_name);
        let mut is_index_corrupted = false;
        let index = if index_name.exists() {
            trace!("file exists");
            Index::from_file(
                index_name.clone(),
                index_config.clone(),
                iodriver.clone(),
                size,
            )
            .await
            .or_else(|error| {
                if let Some(io_error) = error.downcast_ref::<IOError>() {
                    match io_error.kind() {
                        IOErrorKind::PermissionDenied | IOErrorKind::Other => {
                            warn!(
                                "index for file '{:?}' cannot be regenerated due to an error: {}",
                                path, io_error
                            );
                            return Err(error);
                        }
                        _ => {}
                    }
                }
                is_index_corrupted = true;
                Ok(Index::new(index_name, iodriver, index_config))
            })?
        } else {
            trace!("file not found, create new");
            Index::new(index_name, iodriver, index_config)
        };
        trace!("index initialized");
        let header_size = bincode::serialized_size(&header)?;
        let created_at = file.created_at()?;
        let mut blob = Self {
            header,
            file,
            name: Arc::new(name),
            index,
            created_at,
            validate_data_during_index_regen,
        };
        trace!("call update index");
        if is_index_corrupted || size as u64 > header_size {
            blob.try_regenerate_index()
                .await
                .with_context(|| format!("failed to regenerate index for blob file: {:?}", path))?;
        } else {
            warn!("empty or corrupted blob: {:?}", path);
        }
        trace!("check data consistency");
        Self::check_data_consistency();
        info!(
            "{} init finished: {}ms",
            blob.name(),
            now.elapsed().as_millis()
        );
        Ok(blob)
    }

    async fn raw_records(&self, validate_data: bool) -> Result<RawRecords> {
        RawRecords::start(
            self.file.clone(),
            bincode::serialized_size(&self.header)?,
            K::LEN as usize,
            validate_data,
        )
        .await
        .context("failed to create iterator for raw records")
    }

    pub(crate) async fn try_regenerate_index(&mut self) -> Result<()> {
        info!("try regenerate index for blob: {}", self.name);
        if self.index.on_disk() {
            debug!("index already updated");
            return Ok(());
        }
        debug!("index file missed");
        let raw_r = self
            .raw_records(self.validate_data_during_index_regen)
            .await
            .with_context(|| {
                format!(
                    "failed to read raw records from blob {:?}",
                    self.name.as_path()
                )
            })?;
        debug!("raw records loaded");
        if let Some(headers) = raw_r.load().await.with_context(|| {
            format!(
                "load headers from blob file failed, {:?}",
                self.name.as_path()
            )
        })? {
            for header in headers {
                let key = header.key().into();
                self.index.push(&key, header).context("index push failed")?;
            }
        }
        debug!("index successfully generated: {}", self.index.name());
        Ok(())
    }

    pub(crate) fn check_data_consistency() {
        // @TODO implement
    }

    pub(crate) async fn write(blob: &ASRwLock<Self>, key: &K, record: Record) -> Result<WriteResult> {
        debug!("blob write");
        let (partially_serialized, mut header) = record.to_partially_serialized_and_header()?;
        // Only one upgradable_read lock is allowed at a time. This is critical because we want to
        // be sure that only one write operation is running at a time
        let blob = blob.upgradable_read().await;
        let write_result = partially_serialized.write_to_file(&blob.file).await?;
        header.set_offset_checksum(write_result.blob_offset(), write_result.header_checksum());
        blob.index.push(key, header)?;
        Ok(WriteResult { dirty_bytes: blob.file.dirty_bytes() })
    }

    async fn write_mut(&mut self, key: &K, record: Record) -> Result<WriteResult> {
        debug!("blob write");
        let (record, mut header) = record.to_partially_serialized_and_header()?;
        let write_result = record.write_to_file(&self.file).await?;
        header.set_offset_checksum(write_result.blob_offset(), write_result.header_checksum());
        self.index.push(key, header)?;
        Ok(WriteResult { dirty_bytes: self.file.dirty_bytes() })
    }

    #[inline]
    pub(crate) async fn read_all_entries_with_deletion_marker(
        &self,
        key: &K,
    ) -> Result<Vec<Entry>> {
        let headers = self.index.get_all_with_deletion_marker(key).await?;
        debug_assert!(headers
            .iter()
            .zip(headers.iter().skip(1))
            .all(|(x, y)| x.timestamp() >= y.timestamp()));
        Ok(Self::headers_to_entries(headers, &self.file, &self.name))
    }

    pub(crate) async fn delete(
        &mut self,
        key: &K,
        timestamp: BlobRecordTimestamp,
        meta: Option<Meta>,
        only_if_presented: bool,
    ) -> Result<DeleteResult> {
        if !only_if_presented || self.index.get_latest(key).await?.is_found() {
            let record = Record::deleted(key, timestamp.into(), meta)?;
            self.push_deletion_record(key, record).await
        } else {
            Ok(DeleteResult { dirty_bytes: self.file.dirty_bytes(), deleted: false })
        }
    }

    async fn push_deletion_record(&mut self, key: &K, record: Record) -> Result<DeleteResult> {
        let on_disk = self.index.on_disk();
        if on_disk {
            self.load_index().await?;
        }
        let result = self.write_mut(key, record).await?;
        Ok(DeleteResult { dirty_bytes: result.dirty_bytes, deleted: true })
    }

    fn headers_to_entries(headers: Vec<RecordHeader>, file: &File, file_name: &Arc<FileName>) -> Vec<Entry> {
        headers
            .into_iter()
            .map(|header| Entry::new(header, file.clone(), file_name.clone()))
            .collect()
    }

    /// Returns latest Entry from Blob for specified key and meta
    pub(crate) async fn get_latest_entry(
        &self,
        key: &K,
        meta: Option<&Meta>,
        check_filters: bool,
    ) -> Result<ReadResult<Entry>> {
        debug!("blob get any entry {:?}, {:?}", key, meta);
        if check_filters && self.check_filter(key).await == FilterResult::NotContains {
            debug!("Key was filtered out by filters");
            Ok(ReadResult::NotFound)
        } else if let Some(meta) = meta {
            debug!("blob get any entry meta: {:?}", meta);
            self.get_entry_with_meta(key, meta).await
        } else {
            debug!("blob get any entry bloom true no meta");
            Ok(self
                .index
                .get_latest(key)
                .await
                .with_context(|| {
                    format!("index get any failed for blob: {:?}", self.name.as_path())
                })?
                .map(|header| {
                    let entry = Entry::new(header, self.file.clone(), self.name.clone());
                    debug!("blob, get any entry, bloom true no meta, entry found");
                    entry
                }))
        }
    }

    async fn get_entry_with_meta(&self, key: &K, meta: &Meta) -> Result<ReadResult<Entry>> {
        let mut headers = self.index.get_all_with_deletion_marker(key).await?;
        let deleted_ts = headers
            .last()
            .filter(|h| h.is_deleted())
            .map(|h| BlobRecordTimestamp::new(h.timestamp()));
        if deleted_ts.is_some() {
            headers.truncate(headers.len() - 1);
        }
        let entries = Self::headers_to_entries(headers, &self.file, &self.name);
        if let Some(entries) = self.filter_entries(entries, meta).await? {
            Ok(ReadResult::Found(entries))
        } else {
            if let Some(ts) = deleted_ts {
                return Ok(ReadResult::Deleted(ts));
            }
            Ok(ReadResult::NotFound)
        }
    }

    async fn filter_entries(&self, entries: Vec<Entry>, meta: &Meta) -> Result<Option<Entry>> {
        for mut entry in entries {
            if Some(meta) == entry.load_meta().await? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    #[inline]
    pub(crate) fn file_size(&self) -> u64 {
        self.file.size()
    }

    pub(crate) fn records_count(&self) -> usize {
        self.index.count()
    }

    pub(crate) fn file_dirty_bytes(&self) -> u64 {
        self.file.dirty_bytes()
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        self.file.fsyncdata().await
    }

    #[inline]
    pub(crate) fn id(&self) -> usize {
        self.name.id()
    }

    pub(crate) fn index_memory(&self) -> usize {
        self.index.memory_used()
    }

    pub(crate) fn disk_used(&self) -> u64 {
        self.file_size() + self.index.disk_used()
    }
}


struct RawRecords {
    current_offset: u64,
    record_header_size: u64,
    file: File,
    validate_data: bool,
}

impl RawRecords {
    async fn start(
        file: File,
        blob_header_size: u64,
        key_size: usize,
        validate_data: bool,
    ) -> Result<Self> {
        let current_offset = blob_header_size;
        debug!("blob raw records start, current offset: {}", current_offset);
        let size_of_len = bincode::serialized_size(&(0_usize))? as usize;
        let size_of_magic_byte = bincode::serialized_size(&RECORD_MAGIC_BYTE)? as usize;
        debug!(
            "blob raw records start, read at: size {}, offset: {}",
            size_of_len,
            current_offset + size_of_len as u64
        );
        // plus size of usize because serialized
        // vector contains usize len in front
        let buf = file
            .read_exact_at_allocate(size_of_magic_byte + size_of_len, current_offset)
            .await
            .map_err(|err| err.into_bincode_if_unexpected_eof())
            .context("Can't read BLOB header from file")?;
        let (magic_byte_buf, key_len_buf) = buf.split_at(size_of_magic_byte);
        debug!("blob raw records start, read at {} bytes", buf.len());
        let magic_byte = bincode::deserialize::<u64>(magic_byte_buf)
            .map_err(|err| Error::from(err))
            .context("failed to deserialize magic byte")?;
        Self::check_record_header_magic_byte(magic_byte)?;
        let key_len = bincode::deserialize::<usize>(key_len_buf)
            .map_err(|err| Error::from(err))
            .context("failed to deserialize index buf vec length")?;
        if key_len != key_size {
            let msg = "blob key_size is not equal to pearl compile-time key size";
            return Err(Error::validation(ValidationErrorKind::BlobKeySize, msg).into());
        }
        let record_header_size = RecordHeader::default().serialized_size() + key_len as u64;
        debug!(
            "blob raw records start, record header size: {}",
            record_header_size
        );
        Ok(Self {
            current_offset,
            record_header_size,
            file,
            validate_data,
        })
    }

    fn check_record_header_magic_byte(magic_byte: u64) -> Result<()> {
        if magic_byte == RECORD_MAGIC_BYTE {
            Ok(())
        } else {
            let param = ValidationErrorKind::RecordMagicByte;
            Err(Error::validation(param, "First record's magic byte is wrong").into())
        }
    }

    async fn load(mut self) -> Result<Option<Vec<RecordHeader>>> {
        debug!("blob raw records load");
        let mut headers = Vec::new();
        while self.current_offset < self.file.size() {
            let (header, data) = self
                .read_current_record(self.validate_data)
                .await
                .with_context(|| {
                    format!(
                        "read record header or data failed, at {}",
                        self.current_offset
                    )
                })?;
            if let Some(data) = data {
                header.data_checksum_audit(&data)
                    .with_context(|| format!("bad data checksum, at {}", self.current_offset))?;
            }
            headers.push(header);
        }
        if headers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(headers))
        }
    }

    async fn read_current_record(
        &mut self,
        read_data: bool,
    ) -> Result<(RecordHeader, Option<BytesMut>)> {
        let mut buf = self
            .file
            .read_exact_at_allocate(self.record_header_size as usize, self.current_offset)
            .await
            .map_err(|err| err.into_bincode_if_unexpected_eof())
            .with_context(|| format!("read at call failed, size {}", self.current_offset))?;
        let header = RecordHeader::from_raw(&buf)
            .map_err(|e| Error::from(ErrorKind::Bincode(e.to_string())))
            .with_context(|| {
                format!(
                    "header deserialization from raw failed, buf len: {}",
                    buf.len()
                )
            })?;
        header.validate()?;
        self.current_offset += self.record_header_size;
        self.current_offset += header.meta_size();
        let data = if read_data {
            buf.resize(header.data_size() as usize, 0);
            buf = self
                .file
                .read_exact_at(buf, self.current_offset)
                .await
                .map_err(|err| err.into_bincode_if_unexpected_eof())
                .with_context(|| format!("read at call failed, size {}", self.current_offset))?;
            Some(buf)
        } else {
            None
        };
        self.current_offset += header.data_size();
        Ok((header, data))
    }
}

#[async_trait::async_trait]
impl<K> BloomProvider<K> for Blob<K>
where
    for<'a> K: Key<'a> + 'static,
{
    type Filter = CombinedFilter<K>;
    async fn check_filter(&self, item: &K) -> FilterResult {
        match self.index.contains_key_fast(item) {
            Some(true) => { return FilterResult::NeedAdditionalCheck; },
            Some(false) => { return FilterResult::NotContains; },
            None => { }
        }
        
        self.index.get_filter().contains(&self.index, item).await
    }

    fn check_filter_fast(&self, item: &K) -> FilterResult {
        match self.index.contains_key_fast(item) {
            Some(true) => { return FilterResult::NeedAdditionalCheck; },
            Some(false) => { return FilterResult::NotContains; },
            None => { }
        }
        
        self.index.get_filter().contains_fast(item)
    }

    async fn offload_buffer(&mut self, _: usize, _: usize) -> usize {
        self.index.offload_filter()
    }

    async fn get_filter(&self) -> Option<Self::Filter> {
        Some(self.index.get_filter().clone())
    }

    fn get_filter_fast(&self) -> Option<&Self::Filter> {
        Some(self.index.get_filter())
    }

    async fn filter_memory_allocated(&self) -> usize {
        self.index.get_filter().memory_allocated()
    }
}
