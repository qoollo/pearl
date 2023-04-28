use std::time::SystemTime;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::time::Instant;

use crate::error::ValidationErrorKind;
use crate::filter::{CombinedFilter, FilterTrait};
use crate::record::PartiallySerializedRecord;
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
    name: FileName,
    file: File,
    created_at: SystemTime,
    validate_data_during_index_regen: bool,
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
        let file = iodriver.create(name.to_path()).await?;
        let index = Self::create_index(name.clone(), iodriver, index_config);
        let header = Header::new();
        let mut blob = Self {
            header,
            index,
            name,
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
        Ok(())
    }

    #[inline]
    fn create_index(mut name: FileName, iodriver: IoDriver, index_config: IndexConfig) -> Index<K> {
        name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        Index::new(name, iodriver, index_config)
    }

    pub(crate) async fn dump(&mut self) -> Result<usize> {
        if self.index.on_disk() {
            Ok(0) // 0 bytes dumped
        } else {
            self.fsyncdata()
                .await
                .with_context(|| format!("blob file dump failed: {:?}", self.name.to_path()))?;

            self.index.dump(self.file_size()).await.with_context(|| {
                format!(
                    "index file dump failed, associated blob file: {:?}",
                    self.name.to_path()
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

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
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

        let header = Header::from_file(&name, iodriver.clone())
            .await
            .with_context(|| format!("failed to read blob header. Blob file: {:?}", path))?;

        let mut index_name = name.clone();
        let BlobConfig {
            index: index_config,
            validate_data_during_index_regen,
        } = config;
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
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
            name,
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
                    self.name.to_path()
                )
            })?;
        debug!("raw records loaded");
        if let Some(headers) = raw_r.load().await.with_context(|| {
            format!(
                "load headers from blob file failed, {:?}",
                self.name.to_path()
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

    pub(crate) async fn write(blob: &ASRwLock<Self>, key: &K, record: Record) -> Result<()> {
        debug!("blob write");
        // Only one upgradable_read lock is allowed at a time
        let (partially_serialized, mut header) = record.to_partially_serialized_and_header()?;
        let blob = blob.read().await;
        let write_result = partially_serialized.write_to_file(&blob.file).await?;
        header.set_offset_checksum(write_result.blob_offset(), write_result.header_checksum());
        blob.index.push(key, header)?;
        Ok(())
    }

    async fn write_mut(&mut self, key: &K, record: Record) -> Result<RecordHeader> {
        debug!("blob write");
        let (record, mut header) = record.to_partially_serialized_and_header()?;
        let write_result = record.write_to_file(&self.file).await?;
        header.set_offset_checksum(write_result.blob_offset(), write_result.header_checksum());
        self.index.push(key, header.clone())?;
        Ok(header)
    }

    pub(crate) async fn read_last(
        &self,
        key: &K,
        meta: Option<&Meta>,
        check_filters: bool,
    ) -> Result<ReadResult<Bytes>> {
        debug!("blob read any");
        let entry = self.get_entry(key, meta, check_filters).await?;
        match entry {
            ReadResult::Found(entry) => {
                debug!("blob read any entry found");
                let buf = entry
                    .load()
                    .await
                    .with_context(|| {
                        format!(
                            "failed to read key {:?} with meta {:?} from blob {:?}",
                            key,
                            meta,
                            self.name.to_path()
                        )
                    })?
                    .into_data();
                debug!("blob read any entry loaded bytes: {}", buf.len());
                Ok(ReadResult::Found(buf))
            }
            ReadResult::Deleted(ts) => Ok(ReadResult::Deleted(ts)),
            ReadResult::NotFound => Ok(ReadResult::NotFound),
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) async fn read_all_entries(&self, key: &K) -> Result<Vec<Entry>> {
        let headers = self.index.get_all(key).await?;
        debug_assert!(headers
            .iter()
            .zip(headers.iter().skip(1))
            .all(|(x, y)| x.created() >= y.created()));
        Ok(Self::headers_to_entries(headers, &self.file))
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
            .all(|(x, y)| x.created() >= y.created()));
        Ok(Self::headers_to_entries(headers, &self.file))
    }

    pub(crate) async fn mark_all_as_deleted(
        &mut self,
        key: &K,
        only_if_presented: bool,
    ) -> Result<bool> {
        if !only_if_presented || self.index.get_any(key).await?.is_found() {
            self.push_deletion_record(key).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn push_deletion_record(&mut self, key: &K) -> Result<()> {
        let on_disk = self.index.on_disk();
        if on_disk {
            self.load_index().await?;
        }
        let record = Record::deleted(key)?;
        let header = self.write_mut(key, record).await?;
        self.index.push_deletion(key, header)
    }

    fn headers_to_entries(headers: Vec<RecordHeader>, file: &File) -> Vec<Entry> {
        headers
            .into_iter()
            .map(|header| Entry::new(header, file.clone()))
            .collect()
    }

    async fn get_entry(
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
                .get_any(key)
                .await
                .with_context(|| {
                    format!("index get any failed for blob: {:?}", self.name.to_path())
                })?
                .map(|header| {
                    let entry = Entry::new(header, self.file.clone());
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
            .map(|h| BlobRecordTimestamp::new(h.created()));
        if deleted_ts.is_some() {
            headers.truncate(headers.len() - 1);
        }
        let entries = Self::headers_to_entries(headers, &self.file);
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

    pub(crate) async fn contains(
        &self,
        key: &K,
        meta: Option<&Meta>,
    ) -> Result<ReadResult<BlobRecordTimestamp>> {
        debug!("blob contains");
        let contains = self
            .get_entry(key, meta, true)
            .await?
            .map(|e| BlobRecordTimestamp::new(e.created()));
        debug!("blob contains any: {:?}", contains);
        Ok(contains)
    }

    #[inline]
    pub(crate) fn file_size(&self) -> u64 {
        self.file.size()
    }

    pub(crate) fn records_count(&self) -> usize {
        self.index.count()
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        self.file.fsyncdata().await
    }

    #[inline]
    pub(crate) fn id(&self) -> usize {
        self.name.id
    }

    pub(crate) fn index_memory(&self) -> usize {
        self.index.memory_used()
    }

    pub(crate) fn disk_used(&self) -> u64 {
        self.file_size() + self.index.disk_used()
    }
}

#[derive(Debug, Clone)]
pub struct FileName {
    name_prefix: String,
    id: usize,
    extension: String,
    dir: PathBuf,
}

impl FileName {
    pub const fn new(name_prefix: String, id: usize, extension: String, dir: PathBuf) -> Self {
        Self {
            name_prefix,
            id,
            extension,
            dir,
        }
    }

    pub(crate) fn from_path(path: &Path) -> Result<Self> {
        Self::try_from_path(path).ok_or_else(|| Error::file_pattern(path.to_owned()).into())
    }

    pub fn to_path(&self) -> PathBuf {
        self.dir.join(self.to_string())
    }

    fn try_from_path(path: &Path) -> Option<Self> {
        let extension = path.extension()?.to_str()?.to_owned();
        let stem = path.file_stem()?;
        let mut parts = stem
            .to_str()?
            .splitn(2, '.')
            .collect::<Vec<_>>()
            .into_iter();
        let name_prefix = parts.next()?.to_owned();
        let id = parts.next()?.parse().ok()?;
        let dir = path.parent()?.to_owned();
        Some(Self {
            name_prefix,
            id,
            extension,
            dir,
        })
    }

    fn exists(&self) -> bool {
        self.to_path().exists()
    }
}

impl Display for FileName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}.{}.{}", self.name_prefix, self.id, self.extension)
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
            .await?;
        let (magic_byte_buf, key_len_buf) = buf.split_at(size_of_magic_byte);
        debug!("blob raw records start, read at {} bytes", buf.len());
        let magic_byte = bincode::deserialize::<u64>(magic_byte_buf)
            .context("failed to deserialize magic byte")?;
        Self::check_record_header_magic_byte(magic_byte)?;
        let key_len = bincode::deserialize::<usize>(key_len_buf)
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
                Record::data_checksum_audit(&header, &data)
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
            .with_context(|| format!("read at call failed, size {}", self.current_offset))?;
        let header = RecordHeader::from_raw(&buf)
            .map_err(|e| Error::from(ErrorKind::Bincode(e.to_string())))
            .with_context(|| {
                format!(
                    "header deserialization from raw failed, buf len: {}",
                    buf.len()
                )
            })?;
        self.current_offset += self.record_header_size;
        self.current_offset += header.meta_size();
        let data = if read_data {
            buf.resize(header.data_size() as usize, 0);
            buf = self
                .file
                .read_exact_at(buf, self.current_offset)
                .await
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
        self.index.get_filter().contains(&self.index, item).await
    }

    fn check_filter_fast(&self, item: &K) -> FilterResult {
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
