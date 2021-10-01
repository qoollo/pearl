use tokio::time::Instant;

use super::prelude::*;

use super::index::IndexTrait;

const BLOB_VERSION: u32 = 1;
const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;
pub(crate) const BLOB_INDEX_FILE_EXTENSION: &str = "index";

/// A [`Blob`] struct representing file with records,
/// provides methods for read/write access by key
///
/// [`Blob`]: struct.Blob.html
#[derive(Debug)]
pub struct Blob {
    header: Header,
    index: Index,
    name: FileName,
    file: File,
    current_offset: Arc<Mutex<u64>>,
}

impl Blob {
    /// # Description
    /// Creates new blob file with given [`FileName`].
    /// And creates index from existing `.index` file or scans corresponding blob.
    /// # Panic
    /// Panics if file with same path already exists
    ///
    /// [`FileName`]: struct.FileName.html
    pub(crate) async fn open_new(
        name: FileName,
        ioring: Option<Rio>,
        index_config: IndexConfig,
    ) -> Result<Self> {
        let file = File::create(name.to_path(), ioring.clone()).await?;
        let index = Self::create_index(name.clone(), ioring, index_config);
        let current_offset = Arc::new(Mutex::new(0));
        let header = Header::new();
        let mut blob = Self {
            header,
            index,
            name,
            file,
            current_offset,
        };
        blob.write_header().await?;
        Ok(blob)
    }

    pub fn name(&self) -> &FileName {
        &self.name
    }

    async fn write_header(&mut self) -> Result<()> {
        let buf = serialize(&self.header)?;
        let mut offset = self.current_offset.lock().await;
        let bytes_written = self.file.write_append(&buf).await? as u64;
        *offset = bytes_written;
        Ok(())
    }

    #[inline]
    fn create_index(mut name: FileName, ioring: Option<Rio>, index_config: IndexConfig) -> Index {
        name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        Index::new(name, ioring, index_config)
    }

    pub(crate) async fn dump(&mut self) -> Result<usize> {
        if self.index.on_disk() {
            Ok(0) // 0 bytes dumped
        } else {
            self.fsyncdata()
                .await
                .with_context(|| "Blob file dump failed!")?;
            self.index
                .dump()
                .await
                .with_context(|| "Blob index file dump failed!")
        }
    }

    pub(crate) async fn load_index(&mut self, blob_key_size: u16) -> Result<()> {
        if let Err(e) = self.index.load().await {
            warn!("error loading index: {}, regenerating", e);
            self.index.clear();
            self.try_regenerate_index(blob_key_size as usize).await?;
        }
        Ok(())
    }

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub(crate) async fn from_file(
        path: PathBuf,
        ioring: Option<Rio>,
        index_config: IndexConfig,
        blob_key_size: u16,
    ) -> Result<Self> {
        let now = Instant::now();
        let file = File::open(&path, ioring.clone()).await?;
        let name = FileName::from_path(&path)?;
        info!("{} blob init started", name);
        let size = file.size();
        let header = Header::new();
        Self::check_blob_header(&header)?;
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        trace!("looking for index file: [{}]", index_name);
        let mut is_index_corrupted = false;
        let index = if index_name.exists() {
            trace!("file exists");
            Index::from_file(index_name.clone(), index_config.clone(), ioring.clone())
                .await
                .or_else(|error| {
                    if let Some(io_error) = error.downcast_ref::<IOError>() {
                        match io_error.kind() {
                            IOErrorKind::PermissionDenied | IOErrorKind::Other => {
                                warn!("index cannot be regenerated due to error: {}", io_error);
                                return Err(error);
                            }
                            _ => {}
                        }
                    }
                    is_index_corrupted = true;
                    Ok(Index::new(index_name, ioring, index_config))
                })?
        } else {
            trace!("file not found, create new");
            Index::new(index_name, ioring, index_config)
        };
        trace!("index initialized");
        let header_size = bincode::serialized_size(&header)?;
        let mut blob = Self {
            header,
            file,
            name,
            index,
            current_offset: Arc::new(Mutex::new(size)),
        };
        trace!("call update index");
        if is_index_corrupted || size as u64 > header_size {
            blob.try_regenerate_index(blob_key_size as usize)
                .await
                .context("failed to regenerate index")?;
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

    async fn raw_records(&self, key_size: usize) -> Result<RawRecords> {
        RawRecords::start(
            self.file.clone(),
            bincode::serialized_size(&self.header)?,
            key_size,
        )
        .await
        .context("failed to create iterator for raw records")
    }

    pub(crate) async fn try_regenerate_index(&mut self, key_size: usize) -> Result<()> {
        info!("try regenerate index for blob: {}", self.name);
        if self.index.on_disk() {
            debug!("index already updated");
            return Ok(());
        }
        debug!("index file missed");
        let raw_r = self
            .raw_records(key_size)
            .await
            .context("failed to read raw records")?;
        debug!("raw records loaded");
        if let Some(headers) = raw_r.load().await.with_context(|| {
            format!(
                "load headers from blob file failed, {:?}",
                self.name.to_path()
            )
        })? {
            for header in headers {
                self.index.push(header).context("index push failed")?;
            }
        }
        debug!("index successfully generated: {}", self.index.name());
        Ok(())
    }

    pub(crate) const fn check_data_consistency() {
        // @TODO implement
    }

    fn check_blob_header(header: &Header) -> Result<()> {
        if header.version != BLOB_VERSION {
            panic!(
                "old blob version: {}, expected: {}",
                header.version, BLOB_VERSION
            );
        }
        if header.magic_byte != BLOB_MAGIC_BYTE {
            Ok(())
        } else {
            Err(Error::validation("blob header magic byte is wrong").into())
        }
    }

    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        debug!("blob write");
        let mut offset = self.current_offset.lock().await;
        debug!("blob write record offset: {}", *offset);
        record.set_offset(*offset)?;
        let buf = record.to_raw()?;
        let bytes_written = self
            .file
            .write_append(&buf)
            .await
            .map_err(|e| -> anyhow::Error {
                match e.kind() {
                    kind if kind == IOErrorKind::Other || kind == IOErrorKind::NotFound => {
                        Error::file_unavailable(kind).into()
                    }
                    _ => e.into(),
                }
            })? as u64;
        self.index.push(record.header().clone())?;
        *offset += bytes_written;
        Ok(())
    }

    pub(crate) async fn read_any(
        &self,
        key: &[u8],
        meta: Option<&Meta>,
        check_filters: bool,
    ) -> Result<Vec<u8>> {
        debug!("blob read any");
        let entry = self
            .get_entry(key, meta, check_filters)
            .await?
            .ok_or_else(|| Error::from(ErrorKind::RecordNotFound))?;
        debug!("blob read any entry found");
        let buf = entry
            .load()
            .await
            .with_context(|| format!("failed to read key {:?} with meta {:?}", key, meta))?
            .into_data();
        debug!("blob read any entry loaded bytes: {}", buf.len());
        Ok(buf)
    }

    #[inline]
    pub(crate) async fn read_all_entries(&self, key: &[u8]) -> Result<Option<Vec<Entry>>> {
        let headers = self.index.get_all(key).await?;
        Ok(headers.map(|h| {
            debug!("blob core read all {} headers", h.len());
            Self::headers_to_entries(h, &self.file)
        }))
    }

    fn headers_to_entries(headers: Vec<RecordHeader>, file: &File) -> Vec<Entry> {
        headers
            .into_iter()
            .map(|header| Entry::new(header, file.clone()))
            .collect()
    }

    async fn get_entry(
        &self,
        key: &[u8],
        meta: Option<&Meta>,
        check_filters: bool,
    ) -> Result<Option<Entry>> {
        debug!("blob get any entry {:?}, {:?}", key, meta);
        if check_filters && !self.check_filters(key) {
            debug!("Key was filtered out by filters");
            Ok(None)
        } else if let Some(meta) = meta {
            debug!("blob get any entry meta: {:?}", meta);
            self.get_entry_with_meta(key, meta).await
        } else {
            debug!("blob get any entry bloom true no meta");
            if let Some(header) = self
                .index
                .get_any(key)
                .await
                .with_context(|| "blob index get any failed")?
            {
                let entry = Entry::new(header, self.file.clone());
                debug!("blob, get any entry, bloom true no meta, entry found");
                Ok(Some(entry))
            } else {
                Ok(None)
            }
        }
    }

    async fn get_entry_with_meta(&self, key: &[u8], meta: &Meta) -> Result<Option<Entry>> {
        let headers = self.index.get_all(key).await?;
        if let Some(headers) = headers {
            let entries = Self::headers_to_entries(headers, &self.file);
            self.filter_entries(entries, meta).await
        } else {
            Ok(None)
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

    pub(crate) async fn contains(&self, key: &[u8], meta: Option<&Meta>) -> Result<bool> {
        debug!("blob contains");
        let contains = self.get_entry(key, meta, true).await?.is_some();
        debug!("blob contains any: {}", contains);
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
    pub(crate) const fn id(&self) -> usize {
        self.name.id
    }

    pub(crate) fn check_filters(&self, key: &[u8]) -> bool {
        trace!("check filters (range and bloom)");
        if let FilterResult::NotContains = self.index.check_filters_key(key) {
            false
        } else {
            true
        }
    }

    pub(crate) async fn check_filters_non_blocking(&self, key: &[u8]) -> bool {
        self.check_filters(key)
    }

    pub(crate) fn index_memory(&self) -> usize {
        self.index.memory_used()
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Header {
    magic_byte: u64,
    version: u32,
    flags: u64,
}

impl Header {
    pub const fn new() -> Self {
        Self {
            magic_byte: BLOB_MAGIC_BYTE,
            version: BLOB_VERSION,
            flags: 0,
        }
    }
}

#[derive(Debug)]
pub struct Location {
    offset: u64,
    size: usize,
}

struct RawRecords {
    current_offset: u64,
    record_header_size: u64,
    file: File,
}

impl RawRecords {
    async fn start(file: File, blob_header_size: u64, key_size: usize) -> Result<Self> {
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
        let mut buf = vec![0; size_of_magic_byte + size_of_len];
        file.read_at(&mut buf, current_offset).await?;
        let (magic_byte_buf, key_len_buf) = buf.split_at(size_of_magic_byte);
        debug!("blob raw records start, read at {} bytes", buf.len());
        let magic_byte = bincode::deserialize::<u64>(magic_byte_buf)
            .context("failed to deserialize magic byte")?;
        Self::check_record_header_magic_byte(magic_byte)?;
        let key_len = bincode::deserialize::<usize>(key_len_buf)
            .context("failed to deserialize index buf vec length")?;
        if key_len != key_size {
            let msg = "blob key_sizeis not equal to pearl compile-time key size";
            return Err(Error::validation(msg).into());
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
        })
    }

    fn check_record_header_magic_byte(magic_byte: u64) -> Result<()> {
        if magic_byte == RECORD_MAGIC_BYTE {
            Ok(())
        } else {
            Err(Error::validation("First record's magic byte is wrong").into())
        }
    }

    async fn load(mut self) -> Result<Option<Vec<RecordHeader>>> {
        debug!("blob raw records load");
        let mut headers = Vec::new();
        while self.current_offset < self.file.size() {
            let header = self.read_current_record_header().await.with_context(|| {
                format!("read record header failed, at {}", self.current_offset)
            })?;
            headers.push(header);
        }
        if headers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(headers))
        }
    }

    async fn read_current_record_header(&mut self) -> Result<RecordHeader> {
        let mut buf = vec![0; self.record_header_size as usize];
        self.file
            .read_at(&mut buf, self.current_offset)
            .await
            .with_context(|| format!("read at call failed, size {}", self.current_offset))?;
        let header = RecordHeader::from_raw(&buf).with_context(|| {
            format!(
                "header deserialization from raw failed, buf len: {}",
                buf.len()
            )
        })?;
        self.current_offset += self.record_header_size;
        self.current_offset += header.meta_size();
        self.current_offset += header.data_size();
        Ok(header)
    }
}
