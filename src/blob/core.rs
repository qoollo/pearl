use super::prelude::*;

use super::index::Index;

const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;
const BLOB_INDEX_FILE_EXTENSION: &str = "index";

/// A [`Blob`] struct representing file with records,
/// provides methods for read/write access by key
///
/// [`Blob`]: struct.Blob.html
#[derive(Debug)]
pub struct Blob {
    header: Header,
    index: SimpleIndex,
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
    #[cfg(feature = "aio")]
    pub(crate) async fn open_new(
        name: FileName,
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> Result<Self> {
        let file = File::create(name.to_path(), ioring.clone()).await?;
        let index = Self::create_index(name.clone(), ioring, filter_config);
        let current_offset = Arc::new(Mutex::new(0));
        let header = Header::new();
        let mut blob = Self {
            header,
            file,
            index,
            name,
            current_offset,
        };
        blob.write_header().await?;
        Ok(blob)
    }

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn open_new(
        name: FileName,
        filter_config: Option<BloomConfig>,
    ) -> Result<Self> {
        let file = File::create(name.to_path()).await?;
        let index = Self::create_index(name.clone(), filter_config);
        let current_offset = Arc::new(Mutex::new(0));
        let header = Header::new();
        let mut blob = Self {
            header,
            file,
            index,
            name,
            current_offset,
        };
        blob.write_header().await?;
        Ok(blob)
    }

    pub fn name(&self) -> &FileName {
        &self.name
    }

    #[cfg(feature = "aio")]
    async fn write_header(&mut self) -> Result<()> {
        let buf = serialize(&self.header)?;
        let mut offset = self.current_offset.lock().await;
        let bytes_written = self.file.write_append(&buf).await? as u64;
        *offset += bytes_written;
        Ok(())
    }

    #[cfg(not(feature = "aio"))]
    async fn write_header(&mut self) -> Result<()> {
        let buf = serialize(&self.header)?;
        let mut offset = self.current_offset.lock().await;
        let bytes_written = self.file.write_append(buf).await? as u64;
        *offset += bytes_written;
        Ok(())
    }

    #[cfg(feature = "aio")]
    #[inline]
    fn create_index(
        mut name: FileName,
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> SimpleIndex {
        name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        SimpleIndex::new(name, ioring, filter_config)
    }

    #[cfg(not(feature = "aio"))]
    #[inline]
    fn create_index(mut name: FileName, filter_config: Option<BloomConfig>) -> SimpleIndex {
        name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        SimpleIndex::new(name, filter_config)
    }

    pub(crate) async fn dump(&mut self) -> Result<usize> {
        self.index
            .dump()
            .await
            .with_context(|| "blob index dump failed")
    }

    pub(crate) async fn load_index(&mut self) -> Result<()> {
        self.index.load().await
    }

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    #[cfg(feature = "aio")]
    pub(crate) async fn from_file(
        path: PathBuf,
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> Result<Self> {
        trace!("create file instance");
        let file = File::open(&path, ioring.clone()).await?;
        let name = FileName::from_path(&path)?;
        let size = file.size();
        let header = Header::new();
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        trace!("looking for index file: [{}]", index_name);
        let index = if index_name.exists() {
            trace!("file exists");
            SimpleIndex::from_file(index_name, filter_config.is_some(), ioring).await?
        } else {
            trace!("file not found, create new");
            SimpleIndex::new(index_name, ioring, filter_config)
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
        if size as u64 > header_size {
            blob.try_regenerate_index()
                .await
                .context("failed to regenerate index")?;
        } else {
            warn!("empty or corrupted blob: {:?}", path);
        }
        trace!("check data consistency");
        Self::check_data_consistency()?;
        Ok(blob)
    }

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn from_file(
        path: PathBuf,
        filter_config: Option<BloomConfig>,
    ) -> Result<Self> {
        trace!("create file instance");
        let file = File::open(&path).await?;
        let name = FileName::from_path(&path)?;
        let size = file.size();
        let header = Header::new();
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        trace!("looking for index file: [{}]", index_name);
        let index = if index_name.exists() {
            trace!("file exists");
            SimpleIndex::from_file(index_name, filter_config.is_some()).await?
        } else {
            trace!("file not found, create new");
            SimpleIndex::new(index_name, filter_config)
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
        if size as u64 > header_size {
            blob.try_regenerate_index()
                .await
                .context("failed to regenerate index")?;
        } else {
            warn!("empty or corrupted blob: {:?}", path);
        }
        trace!("check data consistency");
        Self::check_data_consistency()?;
        Ok(blob)
    }

    async fn raw_records(&self) -> Result<RawRecords> {
        RawRecords::start(self.file.clone(), bincode::serialized_size(&self.header)?)
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
            .raw_records()
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

    pub(crate) const fn check_data_consistency() -> Result<()> {
        // @TODO implement
        Ok(())
    }

    #[cfg(feature = "aio")]
    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        debug!("blob write");
        let mut offset = self.current_offset.lock().await;
        debug!("blob write record offset: {}", *offset);
        record.set_offset(*offset)?;
        let buf = record.to_raw()?;
        let bytes_written = self.file.write_append(&buf).await? as u64;
        self.index.push(record.header().clone())?;
        *offset += bytes_written;
        Ok(())
    }

    #[cfg(not(feature = "aio"))]
    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        debug!("blob write");
        let mut offset = self.current_offset.lock().await;
        debug!("blob write record offset: {}", *offset);
        record.set_offset(*offset)?;
        let buf = record.to_raw()?;
        let bytes_written = self.file.write_append(buf).await? as u64;
        self.index.push(record.header().clone())?;
        *offset += bytes_written;
        Ok(())
    }

    pub(crate) async fn read_any(&self, key: &[u8], meta: Option<&Meta>) -> Result<Vec<u8>> {
        debug!("blob read any");
        let entry = self
            .get_entry(key, meta)
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

    async fn get_entry(&self, key: &[u8], meta: Option<&Meta>) -> Result<Option<Entry>> {
        debug!("blob get any entry {:?}, {:?}", key, meta);
        if self.check_bloom(key) == Some(false) {
            debug!("blob core get any entry check bloom returned Some(false)");
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
        let contains = self.get_entry(key, meta).await?.is_some();
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

    pub(crate) fn check_bloom(&self, key: &[u8]) -> Option<bool> {
        trace!("check bloom filter");
        self.index.check_bloom_key(key)
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
            version: 0,
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
    async fn start(file: File, blob_header_size: u64) -> Result<Self> {
        let current_offset = blob_header_size;
        debug!("blob raw records start, current offset: {}", current_offset);
        let size_of_usize = std::mem::size_of::<usize>();
        debug!(
            "blob raw records start, read at: size {}, offset: {}",
            size_of_usize,
            current_offset + size_of_usize as u64
        );
        // plus size of usize because serialized
        // vector contains usize len in front
        let mut buf = vec![0; size_of_usize];
        file.read_at(&mut buf, current_offset + size_of_usize as u64)
            .await?;
        debug!("blob raw records start, read at {} bytes", buf.len());
        let key_len = bincode::deserialize::<usize>(&buf)
            .context("failed to deserialize index buf vec length")?;
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
