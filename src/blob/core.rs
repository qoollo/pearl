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
    pub(crate) async fn open_new(
        name: FileName,
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> Result<Self> {
        let file = File::create(name.to_path(), ioring.clone()).await?;
        let index = Self::create_index(&name, ioring, filter_config);
        let current_offset = Self::new_offset();
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

    pub fn name(&self) -> String {
        self.name.to_string()
    }

    #[inline]
    fn new_offset() -> Arc<Mutex<u64>> {
        Arc::new(Mutex::new(0))
    }

    async fn write_header(&mut self) -> Result<()> {
        let buf = serialize(&self.header)?;
        let offset = self.file.write_append(&buf).await? as u64;
        self.update_offset(offset).await;
        Ok(())
    }

    async fn update_offset(&self, offset: u64) {
        *self.current_offset.lock().await = offset;
    }

    #[inline]
    fn create_index(
        name: &FileName,
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> SimpleIndex {
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        SimpleIndex::new(index_name, ioring, filter_config)
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
        let mut index_name: FileName = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        trace!("looking for index file: [{}]", index_name.to_string());
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

    async fn raw_records(&mut self) -> Result<RawRecords> {
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
        let headers = raw_r.try_collect::<Vec<_>>().await?;
        for header in headers {
            self.index
                .push(header)
                .context("failed to push header to index")?;
        }
        debug!("index successfully generated: {}", self.index.name());
        Ok(())
    }

    pub(crate) const fn check_data_consistency() -> Result<()> {
        // @TODO implement
        Ok(())
    }

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

    pub(crate) async fn read_any(&self, key: &[u8], meta: Option<&Meta>) -> Result<Vec<u8>> {
        debug!("blob read any");
        let entry = self
            .get_any_entry(key, meta)
            .await
            .unwrap()
            .ok_or_else(|| Error::from(ErrorKind::RecordNotFound))?;
        debug!("blob read any entry found");
        let buf = entry
            .load()
            .await
            .with_context(|| format!("failed to read key {:?} with meta {:?}", key, meta))
            .unwrap()
            .into_data();
        debug!("blob read any entry loaded bytes: {}", buf.len());
        Ok(buf)
    }

    #[inline]
    pub(crate) async fn read_all(&self, key: &[u8]) -> Result<Option<Vec<Entry>>> {
        let headers = self.index.get_all(key).await?;
        Ok(headers.map(|h| {
            debug!("blob core read all {} headers", h.len());
            Self::headers_to_entries_with_meta(h, self.file.clone())
        }))
    }

    fn headers_to_entries_with_meta(headers: Vec<RecordHeader>, file: File) -> Vec<Entry> {
        headers
            .into_iter()
            .map(|header| Entry::new(header, file.clone()))
            .collect()
    }

    async fn get_any_entry(&self, key: &[u8], meta: Option<&Meta>) -> Result<Option<Entry>> {
        debug!("blob get any entry {:?}, {:?}", key, meta);
        if self.check_bloom(key) == Some(false) {
            debug!("blob core get any entry check bloom returned Some(false)");
            Ok(None)
        } else if let Some(meta) = meta {
            debug!("blob get any entry meta: {:?}", meta);
            let headers = self.index.get_all(key).await?.unwrap();
            let mut entries = headers
                .into_iter()
                .map(|header| Entry::new(header, self.file.clone()))
                .collect::<Vec<_>>();
            for mut entry in entries {
                if entry.load_meta().await.unwrap().unwrap() == meta {
                    return Ok(Some(entry));
                }
            }
            Ok(None)
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

    pub(crate) async fn contains(&self, key: &[u8], meta: Option<&Meta>) -> Result<bool> {
        debug!("blob contains");
        let contains = self.get_any_entry(key, meta).await?.is_some();
        debug!("blob contains any: {}", contains);
        Ok(contains)
    }

    async fn find_entry<'a>(ents: Entries<'a>, meta: &'a Meta) -> Result<Option<Entry>> {
        trace!("find entry with meta: {:?}", meta);
        TryStreamExt::try_next(
            &mut ents.try_filter(|entry| future::ready(Some(meta) == entry.meta())),
        )
        .await
    }

    #[inline]
    pub(crate) fn file_size(&self) -> u64 {
        self.file.size()
    }

    pub(crate) async fn records_count(&self) -> Result<usize> {
        self.index
            .count()
            .await
            .context("failed to get records count from index")
    }

    pub(crate) async fn fsyncdata(&self) -> IOResult<()> {
        self.file.fsyncdata().await
    }

    #[inline]
    pub(crate) const fn id(&self) -> usize {
        self.name.id
    }

    pub(crate) async fn get_all_metas(&self, key: &[u8]) -> Result<Option<Vec<Meta>>> {
        debug!("blob core get all metas");
        if let Some(locations) = self
            .index
            .get_all_meta_locations(key)
            .await
            .with_context(|| "blob get all meta locations failed")?
        {
            if !locations.is_empty() {
                return self.metadata_from_locations(locations).await.map(Some);
            }
        }
        Ok(None)
    }

    async fn metadata_from_locations(&self, locations: Vec<Location>) -> Result<Vec<Meta>> {
        debug!("blob core get all metas got all meta locations");
        locations
            .iter()
            .map(|l| self.location_to_meta(l))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn location_to_meta(&self, location: &Location) -> Result<Meta> {
        let mut buf = vec![0; location.size as usize];
        self.file.read_at(&mut buf, location.offset).await?;
        let meta =
            Meta::from_raw(&buf).with_context(|| "blob get all metas meta creation failed")?;
        Ok(meta)
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

impl Location {
    pub const fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }

    pub(crate) const fn size(&self) -> usize {
        self.size
    }

    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }
}

struct RawRecords {
    current_offset: u64,
    record_header_size: u64,
    file: File,
    file_size: u64,
    read_fut: Option<PinBox<dyn Future<Output = Result<RecordHeader>> + Send>>,
}

impl RawRecords {
    async fn start(file: File, blob_header_size: u64) -> Result<Self> {
        let current_offset = blob_header_size;
        trace!("current offset: {}", current_offset);
        let size_of_usize = std::mem::size_of::<usize>();
        trace!(
            "read at: size {}, offset: {}",
            size_of_usize,
            current_offset + size_of_usize as u64
        );
        // plus size of usize because serialized
        // vector contains usize len in front
        let mut buf = vec![0; size_of_usize];
        file.read_at(&mut buf, current_offset + size_of_usize as u64)
            .await?;
        trace!("read {} bytes", buf.len());
        let key_len = bincode::deserialize::<usize>(&buf)
            .context("failed to deserialize index buf vec length")?;
        let record_header_size = RecordHeader::default().serialized_size() + key_len as u64;
        trace!("record header size: {}", record_header_size);
        let read_fut = Self::read_at(file.clone(), record_header_size, current_offset).boxed();
        let file_size = file.size();
        Ok(Self {
            current_offset,
            record_header_size,
            file,
            file_size,
            read_fut: Some(read_fut),
        })
    }

    async fn read_at(file: File, size: u64, offset: u64) -> Result<RecordHeader> {
        trace!("call read_at: {} bytes at {}", size, offset);
        let mut buf = vec![0; size.try_into()?];
        file.read_at(&mut buf, offset).await?; // TODO: verify amount of data readed
        RecordHeader::from_raw(&buf).map_err(Into::into)
    }

    fn update_future(&mut self, header: &RecordHeader) {
        self.current_offset += self.record_header_size;
        self.current_offset += header.meta_size();
        self.current_offset += header.data_size();
        if self.file_size < self.current_offset + self.record_header_size {
            self.read_fut = None;
        } else {
            let read_fut = Self::read_at(
                self.file.clone(),
                self.record_header_size,
                self.current_offset,
            )
            .boxed();
            self.read_fut = Some(read_fut);
        }
    }
}

impl Stream for RawRecords {
    type Item = Result<RecordHeader>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = if let Some(ref mut f) = self.read_fut {
            Some(ready!(Future::poll(f.as_mut(), cx)).map(|h| {
                self.update_future(&h);
                h
            }))
        } else {
            None
        };

        Poll::Ready(res)
    }
}
