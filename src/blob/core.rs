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

    #[inline]
    fn new_offset() -> Arc<Mutex<u64>> {
        Arc::new(Mutex::new(0))
    }

    async fn write_header(&mut self) -> Result<()> {
        let buf = serialize(&self.header)?;
        let offset = self.file.write_at(&buf, 0).await?;
        self.update_offset(offset).await;
        Ok(())
    }

    async fn update_offset(&self, offset: usize) {
        *self.current_offset.lock().await = offset as u64;
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
        self.index.dump().await
    }

    pub(crate) async fn load_index(&mut self) -> AnyResult<()> {
        self.index.load().await
    }

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub(crate) async fn from_file(
        path: PathBuf,
        ioring: Rio,
        filter_config: Option<BloomConfig>,
    ) -> AnyResult<Self> {
        trace!("create file instance");
        let file = File::open(&path, ioring.clone()).await?;
        let name = FileName::from_path(&path)?;
        let len = file.metadata().await?.len();
        let header = Header::new();
        trace!("blob file size: {} MB", len / 1_000_000);
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
            current_offset: Arc::new(Mutex::new(len)),
        };
        trace!("call update index");
        if len > header_size {
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

    async fn raw_records(&mut self) -> AnyResult<RawRecords> {
        RawRecords::start(self.file.clone(), bincode::serialized_size(&self.header)?)
            .await
            .context("failed to create iterator for raw records")
    }

    pub(crate) async fn try_regenerate_index(&mut self) -> AnyResult<()> {
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
        raw_r
            .try_for_each(|h| self.index.push(h))
            .await
            .context("failed to collect headers from blob")?;
        debug!("skip index dump after generation");
        debug!("index successfully generated: {}", self.index.name());
        Ok(())
    }

    pub(crate) const fn check_data_consistency() -> Result<()> {
        // @TODO implement
        Ok(())
    }

    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        trace!("write: {:?} to blob {}", record.header().key(), self.id());
        let mut offset = self.current_offset.lock().await;
        record.set_offset(*offset)?;
        let buf = record.to_raw()?;
        let bytes_written = self.file.write_at(&buf, *offset).await?;
        self.index.push(record.header().clone());
        *offset += bytes_written as u64;
        Ok(())
    }

    pub(crate) async fn read(&self, key: &[u8], meta: Option<&Meta>) -> AnyResult<Record> {
        trace!("lookup for key in {} blob", self.id());
        let loc = self
            .lookup(key, meta)
            .await?
            .ok_or_else(|| Error::from(ErrorKind::RecordNotFound))?;
        trace!("key found");
        let mut buf = vec![0; loc.size as usize];
        self.file
            .read_at(&mut buf, loc.offset)
            .await
            .with_context(|| format!("failed to read key {:?} with meta {:?}", key, meta))?;
        trace!("buf read finished");
        let record = Record::from_raw(&buf).expect("from raw");
        trace!("record deserialized");
        Ok(record)
    }

    #[inline]
    pub(crate) fn read_all<'a>(&'a self, key: &'a [u8]) -> Entries<'a> {
        self.index.get_entry(key, self.file.clone())
    }

    async fn lookup(&self, key: &[u8], meta: Option<&Meta>) -> Result<Option<Location>> {
        if self.check_bloom(key) == Some(false) {
            trace!("bloom filter returned false");
            Ok(None)
        } else {
            trace!("lookup in index");
            let entries = self.index.get_entry(key, self.file.clone());
            trace!("entries get");
            let entry = Self::find_entry(entries, meta).await?;
            trace!("entry found");
            Ok(entry.map(|entry| Location::new(entry.blob_offset(), entry.full_size())))
        }
    }

    pub(crate) async fn contains(&self, key: &[u8], meta: Option<&Meta>) -> Result<bool> {
        Ok(self.lookup(key, meta).await?.is_some())
    }

    async fn find_entry<'a>(ents: Entries<'a>, meta: Option<&'a Meta>) -> Result<Option<Entry>> {
        trace!("find entry with meta: {:?}", meta);
        TryStreamExt::try_next(&mut ents.try_filter(|entry| {
            future::ready(meta.map_or(true, |m| {
                trace!("check meta: {:?} == {:?}", m, entry.meta());
                *m == entry.meta()
            }))
        }))
        .await
    }

    #[inline]
    pub(crate) async fn file_size(&self) -> IOResult<u64> {
        Ok(self.file.metadata().await?.len())
    }

    pub(crate) async fn records_count(&self) -> AnyResult<usize> {
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

    pub(crate) async fn get_all_metas(&self, key: &[u8]) -> AnyResult<Vec<Meta>> {
        debug!("get_all_metas");
        let locations = self.index.get_all_meta_locations(key).await?;
        trace!("gotten all meta locations");
        let mut metas = Vec::new();
        for location in locations {
            let mut meta_raw = vec![0; location.size as usize];
            self.file
                .read_at(&mut meta_raw, location.offset)
                .await
                .with_context(|| format!("failed to get all metas for key {:?}", key))?; // TODO: verify amount of data readed
            let meta = Meta::from_raw(&meta_raw).map_err(Error::new)?;
            metas.push(meta);
        }
        trace!("get all headers finished");
        Ok(metas)
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
        Self::try_from_path(path)
            .ok_or_else(|| ErrorKind::WrongFileNamePattern(path.to_owned()).into())
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
    file_len: u64,
    read_fut: Option<PinBox<dyn Future<Output = AnyResult<RecordHeader>> + Send>>,
}

impl RawRecords {
    async fn start(file: File, blob_header_size: u64) -> AnyResult<Self> {
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
        let file_len = file.metadata().await.map(|m| m.len())?;
        Ok(Self {
            current_offset,
            record_header_size,
            file,
            file_len,
            read_fut: Some(read_fut),
        })
    }

    async fn read_at(file: File, size: u64, offset: u64) -> AnyResult<RecordHeader> {
        trace!("call read_at: {} bytes at {}", size, offset);
        let mut buf = vec![0; size.try_into()?];
        file.read_at(&mut buf, offset).await?; // TODO: verify amount of data readed
        let header = RecordHeader::from_raw(&buf).map_err(Error::new)?;
        Ok(header)
    }

    fn update_future(&mut self, header: &RecordHeader) {
        self.current_offset += self.record_header_size;
        self.current_offset += header.meta_size();
        self.current_offset += header.data_size();
        trace!(
            "file len: {}, current offset: {}",
            self.file_len,
            self.current_offset,
        );
        if self.file_len < self.current_offset + self.record_header_size {
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
    type Item = AnyResult<RecordHeader>;

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
