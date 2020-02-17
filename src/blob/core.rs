use super::prelude::*;

use super::index::Index;

const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;
const BLOB_INDEX_FILE_EXTENSION: &str = "index";

/// A [`Blob`] struct representing file with records,
/// provides methods for read/write access by key
///
/// [`Blob`]: struct.Blob.html
#[derive(Debug)]
pub(crate) struct Blob {
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
    pub(crate) async fn open_new(name: FileName) -> Result<Self> {
        let file = Self::prepare_file(&name)?;
        let index = Self::create_index(&name);
        let current_offset = Self::new_offset();
        let header = Header::new();
        error!("@TODO provide max records in blob");
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
        let offset = self.file.write(&buf).await?;
        self.update_offset(offset).await;
        Ok(())
    }

    async fn update_offset(&self, offset: usize) {
        *self.current_offset.lock().await = offset as u64;
    }

    #[inline]
    fn create_index(name: &FileName) -> SimpleIndex {
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        SimpleIndex::new(index_name)
    }

    #[inline]
    fn prepare_file(name: &FileName) -> IOResult<File> {
        Self::create_file(&name.to_path()).and_then(File::from_std_file)
    }

    pub(crate) async fn dump(&mut self) -> Result<()> {
        self.index.dump().await
    }

    pub(crate) async fn load_index(&mut self) -> Result<()> {
        self.index.load().await
    }

    #[inline]
    fn create_file(path: &Path) -> IOResult<fs::File> {
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
    }

    #[inline]
    fn open_file(path: &Path) -> IOResult<fs::File> {
        fs::OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path)
    }

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub(crate) async fn from_file(path: PathBuf) -> Result<Self> {
        debug!("create file instance");
        let file: File = File::from_std_file(Self::open_file(&path)?)?;
        let name = FileName::from_path(&path)?;
        let len = file.metadata()?.len();
        let header = Header::new();
        debug!("    blob file size: {} MB", len / 1_000_000);
        let mut index_name: FileName = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        debug!("    looking for index file: [{}]", index_name.to_string());
        let index = if index_name.exists() {
            debug!("    file exists");
            SimpleIndex::from_file(index_name).await?
        } else {
            debug!("    file not found, create new");
            SimpleIndex::new(index_name)
        };
        error!("@TODO provide max blob count");
        debug!("    index initialized");
        let header_size = bincode::serialized_size(&header)?;
        let mut blob = Self {
            header,
            file,
            name,
            index,
            current_offset: Arc::new(Mutex::new(len)),
        };
        debug!("call update index");
        if len > header_size {
            blob.try_regenerate_index().await?;
        } else {
            warn!("empty or corrupted blob: {:?}", path);
        }
        debug!("check data consistency");
        Self::check_data_consistency()?;
        Ok(blob)
    }

    async fn raw_records(&mut self) -> Result<RawRecords> {
        RawRecords::start(self.file.clone(), bincode::serialized_size(&self.header)?).await
    }

    pub(crate) async fn try_regenerate_index(&mut self) -> Result<()> {
        info!("try regenerate index for blob: {}", self.name);
        if self.index.on_disk() {
            debug!("index already updated");
            return Ok(());
        }
        debug!("index file missed");
        let raw_r = self.raw_records().await?;
        debug!("raw records loaded");
        raw_r.try_for_each(|h| self.index.push(h)).await?;
        debug!("index entries collected");
        self.dump().await?;
        debug!("index successfully generated: {}", self.index.name());
        Ok(())
    }

    pub(crate) fn check_data_consistency() -> Result<()> {
        // @TODO implement
        Ok(())
    }

    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        let mut offset = self.current_offset.lock().await;
        record.set_offset(*offset)?;
        let buf = record.to_raw()?;
        let bytes_written = self.file.write_at(buf, *offset).await?;
        self.index.push(record.header().clone());
        *offset += bytes_written as u64;
        Ok(())
    }

    pub(crate) async fn read(&self, key: &[u8], meta: Option<&Meta>) -> Result<Record> {
        let loc = self
            .lookup(&key, meta)
            .await
            .ok_or(ErrorKind::RecordNotFound)?;
        debug!("key found");
        let buf = self.file.read_at(loc.size as usize, loc.offset).await?;
        debug!("buf read finished");
        let record = Record::from_raw(&buf).expect("from raw");
        debug!("record deserialized");
        Ok(record)
    }

    #[inline]
    pub(crate) fn read_all<'a>(&'a self, key: &'a [u8]) -> Entries<'a> {
        self.index.get_entry(key, self.file.clone())
    }

    async fn lookup(&self, key: &[u8], meta: Option<&Meta>) -> Option<Location> {
        let entries = self.index.get_entry(key, self.file.clone());
        Self::find_entry(entries, meta)
            .await
            .map(|entry| Location::new(entry.blob_offset(), entry.full_size()))
    }

    async fn find_entry<'a>(ents: Entries<'a>, meta: Option<&'a Meta>) -> Option<Entry> {
        ents.filter(|entry| {
            let cmp = if let Some(m) = meta {
                *m == entry.meta()
            } else {
                true
            };
            future::ready(cmp)
        })
        .next()
        .await
    }

    #[inline]
    pub(crate) fn file_size(&self) -> IOResult<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub(crate) async fn records_count(&self) -> Result<usize> {
        self.index.count().await
    }

    #[inline]
    pub(crate) fn id(&self) -> usize {
        self.name.id
    }

    pub(crate) async fn get_all_metas(&self, key: &[u8]) -> Result<Vec<Meta>> {
        debug!("get_all_metas");
        let locations = self.index.get_all_meta_locations(key).await?;
        trace!("gotten all meta locations");
        let mut metas = Vec::new();
        for location in locations {
            let meta_raw = self
                .file
                .read_at(location.size as usize, location.offset)
                .await?;
            let meta = Meta::from_raw(&meta_raw).map_err(Error::new)?;
            metas.push(meta);
        }
        trace!("get all headers finished");
        Ok(metas)
    }

    pub(crate) fn contains(&self, key: &impl Key) -> bool {
        debug!("check bloom filter");
        self.index.contains_key(key.as_ref())
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
    pub fn new(name_prefix: String, id: usize, extension: String, dir: PathBuf) -> Self {
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
    pub fn new() -> Self {
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
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }
}

struct RawRecords {
    current_offset: u64,
    record_header_size: u64,
    file: File,
    file_len: u64,
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
        let buf = file
            .read_at(size_of_usize, current_offset + size_of_usize as u64)
            .await?;
        let key_len = bincode::deserialize::<usize>(&buf)?;
        let record_header_size = RecordHeader::default().serialized_size() + key_len as u64;
        let read_fut = Self::read_at(file.clone(), record_header_size, current_offset).boxed();
        let file_len = file.metadata().map(|m| m.len())?;
        Ok(Self {
            current_offset,
            record_header_size,
            file,
            file_len,
            read_fut: Some(read_fut),
        })
    }

    async fn read_at(file: File, size: u64, offset: u64) -> Result<RecordHeader> {
        debug!("call read_at: {} bytes at {}", size, offset);
        let buf = file.read_at(size.try_into()?, offset).await?;
        RecordHeader::from_raw(&buf).map_err(Error::new)
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
            self.read_fut = Some(
                Self::read_at(
                    self.file.clone(),
                    self.record_header_size,
                    self.current_offset,
                )
                .boxed(),
            );
        }
    }
}

impl Stream for RawRecords {
    type Item = Result<RecordHeader>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = if let Some(ref mut f) = self.read_fut {
            match ready!(Future::poll(f.as_mut(), cx)) {
                Ok(header) => {
                    self.update_future(&header);
                    Some(Ok(header))
                }
                Err(e) => {
                    error!("{:?}", e);
                    Some(Err(e))
                }
            }
        } else {
            None
        };

        Poll::Ready(res)
    }
}
