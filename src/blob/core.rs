use std::path::{Path, PathBuf};
use std::task::{Context, Poll};
use std::{convert::TryInto, error, fmt, fs, pin::Pin, result, sync::Arc};

use bincode::serialize;
use futures::{io::AsyncWriteExt, lock::Mutex, Future, FutureExt, Stream, TryStreamExt};

use super::file::File;
use super::index::Index;
use super::simple_index::SimpleIndex;

use crate::record::{Header as RecordHeader, Record};

const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;
const BLOB_INDEX_FILE_EXTENSION: &str = "index";

pub(crate) type Result<T> = std::result::Result<T, Error>;

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
        let buf = serialize(&self.header).map_err(Error::new)?;
        let offset = self.file.write(&buf).await.map_err(Error::new)?;
        self.update_offset(offset).await?;
        Ok(())
    }

    async fn update_offset(&self, offset: usize) -> Result<()> {
        let mut current_offset = self.current_offset.lock().await;
        *current_offset = offset.try_into().map_err(Error::new)?;
        Ok(())
    }

    #[inline]
    fn create_index(name: &FileName) -> SimpleIndex {
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        SimpleIndex::new(index_name)
    }

    #[inline]
    fn prepare_file(name: &FileName) -> Result<File> {
        File::from_std_file(Self::create_file(&name.as_path())?)
    }

    pub(crate) async fn dump(&mut self) -> Result<()> {
        self.index.dump().await
    }

    pub(crate) async fn load_index(&mut self) -> Result<()> {
        self.index.load().await
    }

    fn create_file(path: &Path) -> Result<fs::File> {
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
            .map_err(Error::new)
    }

    fn open_file(path: &Path) -> Result<fs::File> {
        fs::OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path)
            .map_err(Error::new)
    }

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub(crate) async fn from_file(path: PathBuf) -> Result<Self> {
        debug!("create file instance");
        let file: File = File::from_std_file(Self::open_file(&path)?)?;
        let name = FileName::from_path(&path)?;
        let len = file.metadata().map_err(Error::new)?.len();
        debug!("    blob file size: {:.1} MB", len as f64 / 1_000_000.0);
        let mut index_name: FileName = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        debug!(
            "    looking for index file: [{}]",
            index_name.name_to_string()
        );
        let index = if index_name.exists() {
            error!("        file exists");
            SimpleIndex::from_file(index_name).await?
        } else {
            error!("        file not found, create new");
            SimpleIndex::new(index_name)
        };
        debug!("    index initialized");

        let mut blob = Self {
            header: Header::new(),
            file,
            name,
            index,
            current_offset: Arc::new(Mutex::new(len)),
        };
        error!("call update index");
        blob.try_regenerate_index().await?;
        blob.check_data_consistency()?;

        // @TODO Scan existing file to create index
        Ok(blob)
    }

    fn raw_records(&mut self) -> RawRecords {
        let header_size = bincode::serialized_size(&self.header).unwrap();
        RawRecords::new(self.file.clone(), header_size)
    }

    pub(crate) async fn try_regenerate_index(&mut self) -> Result<()> {
        if self.index.on_disk() {
            debug!("index already updated");
            return Ok(());
        }
        let raw_r = self.raw_records();
        raw_r.try_for_each(|h| self.index.push(h)).await?;
        self.dump().await?;
        let dir = self.name.dir.clone();
        error!("updated index");
        fs::read_dir(&dir)
            .unwrap()
            .map(|r| r.unwrap())
            .for_each(|f| error!("{:?}", f));
        Ok(())
    }

    pub(crate) fn check_data_consistency(&self) -> Result<()> {
        // @TODO implement
        Ok(())
    }

    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        let key = record.key().to_vec();
        if self.index.contains_key(&key).await? {
            return Err(ErrorKind::KeyExists.into());
        }
        let mut offset = self.current_offset.lock().await;
        record.set_offset(*offset).map_err(Error::new)?;
        let buf = record.to_raw().map_err(Error::new)?;
        let bytes_written = self.file.write_at(buf, *offset).await.map_err(Error::new)?;
        self.index.push(record.header().clone());
        *offset += bytes_written as u64;
        Ok(())
    }

    pub(crate) async fn read(&self, key: Vec<u8>) -> Result<Record> {
        debug!("lookup key");
        let loc = self.lookup(&key).await?;
        debug!("read at");
        let buf = self.file.read_at(loc.size as usize, loc.offset).await?;
        debug!("record from raw");
        let record = Record::from_raw(&buf).map_err(Error::new)?;
        debug!("return result");
        Ok(record)
    }

    async fn lookup<K>(&self, key: K) -> Result<Location>
    where
        K: AsRef<[u8]> + Ord,
    {
        debug!("index get");
        let h = self.index.get(key.as_ref()).await?;
        debug!("blob offset");
        let offset = h.blob_offset();
        Ok(Location::new(
            offset as u64,
            h.full_len().map_err(Error::new)?,
        ))
    }

    pub(crate) fn file_size(&self) -> Result<u64> {
        Ok(self.file.metadata().map_err(Error::new)?.len())
    }

    pub(crate) async fn records_count(&self) -> Result<usize> {
        self.index.count().await
    }

    pub(crate) fn id(&self) -> usize {
        self.name.id
    }
}

#[derive(Debug)]
pub(crate) struct Error {
    repr: Repr,
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.repr {
            Repr::Inner(_) => None,
            Repr::Other(src) => Some(src.as_ref()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> result::Result<(), fmt::Error> {
        self.repr.fmt(f)
    }
}

impl Error {
    pub(crate) fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Self {
            repr: Repr::Other(error.into()),
        }
    }

    pub(crate) fn is(&self, othr_kind: &ErrorKind) -> bool {
        if let Repr::Inner(kind) = &self.repr {
            kind == othr_kind
        } else {
            false
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Inner(kind),
        }
    }
}

#[derive(Debug)]
enum Repr {
    Inner(ErrorKind),
    Other(Box<dyn error::Error + 'static + Send + Sync>),
}

impl fmt::Display for Repr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> result::Result<(), fmt::Error> {
        match self {
            Repr::Inner(kind) => write!(f, "{:?}", kind),
            Repr::Other(e) => e.fmt(f),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum ErrorKind {
    KeyExists,
    NotFound,
    WrongFileNamePattern(PathBuf),
    EmptyIndexFile,
    EmptyIndexBunch,
    Index(String),
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

    pub fn as_path(&self) -> PathBuf {
        self.dir.join(self.name_to_string())
    }

    fn name_to_string(&self) -> String {
        format!("{}.{}.{}", self.name_prefix, self.id, self.extension)
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
        self.as_path().exists()
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
struct Location {
    offset: u64,
    size: u64,
}

impl Location {
    fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }
}

type PinBoxFut<T> = Pin<Box<dyn Future<Output = Result<T>>>>;

struct RawRecords {
    current_offset: u64,
    header_size: u64,
    file: File,
    file_len: u64,
    read_fut: Option<PinBoxFut<RecordHeader>>,
}

impl RawRecords {
    fn new(file: File, header_size: u64) -> Self {
        let current_offset = header_size;
        let read_fut = Self::read_at(file.clone(), header_size, current_offset).boxed();
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        RawRecords {
            current_offset,
            header_size,
            file,
            file_len,
            read_fut: Some(read_fut),
        }
    }

    async fn read_at(file: File, size: u64, offset: u64) -> Result<RecordHeader> {
        let buf = file.read_at(size.try_into().unwrap(), offset).await?;
        RecordHeader::from_raw(&buf).map_err(Error::new)
    }

    fn update_future(&mut self, header: &RecordHeader) {
        self.current_offset += self.header_size;
        self.current_offset += header.data_len;
        if self.file_len < self.current_offset + self.header_size {
            self.read_fut = None;
        } else {
            self.read_fut = Some(
                Self::read_at(self.file.clone(), self.header_size, self.current_offset).boxed(),
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
