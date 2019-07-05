use std::io::{self, Read, Seek as StdSeek, SeekFrom, Write as StdWrite};
use std::os::unix::fs::FileExt;
use std::{
    convert::TryInto,
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    result::Result as StdResult,
    sync::Arc,
};
use std::{error, fmt, result};

use bincode::serialize;
use futures::{
    future::Future,
    io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt},
    lock::Mutex,
    task::{Context, Poll},
};

use super::index::Index;
use super::simple_index::SimpleIndex;
use crate::record::Record;

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

#[derive(Debug, Clone)]
pub(crate) struct File {
    read_fd: Arc<fs::File>,
    write_fd: Arc<Mutex<fs::File>>,
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<StdResult<usize, io::Error>> {
        let mut file = self.read_fd.as_ref();
        match file.read(buf) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<StdResult<usize, io::Error>> {
        let mut file = self.read_fd.as_ref();
        match file.write_all(buf) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<StdResult<(), io::Error>> {
        unimplemented!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<StdResult<(), io::Error>> {
        unimplemented!()
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<StdResult<u64, io::Error>> {
        let mut file = self.read_fd.as_ref();
        match file.seek(pos) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

struct WriteAt<'a> {
    fd: &'a mut fs::File,
    buf: Vec<u8>,
    offset: u64,
}

impl<'a> Future for WriteAt<'a> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.fd.write_at(&self.buf, self.offset) {
            Ok(t) => Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(Error::new(e))),
        }
    }
}

struct ReadAt {
    fd: Arc<fs::File>,
    len: usize,
    offset: u64,
}

impl Future for ReadAt {
    type Output = Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut buf = vec![0; self.len];
        match self.fd.read_at(&mut buf, self.offset) {
            Ok(_t) => Poll::Ready(Ok(buf)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(Error::new(e))),
        }
    }
}

impl File {
    pub(crate) fn metadata(&self) -> io::Result<fs::Metadata> {
        self.read_fd.metadata()
    }

    pub(crate) async fn write_at(&mut self, buf: Vec<u8>, offset: u64) -> Result<usize> {
        let mut fd = self.write_fd.lock().await;
        let write_fut = WriteAt {
            fd: &mut fd,
            buf,
            offset,
        };
        write_fut.await
    }

    pub(crate) async fn read_at(&self, len: usize, offset: u64) -> Result<Vec<u8>> {
        let read_fut = ReadAt {
            fd: self.read_fd.clone(),
            len,
            offset,
        };
        read_fut.await
    }

    pub(crate) fn from_std_file(fd: fs::File) -> Result<Self> {
        Ok(File {
            read_fd: Arc::new(fd.try_clone().map_err(Error::new)?),
            write_fd: Arc::new(Mutex::new(fd)),
        })
    }
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
            debug!("        file exists");
            SimpleIndex::from_file(index_name).await?
        } else {
            debug!("        file not found, create new");
            SimpleIndex::new(index_name)
        };
        debug!("    index initialized");

        let blob = Self {
            header: Header::new(),
            file,
            name,
            index,
            current_offset: Arc::new(Mutex::new(len)),
        };
        blob.check_data_consistency()?;

        // @TODO Scan existing file to create index
        Ok(blob)
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
