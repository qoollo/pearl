use bincode::{deserialize, serialize_into};
use futures::{
    future::Future,
    lock::Mutex,
    task::{Context, Poll},
};
use std::{
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use std::os::unix::fs::FileExt;

use crate::record::{self, Header as RecordHeader, Record};

const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;
const BLOB_INDEX_FILE_EXTENSION: &str = "index";

type Result<T> = std::result::Result<T, Error>;

/// A [`Blob`] struct representing file with records,
/// provides methods for read/write access by key
///
/// [`Blob`]: struct.Blob.html
#[derive(Debug)]
pub(crate) struct Blob<I>
where
    I: Index,
{
    header: Header,
    index: I,
    name: FileName,
    file: File,
    current_offset: Arc<Mutex<u64>>,
}

#[derive(Debug, Clone)]
pub(crate) struct File {
    read_fd: Arc<fs::File>,
    write_fd: Arc<Mutex<fs::File>>,
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
            Err(e) => Poll::Ready(Err(Error::IO(e))),
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
            Err(e) => Poll::Ready(Err(Error::IO(e))),
        }
    }
}

impl File {
    fn metadata(&self) -> io::Result<fs::Metadata> {
        self.read_fd.metadata()
    }

    async fn write_at(&mut self, buf: Vec<u8>, offset: u64) -> Result<usize> {
        let mut fd = await!(self.write_fd.lock());
        let write_fut = WriteAt {
            fd: &mut fd,
            buf,
            offset,
        };
        await!(write_fut)
    }

    async fn read_at(&self, len: usize, offset: u64) -> Result<Vec<u8>> {
        let read_fut = ReadAt {
            fd: self.read_fd.clone(),
            len,
            offset,
        };
        await!(read_fut)
    }

    fn from_std_file(fd: fs::File) -> Result<Self> {
        Ok(File {
            read_fd: Arc::new(fd.try_clone()?),
            write_fd: Arc::new(Mutex::new(fd)),
        })
    }
}

pub(crate) trait Index {
    fn new(name: FileName) -> Self;
    fn from_file(name: FileName) -> Result<Self>
    where
        Self: Sized;
    fn get(&self, key: &[u8]) -> Result<RecordHeader>;
    fn push(&mut self, h: RecordHeader);
    fn contains_key(&self, key: &[u8]) -> bool;
    fn count(&self) -> usize;
    fn flush(&mut self) -> Result<()>;
}

#[derive(Debug)]
pub(crate) struct SimpleIndex {
    inner: State,
    name: FileName,
}

#[derive(Debug, Clone)]
enum State {
    InMemory(Vec<RecordHeader>),
    OnDisk(File),
}

impl SimpleIndex {
    fn load(mut file: &fs::File) -> Vec<RecordHeader> {
        let mut meta = Vec::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut meta).unwrap();
        deserialize(meta.as_slice()).unwrap()
    }
}

impl Index for SimpleIndex {
    fn new(name: FileName) -> Self {
        Self {
            inner: State::InMemory(Vec::new()),
            name,
        }
    }

    fn from_file(name: FileName) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(name.as_path())?;
        let meta = Self::load(&file);
        Ok(Self {
            inner: State::InMemory(meta),
            name,
        })
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_ok()
    }

    fn push(&mut self, h: RecordHeader) {
        match &mut self.inner {
            State::InMemory(bunch) => {
                bunch.push(h);
            }
            State::OnDisk(_) => unimplemented!(),
        }
    }

    fn get(&self, key: &[u8]) -> Result<RecordHeader> {
        // @TODO implement binary search
        match &self.inner {
            State::InMemory(bunch) => bunch
                .iter()
                .find(|h| h.key() == key)
                .cloned()
                .ok_or(Error::NotFound),
            State::OnDisk(f) => {
                let index = Self::load(&f.read_fd);
                let i = index
                    .binary_search_by_key(&key, |h| h.key())
                    .map_err(|_| Error::NotFound)?;
                index.get(i).cloned().ok_or(Error::NotFound)
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match &mut self.inner {
            State::InMemory(bunch) => {
                let fd = fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(self.name.as_path())?;
                bunch.sort_by_key(|h| h.key().to_vec());
                serialize_into(&fd, bunch)?;
                self.inner = State::OnDisk(File::from_std_file(fd)?);
            }
            State::OnDisk(_) => unimplemented!(),
        }
        Ok(())
    }

    fn count(&self) -> usize {
        match &self.inner {
            State::InMemory(bunch) => bunch.len(),
            State::OnDisk(_) => unimplemented!(),
        }
    }
}

impl<I> Blob<I>
where
    I: Index,
{
    /// # Description
    /// Creates new blob file with given [`FileName`]
    /// # Panic
    /// Panics if file with same path already exists
    ///
    /// [`FileName`]: struct.FileName.html
    pub(crate) fn open_new(name: FileName) -> Result<Self> {
        let file = File::from_std_file(Self::create_file(&name.as_path())?)?;
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        let blob = Self {
            header: Header::new(),
            file,
            index: Index::new(index_name),
            name,
            current_offset: Arc::new(Mutex::new(0)),
        };
        Ok(blob)
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        self.index.flush()
    }

    fn create_file(path: &Path) -> Result<fs::File> {
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| Error::OpenNew(e, path.into()))
    }

    fn open_file(path: &Path) -> Result<fs::File> {
        Ok(fs::OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path)?)
    }

    pub(crate) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub(crate) fn from_file(path: PathBuf) -> Result<Self> {
        let file: File = File::from_std_file(Self::open_file(&path)?)?;
        let name = FileName::from_path(&path)?;
        let len = file.metadata()?.len();
        let mut index_name = name.clone();
        index_name.extension = BLOB_INDEX_FILE_EXTENSION.to_owned();
        let index = Index::from_file(index_name)?;

        let blob = Self {
            header: Header::new(),
            file,
            name,
            index,
            current_offset: Arc::new(Mutex::new(len)),
        };

        // @TODO Scan existing file to create index
        Ok(blob)
    }

    pub(crate) fn check_data_consistency(&self) -> Result<()> {
        // @TODO implement
        Ok(())
    }

    pub(crate) async fn write(&mut self, mut record: Record) -> Result<()> {
        let key = record.key().to_vec();
        if self.index.contains_key(&key) {
            return Err(Error::AlreadyContainsSameKey);
        }
        let mut offset = await!(self.current_offset.lock());
        record.set_offset(*offset);
        let buf = record.to_raw()?;
        let bytes_written = await!(self.file.write_at(buf, *offset))?;
        self.index.push(record.header().clone());
        *offset += bytes_written as u64;
        Ok(())
    }

    pub(crate) async fn read(&self, key: Vec<u8>) -> Result<Record> {
        let loc = self.lookup(&key)?;
        let buf = await!(self.file.read_at(loc.size as usize, loc.offset))?;
        let record = Record::from_raw(&buf)?;
        Ok(record)
    }

    fn lookup<K>(&self, key: &K) -> Result<Location>
    where
        K: AsRef<[u8]> + Ord,
    {
        let h = self.index.get(key.as_ref())?;
        let offset = h.blob_offset();
        Ok(Location::new(offset as u64, h.full_len()?))
    }

    pub(crate) fn file_size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub(crate) fn records_count(&self) -> usize {
        self.index.count()
    }

    pub(crate) fn id(&self) -> usize {
        self.name.id
    }
}

#[derive(Debug)]
pub enum Error {
    OpenNew(io::Error, PathBuf),
    NotFound,
    RecordFromRawFailed,
    PathWithoutFileName(PathBuf),
    NonUnicode(std::ffi::OsString),
    WrongFileNamePattern(PathBuf),
    IndexParseFailed(String),
    RecordError(record::Error),
    AlreadyContainsSameKey,
    IO(io::Error),
    SerDe(bincode::Error),
}

impl From<bincode::Error> for Error {
    fn from(bincode_error: bincode::Error) -> Self {
        Error::SerDe(bincode_error)
    }
}

impl From<record::Error> for Error {
    fn from(record_error: record::Error) -> Self {
        Error::RecordError(record_error)
    }
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Self {
        Error::IO(io_error)
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

    pub fn from_path(path: &Path) -> Result<Self> {
        Self::try_from_path(path).ok_or_else(|| Error::WrongFileNamePattern(path.to_owned()))
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
}

#[derive(Debug, Clone)]
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
