use bincode::*;
use futures::{
    future::Future,
    lock::Mutex,
    task::{Context, Poll},
};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use crate::record::{self, Header as RecordHeader, Record};

const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;

type Result<T> = std::result::Result<T, Error>;

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

#[derive(Debug, Clone)]
struct File {
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
            Err(e) => Poll::Ready(Err(Error::WriteFailed(e))),
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
            Err(e) => Poll::Ready(Err(Error::ReadFailed(e))),
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
}

impl From<fs::File> for File {
    fn from(fd: fs::File) -> Self {
        File {
            read_fd: Arc::new(fd.try_clone().unwrap()),
            write_fd: Arc::new(Mutex::new(fd)),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct Index {
    inner: IndexInner,
}

#[derive(Debug, Clone)]
enum IndexInner {
    InMemory(Vec<RecordMetaData>),
    OnDisk(File),
}

impl Default for IndexInner {
    fn default() -> Self {
        IndexInner::InMemory(Default::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RecordMetaData {
    header: RecordHeader,
    key: Vec<u8>,
}

impl RecordMetaData {
    #[inline]
    fn header(&self) -> &RecordHeader {
        &self.header
    }
}

impl Index {
    fn new(_path: &Path) -> Self {
        // @TODO initialize new index from file
        Self {
            inner: IndexInner::InMemory(Default::default()),
        }
    }

    fn from_default(_path: &Path) -> Result<Self> {
        // @TODO implement
        Ok(Default::default())
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    fn push(&mut self, meta: RecordMetaData) {
        match &mut self.inner {
            IndexInner::InMemory(bunch) => {
                bunch.push(meta);
            }
            IndexInner::OnDisk(f) => unimplemented!(),
        }
    }

    fn get(&self, key: &[u8]) -> Option<&RecordMetaData> {
        match &self.inner {
            IndexInner::InMemory(bunch) => bunch.iter().find(|meta| meta.key == key),
            IndexInner::OnDisk(f) => unimplemented!(),
        }
    }

    fn flush(&mut self) {
        match &mut self.inner {
            IndexInner::InMemory(bunch) => {
                let fd = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open("blob.index")
                    .unwrap();
                bunch.sort_by_key(|meta| meta.key.clone());
                let errors: Vec<_> = bunch
                    .iter()
                    .map(|meta| serialize_into(&fd, meta).unwrap())
                    .collect();
                self.inner = IndexInner::OnDisk(File::from(fd));
            }
            IndexInner::OnDisk(f) => unimplemented!(),
        }
    }

    fn len(&self) -> usize {
        match &self.inner {
            IndexInner::InMemory(bunch) => bunch.len(),
            IndexInner::OnDisk(f) => unimplemented!(),
        }
    }
}

impl Blob {
    /// # Description
    /// Creates new blob file with given [`FileName`]
    /// # Panic
    /// Panics if file with same path already exists
    ///
    /// [`FileName`]: struct.FileName.html
    pub fn open_new(name: FileName) -> Result<Self> {
        let file = Self::create_file(&name.as_path())?.into();
        let blob = Self {
            header: Header::new(),
            file,
            index: Index::new(&name.as_path()),
            name,
            current_offset: Arc::new(Mutex::new(0)),
        };
        Ok(blob)
    }

    pub fn flush(&mut self) {
        self.index.flush();
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
        fs::OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(path)
            .map_err(Error::FromFile)
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub fn from_file(path: PathBuf) -> Result<Self> {
        let file: File = Self::open_file(&path)?.into();
        let name = FileName::from_path(&path)?;
        let len = file.metadata().map_err(Error::FromFile)?.len();
        let index = Index::from_default(&path)?;

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

    pub fn check_data_consistency(&self) -> Result<()> {
        // @TODO implement
        Ok(())
    }

    pub async fn write(&mut self, mut record: Record) -> Result<()> {
        let key = record.key().to_vec();
        if self.index.contains_key(&key) {
            return Err(Error::AlreadyContainsSameKey);
        }
        let mut offset = await!(self.current_offset.lock());
        record.set_offset(*offset);
        let buf = record.to_raw();
        let bytes_written = await!(self.file.write_at(buf, *offset))?;
        let meta = RecordMetaData {
            key,
            header: record.header().clone(),
        };
        self.index.push(meta);
        *offset += bytes_written as u64;
        Ok(())
    }

    pub async fn read(&self, key: Vec<u8>) -> Result<Record> {
        let loc = self.lookup(&key)?;
        let buf = await!(self.file.read_at(loc.size as usize, loc.offset))?;
        let record = Record::from_raw(&buf).map_err(Error::RecordError)?;
        Ok(record)
    }

    fn lookup<K>(&self, key: &K) -> Result<Location>
    where
        K: AsRef<[u8]> + Ord,
    {
        let meta = self.index.get(key.as_ref()).ok_or(Error::NotFound)?;
        let offset = meta.header().blob_offset();
        Ok(Location::new(offset as u64, meta.header().full_len()))
    }

    pub fn file_size(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .map_err(Error::GetMetadataFailed)?
            .len())
    }

    pub fn records_count(&self) -> usize {
        self.index.len()
    }

    pub fn path(&self) -> PathBuf {
        self.name.as_path()
    }

    pub fn id(&self) -> usize {
        self.name.id
    }
}

#[derive(Debug)]
pub enum Error {
    OpenNew(io::Error, PathBuf),
    FromFile(io::Error),
    CloneFd(io::Error),
    NotFound,
    RecordFromRawFailed,
    PathWithoutFileName(PathBuf),
    NonUnicode(std::ffi::OsString),
    WrongFileNamePattern(PathBuf),
    IndexParseFailed(String),
    RecordError(record::Error),
    AlreadyContainsSameKey,
    DeserializationFailed(bincode::ErrorKind),
    WriteFailed(io::Error),
    CloneFailed(io::Error),
    ReadFailed(io::Error),
    CorruptedData,
    GetMetadataFailed(io::Error),
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

/// # Description
/// # Examples
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
