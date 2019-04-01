use futures::{
    future::Future,
    task::{Poll, Waker},
};
use std::{
    collections::BTreeMap,
    fs, io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    pin::Pin,
};

use crate::record::{self, Record};

const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;

type Result<T> = std::result::Result<T, Error>;

/// A `Blob` struct for performing of database,
#[derive(Debug)]
pub struct Blob {
    index: BTreeMap<Vec<u8>, record::Header>,
    header: Header,
    file: fs::File,
    name: FileName,
    current_offset: u64,
}

#[derive(Debug)]
pub struct WriteFuture {
    file: fs::File,
    buf: Option<Vec<u8>>,
    current_offset: u64,
}

impl Future for WriteFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let buf = self.buf.take().unwrap();
        self.file.write_at(&buf, self.current_offset).unwrap();
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct ReadFuture {
    file: fs::File,
    key: Vec<u8>,
    loc: Location,
}

impl Future for ReadFuture {
    type Output = Result<Record>;

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let mut buf = vec![0u8; self.loc.size as usize];
        self.file.read_at(&mut buf, self.loc.offset).unwrap();
        Poll::Ready(Record::from_raw(&buf).map_err(Error::RecordError))
    }
}

impl Blob {
    /// # Description
    /// Creates new blob file
    pub fn open_new(name: FileName) -> Result<Self> {
        Ok(Self {
            header: Header::new(),
            file: fs::OpenOptions::new()
                .create_new(true)
                .append(true)
                .read(true)
                .open(name.as_path())
                .map_err(Error::OpenNew)?,
            name,
            index: BTreeMap::new(),
            current_offset: 0,
        })
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub fn from_file(path: PathBuf) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .create(false)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(Error::FromFile)?;
        let name = FileName::from_path(&path)?;
        let len = file.metadata().map_err(Error::FromFile)?.len();
        // @TODO Scan existing file to create index
        Ok(Self {
            header: Header::new(),
            file,
            name,
            index: Default::default(),
            current_offset: len,
        })
    }

    pub fn write(&mut self, record: &mut Record) -> Result<WriteFuture> {
        record.set_blob_offset(self.current_offset);
        self.index
            .insert(record.key().to_vec(), record.header().clone());
        let buf = record.to_raw();
        self.current_offset += buf.len() as u64;
        Ok(WriteFuture {
            file: self.file.try_clone().map_err(Error::CloneFd)?,
            buf: Some(buf),
            current_offset: self.current_offset,
        })
    }

    pub fn read<K>(&self, key: &K) -> Result<ReadFuture>
    where
        K: AsRef<[u8]> + Ord,
    {
        Ok(ReadFuture {
            file: self.file.try_clone().map_err(Error::CloneFd)?,
            key: key.as_ref().to_vec(),
            loc: self.locate(&key)?,
        })
    }

    fn locate<K>(&self, key: &K) -> Result<Location>
    where
        K: AsRef<[u8]> + Ord,
    {
        let header = self.index.get(key.as_ref()).ok_or(Error::NotFound)?;
        Ok(Location::new(header.blob_offset(), header.full_len()))
    }

    /// # Description
    // @TODO more useful result
    // pub fn flush(&mut self) -> Result<(), Error> {
    // @TODO implement
    //     Ok(())
    // }

    /// # Description
    /// Returns size of file in bytes
    // @TODO more useful result
    pub fn file_size(&self) -> Result<u64> {
        // @TODO implement
        Ok(0)
    }

    /// # Description
    /// Returns number of records in current blob
    // @TODO more useful result
    pub fn records_count(&self) -> Result<usize> {
        // @TODO implement
        Ok(0usize)
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
    OpenNew(io::Error),
    FromFile(io::Error),
    CloneFd(io::Error),
    NotFound,
    RecordFromRawFailed,
    PathWithoutFileName(PathBuf),
    NonUnicode(std::ffi::OsString),
    FileNamePattern(String),
    IndexParseFailed(String),
    RecordError(record::Error),
}

#[derive(Debug)]
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
        let file_name = path
            .file_name()
            .ok_or_else(|| Error::PathWithoutFileName(path.to_owned()))?;
        let fname_str = file_name
            .to_str()
            .ok_or_else(|| Error::NonUnicode(file_name.to_owned()))?;
        let mut parts = fname_str.split('.');
        let name_prefix = parts.next().unwrap().to_owned();
        let id = parts.next().unwrap().parse().unwrap();
        let extension = path.extension().unwrap().to_str().unwrap().to_owned();
        let dir = path.parent().unwrap().to_owned();
        Ok(Self {
            name_prefix,
            id,
            extension,
            dir,
        })
    }

    pub fn as_path(&self) -> PathBuf {
        self.dir.join(self.name_to_string())
    }

    fn name_to_string(&self) -> String {
        format!("{}.{}.{}", self.name_prefix, self.id, self.extension)
    }
}

/// # Description
/// # Examples
#[derive(Debug)]
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
