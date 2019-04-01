use futures::{
    future::Future,
    task::{Poll, Waker},
};
use std::{collections::BTreeMap, fs, io, os::unix::fs::FileExt, path::PathBuf, pin::Pin};

use crate::record::{self, Record};

type Result<T> = std::result::Result<T, Error>;

/// A `Blob` struct for performing of database,
#[derive(Debug, Default)]
pub struct Blob {
    index: BTreeMap<Vec<u8>, record::Header>,
    header: Header,
    file: Option<fs::File>,
    path: PathBuf,
    current_offset: u64,
}

/// # Description
/// # Examples
#[derive(Debug, Default)]
struct Header {
    magic_byte: u64,
    version: u32,
    key_size: u32,
    flags: u64,
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
    f: fs::File,
    k: Vec<u8>,
    offset: u64,
    size: u64,
}

impl Future for ReadFuture {
    type Output = Result<Record>;

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let mut buf = vec![0u8; self.size as usize];
        self.f.read_at(&mut buf, self.offset).unwrap();
        if let Some(rec) = Record::from_raw(&buf).take() {
            Poll::Ready(Ok(rec))
        } else {
            Poll::Ready(Err(Error::RecordFromRawFailed))
        }
    }
}

impl Blob {
    /// # Description
    /// Creates new blob file
    pub fn open_new<T>(path: T) -> Result<Self> where T: Into<PathBuf> {
        let path = path.into();
        Ok(Self {
            header: Default::default(),
            file: Some(
                fs::OpenOptions::new()
                    .create_new(true)
                    .append(true)
                    .read(true)
                    .open(&path)
                    .map_err(Error::OpenNew)?,
            ),
            path,
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
        let len = file.metadata().unwrap().len();
        let file = Some(file);
        // @TODO Scan existing file to create index
        Ok(Self {
            header: Default::default(),
            file,
            path,
            index: Default::default(),
            current_offset: len,
        })
    }

    pub fn write(&mut self, record: &mut Record) -> WriteFuture {
        record.header_mut().blob_offset = self.current_offset;
        self.index
            .insert(record.key().to_vec(), record.header().clone());
        let buf = record.to_raw();
        self.current_offset += buf.len() as u64;
        WriteFuture {
            file: self.file.as_ref().unwrap().try_clone().unwrap(),
            buf: Some(buf),
            current_offset: self.current_offset,
        }
    }

    pub fn read<K>(&self, key: &K) -> Option<ReadFuture>
    where
        K: AsRef<[u8]> + Ord,
    {
        let (offset, size) = self.locate(&key)?;
        Some(ReadFuture {
            f: self.file.as_ref()?.try_clone().ok()?,
            k: key.as_ref().to_vec(),
            offset,
            size,
        })
    }

    fn locate<K>(&self, key: &K) -> Option<(u64, u64)>
    where
        K: AsRef<[u8]> + Ord,
    {
        let header = self.index.get(key.as_ref())?;
        Some((header.blob_offset, header.size))
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
    pub fn size(&self) -> Result<usize> {
        // @TODO implement
        Ok(0usize)
    }

    /// # Description
    /// Returns number of records in current blob
    // @TODO more useful result
    pub fn count(&self) -> Result<usize> {
        // @TODO implement
        Ok(0usize)
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

#[derive(Debug)]
pub enum Error {
    OpenNew(io::Error),
    FromFile(io::Error),
    NotFound,
    RecordFromRawFailed,
}
