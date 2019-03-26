use futures::{
    future::Future,
    task::{Poll, Waker},
};
use serde::{Deserialize, Serialize};
use std::{fs, io, os::unix::fs::FileExt, path::PathBuf, pin::Pin};
// use std::fmt::Debug;

use crate::record::Record;

/// A `Blob` struct for performing of database,
#[derive(Debug, Default)]
pub struct Blob<K>
where
    K: AsRef<[u8]>,
{
    header: Header,
    records: Vec<K>, // @TODO needs verification, created to yield generic K up
    file: Option<fs::File>,
    path: PathBuf,
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
    f: fs::File,
    b: Option<Vec<u8>>,
}

impl Future for WriteFuture {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let buf = self.b.take().unwrap();
        self.f.write_all_at(&buf, 0).unwrap();
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct ReadFuture<K> {
    f: fs::File,
    k: K,
}

impl<K> Future for ReadFuture<K>
where
    K: for<'de> Deserialize<'de> + Serialize + Default + AsRef<[u8]>,
{
    type Output = Result<Record<Vec<u8>>, Error>;

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let header_size = Record::<Vec<_>>::header_size();
        let mut raw_header = vec![0u8; header_size];
        self.f.read_at(&mut raw_header, 0).unwrap();
        let mut rec = Record::with_raw_header(&raw_header);
        let key_len = rec.header_key_len();
        let mut raw_key = vec![0u8; key_len as usize];
        self.f.read_at(&mut raw_key, header_size as u64).unwrap();
        let data_len = rec.header_data_len();
        let mut data = vec![0u8; data_len as usize];
        self.f
            .read_at(&mut data, header_size as u64 + key_len)
            .unwrap();
        rec.set_body(raw_key, data);
        Poll::Ready(Ok(rec))
    }
}

impl<K> Blob<K>
where
    K: AsRef<[u8]>,
{
    /// # Description
    /// Creates new blob file
    pub fn open_new(path: PathBuf) -> Result<Self, Error> {
        Ok(Self {
            header: Default::default(),
            records: Vec::new(),
            file: Some(
                fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .read(true)
                    .open(&path)
                    .map_err(Error::OpenNew)?,
            ),
            path,
        })
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub fn from_file(path: PathBuf) -> Result<Self, Error> {
        Ok(Self {
            header: Default::default(),
            records: Vec::new(),
            file: Some(
                fs::OpenOptions::new()
                    .create(false)
                    .read(true)
                    .open(&path)
                    .map_err(Error::FromFile)?,
            ),
            path,
        })
    }

    pub fn write(&self, record: Record<K>) -> WriteFuture
    where
        K: Serialize + Default,
    {
        let buf = record.to_raw();
        WriteFuture {
            f: self.file.as_ref().unwrap().try_clone().unwrap(),
            b: Some(buf),
        }
    }

    pub fn read(&self, key: K) -> ReadFuture<K> {
        ReadFuture {
            f: self.file.as_ref().unwrap().try_clone().unwrap(),
            k: key,
        }
    }

    pub fn contains(&self, _key: &K) -> bool {
        false
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
    pub fn size(&self) -> Result<usize, ()> {
        // @TODO implement
        Ok(0usize)
    }

    /// # Description
    /// Returns number of records in current blob
    // @TODO more useful result
    pub fn count(&self) -> Result<usize, ()> {
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
}
