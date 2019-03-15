use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use futures::{Async, Poll};
use std::{
    io,
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tokio::{
    fs::file::{File, OpenFuture, OpenOptions},
    prelude::Future,
};

use crate::record::Record;

/// A `Blob` struct for performing of database,
#[derive(Debug, Default)]
pub struct Blob<K>
where
    K: Send,
{
    header: Header,
    records: Vec<K>, // @TODO needs verification, created to yield generic T up
    path: PathBuf,
    file: Option<File>,
}

pub struct BlobOpenFuture<K, P>
where
    K: Default + Send,
    P: AsRef<Path> + Send + 'static,
{
    f: OpenFuture<P>,
    p: PathBuf,
    _m: PhantomData<K>,
}

impl<K, P> Future for BlobOpenFuture<K, P>
where
    K: Default + Send,
    P: AsRef<Path> + Send + 'static,
{
    type Item = Blob<K>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let file = try_ready!(self.f.poll());
        Ok(Async::Ready(Blob {
            header: Default::default(),
            records: Vec::new(),
            file: Some(file),
            path: self.p.clone(),
        }))
    }
}

// pub struct BlobWriteFuture {

// }

// impl Future for BlobWriteFuture
// {
//     type Item = ();
//     type Error = io::Error;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//     }
// }

/// # Description
/// # Examples
#[derive(Debug, Default)]
struct Header {
    magic_byte: u64,
    version: u32,
    key_size: u32,
    flags: u64,
}

impl<K> Blob<K>
where
    K: for<'de> Deserialize<'de> + Serialize + Default + Send + PartialEq + Debug,
{
    /// Creates new blob file or openes existing and truncates it
    pub fn open_new<P: AsRef<Path> + Send + 'static>(path: P) -> BlobOpenFuture<K, P> {
        let p = path.as_ref().to_owned();
        let open = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path);
        BlobOpenFuture {
            f: open,
            p,
            _m: PhantomData,
        }
    }
    /// # Description
    /// Create new blob from file
    // @TODO more useful result
    pub fn from_file<P: AsRef<Path> + Send + 'static>(path: P) -> BlobOpenFuture<K, P> {
        let p = path.as_ref().to_owned();
        let open = OpenOptions::new().read(true).write(true).open(path);
        BlobOpenFuture {
            f: open,
            p,
            _m: PhantomData,
        }
    }

    /// # Description
    /// Writes given record to file
    // @TODO more useful result
    pub fn write(&mut self, record: Record<K>) -> Result<(), ()> {
        let encoded = serialize(&record).map_err(|e| error!("{}", e))?;
        let temp_file = self.file.take().unwrap();
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        println!("write");
        let (temp_file, _) = runtime
            .block_on(tokio::io::write_all(temp_file, encoded))
            .unwrap();
        let (temp_file, _) = runtime
            .block_on(temp_file.seek(io::SeekFrom::Start(0)))
            .unwrap();
        self.file = Some(temp_file);
        Ok(())
    }

    /// # Description
    /// Reads record data, yields `Ok(Vec<u8>)` if read successful,
    /// otherwise - `Err`
    // @TODO more useful result
    pub fn read(&mut self, key: &K) -> Result<Record<K>, ()> {
        use tokio::io::AsyncRead;
        let (reader, writer) = self.file.take().unwrap().split();
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        // @TODO remove block on
        println!("read");
        let (reader, buf) = runtime
            .block_on(tokio::io::read_to_end(reader, Vec::new()))
            .unwrap();
        self.file = Some(reader.unsplit(writer));

        let record: Record<K> = deserialize(&buf).unwrap();
        if record.key() != key {
            error!("no matching key");
        }
        Ok(record)
    }

    /// # Description
    // @TODO more useful result
    pub fn flush(&mut self) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }

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

#[cfg(test)]
mod tests {
    use super::Blob;

    #[test]
    fn test_blob_new() {
        let _b: Blob<u32> = Blob {
            header: Default::default(),
            records: Vec::new(),
            file: None,
            path: Default::default(),
        };
    }
}
