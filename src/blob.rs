use futures::{Async, Poll};
use std::{io, marker::PhantomData, path::Path};
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
    file: Option<File>,
}

pub struct BlobOpenFuture<K, P>
where
    K: Default + Send,
    P: AsRef<Path> + Send + 'static,
{
    f: OpenFuture<P>,
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
        }))
    }
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

impl<K> Blob<K>
where
    K: Default + Send,
{
    /// Creates new blob file or openes existing and truncates it
    pub fn open_new<P: AsRef<Path> + Send + 'static>(path: P) -> BlobOpenFuture<K, P> {
        let open = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path);
        BlobOpenFuture {
            f: open,
            _m: PhantomData,
        }
    }
    /// # Description
    /// Create new blob from file
    // @TODO more useful result
    pub fn from_file<P: AsRef<Path>>(_path: P) -> Result<Self, ()> {
        // @TODO implement
        Ok(Self::default())
    }

    /// # Description
    /// Writes given slice to file
    // @TODO more useful result
    pub fn write(&mut self, _record: Record<K>) -> Result<(), ()> {
        // @TODO implement
        Ok(())
    }

    /// # Description
    /// Reads record data, yields `Ok(Vec<u8>)` if read successful,
    /// otherwise - `Err`
    // @TODO more useful result
    pub fn read(&self, _key: &K) -> Result<Record<K>, ()> {
        // @TODO implement
        Ok(Record::new())
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
}

#[cfg(test)]
mod tests {
    use super::{Blob};

    #[test]
    fn test_blob_new() {
        let _b: Blob<u32> = Blob {
            header: Default::default(),
            records: Vec::new(),
            file: None,
        };
    }
}
