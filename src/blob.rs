// use bincode::{deserialize, serialize};
// use serde::{Deserialize, Serialize};
// use std::fmt::Debug;

use std::{fs, io, path::PathBuf};

// use crate::record::Record;

/// A `Blob` struct for performing of database,
#[derive(Debug, Default)]
pub struct Blob<K> {
    header: Header,
    records: Vec<K>, // @TODO needs verification, created to yield generic T up
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

impl<K> Blob<K> {
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

#[derive(Debug)]
pub enum Error {
    OpenNew(io::Error),
    FromFile(io::Error),
}
