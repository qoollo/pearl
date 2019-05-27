use bincode::{deserialize, serialize};
use futures::{future, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Future, FutureExt, TryFutureExt};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering::{Greater, Equal};
use std::fs;
use std::io::SeekFrom;
use std::pin::Pin;

use super::core::{Error, File, FileName, Result};
use super::index::{ContainsKey, Count, Dump, Get, Index, Push};
use crate::record::Header as RecordHeader;

#[derive(Debug)]
pub(crate) struct SimpleIndex {
    header: Header,
    inner: State,
    name: FileName,
}

#[derive(Debug, Deserialize, Default, Serialize, Copy, Clone)]
struct Header {
    records_count: usize,
    record_header_size: usize,
}

impl Header {
    fn serialized_size() -> u64 {
        let header = Header::default();
        bincode::serialized_size(&header).unwrap()
    }
}

#[derive(Debug, Clone)]
enum State {
    InMemory(Vec<RecordHeader>),
    OnDisk(File),
}

impl SimpleIndex {
    pub(crate) async fn load(mut file: File) -> Result<Vec<RecordHeader>> {
        debug!("seek to file start");
        await!(file.seek(SeekFrom::Start(0)))?;
        let mut buf = Vec::new();
        debug!("read to end index");
        await!(file.read_to_end(&mut buf))?;
        let res = deserialize(&buf).map_err(Error::SerDe);
        debug!("index deserialized");
        res
    }
}

impl SimpleIndex {
    pub(crate) fn new(name: FileName) -> Self {
        Self {
            header: Header {
                records_count: 0,
                record_header_size: 0,
            },
            inner: State::InMemory(Vec::new()),
            name,
        }
    }

    pub(crate) async fn from_file(name: FileName) -> Result<Self> {
        debug!("opening index file");
        let fd = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(name.as_path())?;
        let file = File::from_std_file(fd)?;
        await!(Self::load(file)
            .and_then(|index| {
                future::ok(Self {
                    header: Header {
                        records_count: index.len(),
                        record_header_size: index[0].serialized_size().unwrap() as usize,
                    },
                    inner: State::InMemory(index),
                    name,
                })
            })
            .boxed())
    }

    async fn binary_search(mut file: File, key: Vec<u8>) -> Result<RecordHeader> {
        let header: Header = await!(Self::read_index_header(&mut file))?;

        let mut size = header.records_count;
        if size == 0 {
            unimplemented!();
        }

        let mut start = 0;

        while size > 1 {
            let half = size / 2;
            let mid = start + half;
            let mid_record_header = await!(Self::read_at(&mut file, mid, header)).unwrap();
            let cmp = mid_record_header.key().cmp(&key);
            start = if cmp == Greater { start } else { mid };

            size -= half;
        }
        let record_header = await!(Self::read_at(&mut file, start, header)).unwrap();
        let cmp = record_header.key().cmp(&key);
        if cmp == Equal {Ok(record_header)} else {Err(Error::NotFound)}
    }

    async fn read_at(file: &mut File, index: usize, header: Header) -> Result<RecordHeader> {
        let header_size = bincode::serialized_size(&header).unwrap();
        let offset = header_size + (header.record_header_size * index) as u64;
        let buf = await!(file.read_at(header.record_header_size, offset)).unwrap();
        let record_header: RecordHeader = deserialize(&buf).unwrap();
        Ok(record_header)
    }

    async fn read_index_header(file: &mut File) -> Result<Header> {
        let header_size = Header::serialized_size() as usize;
        let mut buf = vec![0; header_size];
        debug!("seek to file start");
        await!(file.seek(SeekFrom::Start(0)))?;
        await!(file.read(&mut buf)).unwrap();
        let header = deserialize(&buf).unwrap();
        Ok(header)
    }
}

impl Index for SimpleIndex {
    fn contains_key(&self, key: &[u8]) -> ContainsKey {
        ContainsKey(
            self.get(key)
                .then(|res| match res {
                    Ok(_) => future::ok(true),
                    Err(Error::NotFound) => future::ok(false),
                    Err(e) => {
                        error!("{:?}", e);
                        future::err(e)
                    }
                })
                .boxed(),
        )
    }

    fn push(&mut self, h: RecordHeader) -> Push {
        match &mut self.inner {
            State::InMemory(bunch) => {
                bunch.push(h);
                Push(future::ok(()).boxed())
            }
            State::OnDisk(_) => unimplemented!(),
        }
    }

    fn get(&self, key: &[u8]) -> Get {
        match &self.inner {
            State::InMemory(bunch) => Get(if let Some(res) =
                bunch.iter().find(|h| h.key() == key).cloned()
            {
                debug!("found in memory");
                future::ok(res)
            } else {
                future::err(Error::NotFound)
            }
            .boxed()),
            State::OnDisk(f) => {
                debug!("index state on disk");
                let cloned_key = key.to_vec();
                Get(Self::binary_search(f.clone(), cloned_key).boxed())
            }
        }
    }

    fn dump(&mut self) -> Dump {
        match &mut self.inner {
            State::InMemory(bunch) => {
                let fd_res = fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(self.name.as_path())
                    .map_err(Error::IO);
                bunch.sort_by_key(|h| h.key().to_vec());
                let buf_res = serialize(&bunch);
                let file_res = fd_res.and_then(File::from_std_file);
                let mut file = match file_res {
                    Ok(file) => file,
                    Err(e) => return Dump(future::err(e).boxed()),
                };
                let inner = State::OnDisk(file.clone());
                self.inner = inner;
                let fut = match buf_res.map_err(Error::SerDe) {
                    Ok(buf) => {
                        async move { await!(file.write_all(&buf).map_err(Error::IO)) }.boxed()
                    }
                    Err(e) => future::err(e).boxed(),
                };
                Dump(fut)
            }
            State::OnDisk(_) => unimplemented!(),
        }
    }

    fn count(&self) -> Count {
        Count(match &self.inner {
            State::InMemory(bunch) => future::ok(bunch.len()).boxed(),
            State::OnDisk(_) => Self::from_file(self.name.clone())
                .map_ok(|st| {
                    if let State::InMemory(index) = st.inner {
                        index.len()
                    } else {
                        0
                    }
                })
                .boxed(),
        })
    }
}
