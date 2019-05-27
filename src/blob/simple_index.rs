use bincode::{deserialize, serialize};
use futures::{future, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Future, FutureExt, TryFutureExt};
use serde::Deserialize;
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

#[derive(Debug, Deserialize)]
struct Header {
    records_count: usize,
    header_size: usize,
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
                header_size: 0,
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
                        header_size: index[0].serialized_size().unwrap() as usize,
                    },
                    inner: State::InMemory(index),
                    name,
                })
            })
            .boxed())
    }

    async fn binary_search(mut file: File, key: Vec<u8>) -> Result<RecordHeader> {
        let recs_num = await!(Self::read_index_header(&file))?;
        let usize_len = std::mem::size_of::<usize>();
        debug!("seek to file start");
        await!(file.seek(SeekFrom::Start(0)))?;
        debug!("read to end");
        let mut num_buf = vec![0; usize_len];
        dbg!(await!(file.read(&mut num_buf)).unwrap());
        debug!("deserialize number of records");
        let num: usize = deserialize(&num_buf).unwrap();
        debug!("records num: {}", num);
        let mut buf = Vec::new();
        await!(file.read_to_end(&mut buf)).unwrap();
        debug!("read buf len: {}", buf.len());
        let raw_headers: Box<[RecordHeader]> = deserialize(&buf).unwrap();
        dbg!(raw_headers.len());
        unimplemented!()
    }

    async fn read_index_header(mut file: &File) -> Result<Header> {
        unimplemented!()
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
