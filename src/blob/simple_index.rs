use bincode::{deserialize, serialize, serialize_into};
use futures::{future, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, FutureExt, TryFutureExt};
use std::{cmp::Ordering, fs, io::SeekFrom};

use super::core::{Error, ErrorKind, FileName, Result};
use super::file::File;
use super::index::{ContainsKey, Count, Dump, Get, Index, Load, Push};

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
    fn serialized_size() -> Result<u64> {
        let header = Header::default();
        bincode::serialized_size(&header).map_err(Error::new)
    }
}

#[derive(Debug, Clone)]
enum State {
    InMemory(Vec<RecordHeader>),
    OnDisk(File),
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

    pub(crate) async fn load(mut file: File) -> Result<Vec<RecordHeader>> {
        debug!("seek to file start");
        file.seek(SeekFrom::Start(0)).await.map_err(Error::new)?;
        let mut buf = Vec::new();
        debug!("read to end index");
        file.read_to_end(&mut buf).await.map_err(Error::new)?;
        deserialize(&buf).map_err(Error::new)
    }

    pub(crate) async fn from_file(name: FileName) -> Result<Self> {
        debug!("opening index file");
        let fd = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(name.as_path())
            .map_err(Error::new)?;
        let file = File::from_std_file(fd)?;
        let index = Self::load(file).await?;
        let record_header_size = index
            .first()
            .ok_or_else(|| Error::from(ErrorKind::EmptyIndexFile))?
            .serialized_size()
            .map_err(Error::new)? as usize;
        let header = Header {
            records_count: index.len(),
            record_header_size,
        };
        Ok(Self {
            header,
            inner: State::InMemory(index),
            name,
        })
    }

    async fn binary_search(mut file: File, key: Vec<u8>) -> Result<RecordHeader> {
        let header: Header = Self::read_index_header(&mut file).await?;

        let mut size = header.records_count;
        if size == 0 {
            error!("empty key was provided");
        }

        let mut start = 0;

        while size > 1 {
            let half = size / 2;
            let mid = start + half;
            let mid_record_header = Self::read_at(&mut file, mid, header).await?;
            let cmp = mid_record_header.key().cmp(&key);
            debug!("mid read: {:?}", mid_record_header.key());
            start = match cmp {
                Ordering::Greater => start,
                Ordering::Equal => {
                    return Ok(mid_record_header);
                }
                Ordering::Less => mid,
            };
            size -= half;
        }
        debug!("binary search not found");
        Err(ErrorKind::NotFound.into())
    }

    async fn read_at(file: &mut File, index: usize, header: Header) -> Result<RecordHeader> {
        let header_size = bincode::serialized_size(&header).map_err(Error::new)?;
        let offset = header_size + (header.record_header_size * index) as u64;
        let buf = file.read_at(header.record_header_size, offset).await?;
        deserialize(&buf).map_err(Error::new)
    }

    async fn read_index_header(file: &mut File) -> Result<Header> {
        let header_size = Header::serialized_size()? as usize;
        debug!("header s: {}", header_size);
        let mut buf = vec![0; header_size];
        debug!("seek to file start");
        file.seek(SeekFrom::Start(0)).await.map_err(Error::new)?;
        file.read(&mut buf).map_err(Error::new).await?;
        deserialize(&buf).map_err(Error::new)
    }

    fn serialize_bunch(bunch: &mut [RecordHeader]) -> Result<Vec<u8>> {
        let record_header_size = bunch
            .first()
            .ok_or_else(|| Error::from(ErrorKind::NotFound))?
            .serialized_size()
            .map_err(Error::new)? as usize;
        bunch.sort_by_key(|h| h.key().to_vec());
        let header = Header {
            record_header_size,
            records_count: bunch.len(),
        };
        let mut buf = Vec::with_capacity(
            Header::serialized_size()? as usize + bunch.len() * record_header_size,
        );
        serialize_into(&mut buf, &header).map_err(Error::new)?;
        bunch
            .iter()
            .filter_map(|h| {
                debug!("write key: {:?}", h.key());
                serialize(&h).ok()
            })
            .fold(&mut buf, |acc, h_buf| {
                acc.extend_from_slice(&h_buf);
                acc
            });
        Ok(buf)
    }

    fn deserialize_bunch(buf: &[u8]) -> Result<Vec<RecordHeader>> {
        let header: Header = deserialize(buf).map_err(Error::new)?;
        let header_size = Header::serialized_size()? as usize;
        Ok((0..header.records_count)
            .map(|i| {
                let offset = header_size + i * header.record_header_size;
                deserialize(&buf[offset..]).unwrap()
            })
            .collect())
    }

    pub fn on_disk(&self) -> bool {
        match self.inner {
            State::OnDisk(_) => true,
            _ => false,
        }
    }
}

impl Index for SimpleIndex {
    fn contains_key(&self, key: &[u8]) -> ContainsKey {
        ContainsKey(
            self.get(key)
                .then(|res| match res {
                    Ok(_) => future::ok(true),
                    Err(ref e) if e.is(&ErrorKind::NotFound) => future::ok(false),
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
            State::OnDisk(_) => Push(
                future::err(
                    ErrorKind::Index("Index is closed and dumped, push is unavalaible".to_string())
                        .into(),
                )
                .boxed(),
            ),
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
                future::err(ErrorKind::NotFound.into())
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
                    .map_err(Error::new);
                let buf = Self::serialize_bunch(bunch);
                let file_res = fd_res.and_then(File::from_std_file);
                match file_res {
                    Ok(mut file) => {
                        let inner = State::OnDisk(file.clone());
                        self.inner = inner;
                        let fut = async move {
                            let buf = buf?;
                            file.write_all(&buf).map_err(Error::new).await
                        }
                            .boxed();
                        Dump(fut)
                    }
                    Err(e) => Dump(future::err(e).boxed()),
                }
            }
            State::OnDisk(_) => Dump(
                future::err(ErrorKind::Index("Index is dumped already".to_string()).into()).boxed(),
            ),
        }
    }

    fn load<'a>(&'a mut self) -> Load {
        match &mut self.inner {
            State::InMemory(_) => Load(
                future::err(ErrorKind::Index("Index is loaded already".to_string()).into()).boxed(),
            ),
            State::OnDisk(file) => {
                let mut buf = Vec::new();
                let mut file = file.clone();
                let task = async move {
                    file.read_to_end(&mut buf).map_err(Error::new).await?;
                    let bunch = Self::deserialize_bunch(&buf)?;
                    self.inner = State::InMemory(bunch);
                    Ok(())
                }
                    .boxed();
                Load(task)
            }
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
