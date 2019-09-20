use crate::prelude::*;

use super::core::{Error, ErrorKind, FileName, Result};
use super::file::File;
use super::index::{ContainsKey, Count, Dump, Get, Index, Load, Push};

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

pub(crate) struct MetaLocation {
    pub(crate) len: usize,
    pub(crate) offset: u64,
}

impl MetaLocation {
    fn new(len: usize, offset: u64) -> Self {
        Self { len, offset }
    }
}

impl Header {
    fn serialized_size_default() -> bincode::Result<u64> {
        let header = Header::default();
        header.serialized_size()
    }

    #[inline]
    fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    #[inline]
    fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(buf)
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

    fn extract_matching_meta_locations(rec_hdrs: &[RecordHeader], key: &[u8]) -> Vec<MetaLocation> {
        rec_hdrs
            .iter()
            .filter(|h| h.has_key(key))
            .filter_map(|header| {
                Some(MetaLocation::new(
                    header.meta_len().try_into().ok()?,
                    header.blob_offset() + header.serialized_size().ok()?,
                ))
            })
            .collect()
    }

    pub async fn get_all_meta_locations(&self, key: &[u8]) -> Result<Vec<MetaLocation>> {
        Ok(match &self.inner {
            State::InMemory(bunch) => Self::extract_matching_meta_locations(bunch, key),
            State::OnDisk(file) => {
                let record_headers = Self::load(file).await?;
                Self::extract_matching_meta_locations(&record_headers, key)
            }
        })
    }

    async fn load(mut file: &File) -> Result<Vec<RecordHeader>> {
        debug!("seek to file start");
        file.seek(SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        debug!("read to end index");
        file.read_to_end(&mut buf).await?;
        Ok(if buf.is_empty() {
            debug!("empty index file");
            Vec::new()
        } else {
            debug!("deserialize buffer:{} to headers", buf.len());
            Self::deserialize_bunch(&buf)?
        })
    }

    pub(crate) async fn from_file(name: FileName) -> Result<Self> {
        debug!("opening index file");
        let fd = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(name.as_path())?;
        let mut file = File::from_std_file(fd)?;
        let mut buf = vec![0; Header::serialized_size_default()?.try_into()?];
        file.read_exact(&mut buf).await?;
        let header = Header::from_raw(&buf)?;
        let index = Self::load(&file).await?;
        Ok(Self {
            header,
            inner: State::InMemory(index),
            name,
        })
    }

    async fn binary_search(mut file: File, key: Vec<u8>) -> Result<RecordHeader> {
        let header: Header = Self::read_index_header(&mut file).await?;

        let mut size = header.records_count;
        if key.is_empty() {
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
                CmpOrdering::Greater => start,
                CmpOrdering::Equal => {
                    return Ok(mid_record_header);
                }
                CmpOrdering::Less => mid,
            };
            size -= half;
        }
        info!("record with key: {:?} not found", key);
        Err(ErrorKind::NotFound.into())
    }

    async fn read_at(file: &mut File, index: usize, header: Header) -> Result<RecordHeader> {
        let header_size = bincode::serialized_size(&header)?;
        let offset = header_size + (header.record_header_size * index) as u64;
        let buf = file.read_at(header.record_header_size, offset).await?;
        info!("file read at: {}, buf len: {}", offset, buf.len());
        Ok(deserialize(&buf)?)
    }

    async fn read_index_header(file: &mut File) -> Result<Header> {
        let header_size = Header::serialized_size_default()? as usize;
        debug!("header s: {}", header_size);
        let mut buf = vec![0; header_size];
        debug!("seek to file start");
        file.seek(SeekFrom::Start(0)).await?;
        debug!("read header");
        file.read(&mut buf).await?;
        debug!("deserialize header");
        Ok(deserialize(&buf)?)
    }

    fn serialize_bunch(bunch: &mut [RecordHeader]) -> Result<Vec<u8>> {
        let record_header = bunch.first().ok_or(ErrorKind::EmptyIndexBunch)?;
        let record_header_size = record_header.serialized_size()? as usize;
        debug!("record header serialized size: {}", record_header_size);
        bunch.sort_by_key(|h| h.key().to_vec());
        let header = Header {
            record_header_size,
            records_count: bunch.len(),
        };
        let mut buf = Vec::with_capacity(
            header.serialized_size()? as usize + bunch.len() * record_header_size,
        );
        serialize_into(&mut buf, &header)?;
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

    fn deserialize_bunch(buf: &[u8]) -> bincode::Result<Vec<RecordHeader>> {
        debug!("deserialize header from buf: {}", buf.len());
        let header: Header = deserialize(buf)?;
        debug!("header deserialized: {:?}", header);
        let header_size = header.serialized_size()? as usize;
        debug!("header serialized size: {}", header_size);
        (0..header.records_count).try_fold(Vec::new(), |mut record_headers, i| {
            let offset = header_size + i * header.record_header_size;
            debug!("deserialize record header at: {}", offset);
            deserialize(&buf[offset..]).map(|rh| {
                record_headers.push(rh);
                record_headers
            })
        })
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
        trace!("push header: {:?}", h);
        let fut = match &mut self.inner {
            State::InMemory(bunch) => {
                bunch.push(h);
                future::ok(()).boxed()
            }
            State::OnDisk(_) => future::err(
                ErrorKind::Index("Index is closed, push is unavalaible".to_string()).into(),
            )
            .boxed(),
        };
        debug!("future created");
        Push(fut)
    }

    fn get(&self, key: &[u8]) -> Get {
        match &self.inner {
            State::InMemory(bunch) => {
                let inner = if let Some(res) = bunch.iter().find(|h| h.key() == key).cloned() {
                    debug!("found in memory");
                    future::ok(res)
                } else {
                    future::err(ErrorKind::NotFound.into())
                }
                .boxed();
                Get { inner }
            }
            State::OnDisk(f) => {
                debug!("index state on disk");
                let cloned_key = key.to_vec();
                let file = f.clone();
                let inner = async { Self::binary_search(file, cloned_key).await }.boxed();
                Get { inner }
            }
        }
    }

    fn dump(&mut self) -> Dump {
        debug!("dump simple index");
        match &mut self.inner {
            State::InMemory(bunch) => {
                debug!("index state is InMemory");
                debug!("create new index file");
                let fd_res = fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(self.name.as_path());
                debug!("serialize index inner data");
                let buf = Self::serialize_bunch(bunch);
                let file_res = fd_res.and_then(File::from_std_file);
                match file_res {
                    Ok(mut file) => {
                        debug!("set index state to OnDisk");
                        let inner = State::OnDisk(file.clone());
                        self.inner = inner;
                        debug!("async write to file");
                        let fut = async move {
                            match buf {
                                Ok(buf) => {
                                    debug!("write all buffer");
                                    file.write_all(&buf).map_err(Error::new).await
                                }
                                Err(ref e) if e.is(&ErrorKind::EmptyIndexBunch) => Ok(()),
                                Err(e) => Err(Error::new(e)),
                            }
                        }
                            .boxed();
                        Dump(fut)
                    }
                    Err(e) => Dump(future::err(Error::new(e)).boxed()),
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
                    debug!("seek to file start");
                    file.seek(SeekFrom::Start(0)).await.map_err(Error::new)?;
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
