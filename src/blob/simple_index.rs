use crate::prelude::*;

use super::core::{Error, ErrorKind, FileName, Result};
use super::file::File;
use super::index::{ContainsKey, Count, Dump, Get, Index, Load, Next, Push};

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
    fn serialized_size() -> bincode::Result<u64> {
        let header = Header::default();
        bincode::serialized_size(&header)
    }

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

    async fn load(mut file: File) -> Result<Vec<RecordHeader>> {
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
        let mut buf = vec![0; Header::serialized_size()?.try_into().map_err(Error::new)?];
        file.read_exact(&mut buf).await?;
        let header = Header::from_raw(&buf)?;
        let index = Self::load(file).await?;
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
        let header_size = Header::serialized_size()? as usize;
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
        debug!("get record header size");
        let record_header_size = bunch
            .first()
            .ok_or_else(|| Error::from(ErrorKind::EmptyIndexBunch))?
            .serialized_size()? as usize;
        debug!("sort bunch");
        bunch.sort_by_key(|h| h.key().to_vec());
        let header = Header {
            record_header_size,
            records_count: bunch.len(),
        };
        let mut buf = Vec::with_capacity(
            Header::serialized_size()? as usize + bunch.len() * record_header_size,
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
        trace!("header: {:?}", header);
        let header_size = Header::serialized_size()? as usize;
        (0..header.records_count).try_fold(Vec::new(), |mut records, i| {
            let offset = header_size + i * header.record_header_size;
            deserialize(&buf[offset..]).map(|r| {
                records.push(r);
                records
            })
        })
    }

    pub fn on_disk(&self) -> bool {
        match self.inner {
            State::OnDisk(_) => true,
            _ => false,
        }
    }

    async fn next_from(
        mut file: File,
        key: Vec<u8>,
        offset: usize,
    ) -> Result<(usize, RecordHeader)> {
        info!("next_from: {}", offset);
        let mut new_offset = offset;
        let header: Header = Self::read_index_header(&mut file).await?;
        loop {
            let record_header = Self::read_at(&mut file, new_offset, header).await?;
            new_offset += 1;
            info!("new_offset: {}", new_offset);
            if record_header.key() == key.as_slice() {
                info!("key matched");
                return Ok((new_offset, record_header));
            }
        }
    }
}

impl Index for SimpleIndex {
    fn next_item(&self, key: &[u8], file_index: Option<usize>, vec_index: Option<usize>) -> Next {
        info!("next_item");
        match &self.inner {
            State::InMemory(bunch) => {
                let mut id = None;
                let start = if let Some(i) = vec_index { i + 1 } else { 0 };
                info!("start index: {}", start);
                let inner = if let Some(res) = bunch[start..]
                    .iter()
                    .enumerate()
                    .find_map(|(i, h)| {
                        if h.key() == key {
                            id = Some(start + i);
                            Some(h)
                        } else {
                            None
                        }
                    })
                    .cloned()
                {
                    debug!("found in memory");
                    future::ok((0, res))
                } else {
                    future::err(ErrorKind::NotFound.into())
                }
                .boxed();
                Next {
                    inner,
                    vec_index: id,
                }
            }
            State::OnDisk(f) => {
                info!("next item index on disk");
                let cloned_key = key.to_vec();
                let file = f.clone();
                let inner = Self::next_from(file, cloned_key, file_index.unwrap_or(0)).boxed();
                Next {
                    inner,
                    vec_index: None,
                }
            }
        }
    }

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
