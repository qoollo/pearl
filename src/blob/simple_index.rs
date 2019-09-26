use super::prelude::*;
use crate::prelude::*;

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
pub(crate) enum State {
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

    pub(crate) fn name(&self) -> &FileName {
        &self.name
    }

    pub(crate) async fn get_all_meta_locations(&self, key: &[u8]) -> Result<Vec<Location>> {
        Ok(match &self.inner {
            State::InMemory(bunch) => Self::extract_matching_meta_locations(bunch, key),
            State::OnDisk(file) => {
                let record_headers = Self::load(file).await?;
                Self::extract_matching_meta_locations(&record_headers, key)
            }
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

    pub(crate) fn on_disk(&self) -> bool {
        match self.inner {
            State::OnDisk(_) => true,
            _ => false,
        }
    }

    pub(crate) fn get_entry<'a, 'b: 'a>(&'b self, key: &'a [u8], file: &'b File) -> Entries<'a> {
        info!("create iterator");
        Entries::new(&self.inner, key, file)
    }

    #[inline]
    fn extract_matching_meta_locations(rec_hdrs: &[RecordHeader], key: &[u8]) -> Vec<Location> {
        rec_hdrs
            .iter()
            .filter(|h| h.has_key(key))
            .filter_map(Self::try_create_location)
            .collect()
    }

    fn try_create_location(h: &RecordHeader) -> Option<Location> {
        Some(Location::new(
            h.blob_offset() + h.serialized_size().ok()?,
            h.meta_size().try_into().ok()?,
        ))
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
        Err(ErrorKind::RecordNotFound.into())
    }

    async fn read_at(file: &mut File, index: usize, header: Header) -> Result<RecordHeader> {
        let header_size = bincode::serialized_size(&header)?;
        let offset = header_size + (header.record_header_size * index) as u64;
        let buf = file.read_at(header.record_header_size, offset).await?;
        debug!("file read at: {}, buf len: {}", offset, buf.len());
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
            trace!("deserialize record header at: {}", offset);
            deserialize(&buf[offset..]).map(|rh| {
                record_headers.push(rh);
                record_headers
            })
        })
    }

    async fn check_result(res: Result<RecordHeader>) -> Result<bool> {
        match res {
            Ok(_) => Ok(true),
            Err(ref e) if e.is(&ErrorKind::RecordNotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn get_from_bunch(
        bunch: &[RecordHeader],
        key: &[u8],
    ) -> impl Future<Output = Result<RecordHeader>> {
        future::ready(
            bunch
                .iter()
                .find(|h| h.key() == key)
                .cloned()
                .ok_or_else(|| ErrorKind::RecordNotFound.into()),
        )
    }

    fn dump_in_memory(&mut self, buf: Vec<u8>) -> Dump {
        let fd_res = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.name.as_path())
            .expect("open new index file");
        let mut file = File::from_std_file(fd_res).expect("convert std file to own format");
        let inner = State::OnDisk(file.clone());
        self.inner = inner;
        let fut = async move { file.write_all(&buf).await.map_err(Into::into) }.boxed();
        Dump(fut)
    }

    async fn load_in_memory(&mut self, mut file: File) -> Result<()> {
        let mut buf = Vec::new();
        debug!("seek to file start");
        file.seek(SeekFrom::Start(0)).await.map_err(Error::new)?;
        file.read_to_end(&mut buf).map_err(Error::new).await?;
        let bunch = Self::deserialize_bunch(&buf)?;
        self.inner = State::InMemory(bunch);
        Ok(())
    }

    fn count_inner(self) -> usize {
        if let State::InMemory(index) = self.inner {
            index.len()
        } else {
            0
        }
    }
}

impl Index for SimpleIndex {
    fn contains_key(&self, key: &[u8]) -> ContainsKey {
        ContainsKey(self.get(key).then(Self::check_result).boxed())
    }

    fn push(&mut self, h: RecordHeader) -> Push {
        trace!("push header: {:#?}", h);
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
                let inner = Self::get_from_bunch(bunch, key).boxed();
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
        if let State::InMemory(bunch) = &mut self.inner {
            let buf = Self::serialize_bunch(bunch);
            match buf {
                Ok(buf) => self.dump_in_memory(buf),
                Err(ref e) if e.is(&ErrorKind::EmptyIndexBunch) => Dump(future::ok(()).boxed()),
                Err(e) => Dump(future::err(e).boxed()),
            }
        } else {
            Dump(future::ok(()).boxed())
        }
    }

    fn load<'a>(&'a mut self) -> Load {
        match &self.inner {
            State::InMemory(_) => Load(future::ok(()).boxed()),
            State::OnDisk(file) => {
                let file = file.clone();
                Load(self.load_in_memory(file).boxed())
            }
        }
    }

    fn count(&self) -> Count {
        Count(match &self.inner {
            State::InMemory(bunch) => future::ok(bunch.len()).boxed(),
            State::OnDisk(_) => Self::from_file(self.name.clone())
                .map_ok(Self::count_inner)
                .boxed(),
        })
    }
}
