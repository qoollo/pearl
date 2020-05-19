use super::prelude::*;

#[derive(Debug)]
pub(crate) struct Simple {
    header: Header,
    filter: Bloom,
    inner: State,
    name: FileName,
}

#[derive(Debug, Deserialize, Default, Serialize, Clone)]
struct Header {
    records_count: usize,
    record_header_size: usize,
    filter_buf_size: usize,
}

impl Header {
    fn serialized_size_default() -> bincode::Result<u64> {
        let header = Self::default();
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

impl Simple {
    pub(crate) fn new(filter_config: &Config, name: FileName) -> Self {
        let filter = Bloom::new(filter_config);
        Self {
            header: Header {
                records_count: 0,
                record_header_size: 0,
                filter_buf_size: 0,
            },
            filter,
            inner: State::InMemory(Vec::new()),
            name,
        }
    }

    pub fn check_bloom_key(&self, key: &[u8]) -> bool {
        self.filter.contains(key)
    }

    pub(crate) const fn name(&self) -> &FileName {
        &self.name
    }

    pub(crate) async fn get_all_meta_locations(&self, key: &[u8]) -> Result<Vec<Location>> {
        Ok(match &self.inner {
            State::InMemory(bunch) => Self::extract_matching_meta_locations(bunch, key),
            State::OnDisk(file) => {
                let record_headers = Self::load_records(file).await?;
                Self::extract_matching_meta_locations(&record_headers, key)
            }
        })
    }

    pub(crate) async fn from_file(name: FileName) -> Result<Self> {
        debug!("open index file");
        let fd = TokioOpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(name.to_path())
            .await?;
        let mut file = File::from_tokio_file(fd).await;
        debug!("load index header");
        let mut buf = vec![0; Header::serialized_size_default()?.try_into()?];
        debug!("read header into buf: [0; {}]", buf.len());
        file.read_exact(&mut buf).await?;
        debug!("serialize header from bytes");
        let header = Header::from_raw(&buf)?;
        debug!("load filter");
        let mut buf = vec![0; header.filter_buf_size];
        debug!("read filter into buf: [0; {}]", buf.len());
        file.read_exact(&mut buf).await?;
        let filter = Bloom::from_raw(&buf)?;
        debug!("index restored successfuly");
        error!("@TODO check consistency");
        Ok(Self {
            header,
            inner: State::OnDisk(file),
            name,
            filter,
        })
    }

    pub(crate) fn on_disk(&self) -> bool {
        match self.inner {
            State::OnDisk(_) => true,
            _ => false,
        }
    }

    pub(crate) fn get_entry<'a, 'b: 'a>(&'b self, key: &'a [u8], file: File) -> Entries<'a> {
        debug!("create iterator");
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
            h.blob_offset() + h.serialized_size(),
            h.meta_size().try_into().ok()?,
        ))
    }

    pub async fn load_records(mut file: &File) -> Result<Vec<RecordHeader>> {
        trace!("seek to file start");
        file.seek(SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        trace!("read to end index");
        file.read_to_end(&mut buf).await?;
        if buf.is_empty() {
            debug!("empty index file");
            Ok(Vec::new())
        } else {
            trace!("deserialize bunch");
            let header = Self::deserialize_header(&buf)?;
            Self::deserialize_bunch(
                &buf[header.serialized_size()? as usize + header.filter_buf_size..],
                header.records_count,
                header.record_header_size,
            )
        }
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
            let mid_record_header = Self::read_at(&mut file, mid, &header).await?;
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

    async fn read_at(file: &mut File, index: usize, header: &Header) -> Result<RecordHeader> {
        let header_size = bincode::serialized_size(&header)?;
        let offset = header_size + (header.record_header_size * index) as u64;
        let buf = file.read_at(header.record_header_size, offset).await?;
        debug!("file read at: {}, buf len: {}", offset, buf.len());
        Ok(deserialize(&buf)?)
    }

    async fn read_index_header(file: &mut File) -> Result<Header> {
        let header_size = Header::serialized_size_default()?.try_into()?;
        debug!("header s: {}", header_size);
        let mut buf = vec![0; header_size];
        debug!("seek to file start");
        file.seek(SeekFrom::Start(0)).await?;
        debug!("read header");
        file.read(&mut buf).await?;
        debug!("deserialize header");
        Ok(deserialize(&buf)?)
    }

    fn serialize_bunch(bunch: &mut [RecordHeader], filter: &Bloom) -> Result<Vec<u8>> {
        let record_header = bunch.first().ok_or(ErrorKind::EmptyIndexBunch)?;
        let record_header_size = record_header.serialized_size().try_into()?;
        debug!("record header serialized size: {}", record_header_size);
        bunch.sort_by_key(|h| h.key().to_vec());
        let filter_buf = filter.to_raw();
        let header = Header {
            record_header_size,
            records_count: bunch.len(),
            filter_buf_size: filter_buf.len(),
        };
        let hs: usize = header.serialized_size()?.try_into().expect("u64 to usize");
        debug!("index header size: {}b", hs);
        let mut buf = Vec::with_capacity(hs + bunch.len() * record_header_size);
        serialize_into(&mut buf, &header)?;
        debug!(
            "filter serialized_size: {}, header.filter_buf_size: {}, buf.len: {}",
            filter_buf.len(),
            header.filter_buf_size,
            buf.len()
        );
        buf.extend_from_slice(&filter_buf);
        debug!("buf len after: {}", buf.len());
        bunch
            .iter()
            .filter_map(|h| {
                trace!("write key: {:?}", h.key());
                serialize(&h).ok()
            })
            .fold(&mut buf, |acc, h_buf| {
                acc.extend_from_slice(&h_buf);
                acc
            });
        Ok(buf)
    }

    fn check_result(res: Result<RecordHeader>) -> Result<bool> {
        match res {
            Ok(_) => Ok(true),
            Err(ref e) if e.is(&ErrorKind::RecordNotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn deserialize_header(buf: &[u8]) -> Result<Header> {
        trace!("deserialize header from buf: {}", buf.len());
        deserialize(buf).map_err(Error::new)
    }

    fn deserialize_bunch(
        buf: &[u8],
        count: usize,
        record_header_size: usize,
    ) -> Result<Vec<RecordHeader>> {
        (0..count).try_fold(Vec::new(), |mut record_headers, i| {
            let offset = i * record_header_size;
            trace!("at: {}", offset);
            let header = deserialize(&buf[offset..])?;
            record_headers.push(header);
            Ok(record_headers)
        })
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
        let fd_res = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.name.to_path())
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
        debug!("read to end index file");
        file.read_to_end(&mut buf).map_err(Error::new).await?;
        trace!("read total {} bytes", buf.len());
        let header = Self::deserialize_header(&buf)?;
        debug!("header: {:?}", header);
        let offset = header.serialized_size()? as usize;
        trace!("filter offset: {}", offset);
        let buf_ref = &buf[offset..];
        trace!("slice len: {}", buf_ref.len());
        let filter = Bloom::from_raw(buf_ref)?;
        let bunch = Self::deserialize_bunch(
            &buf[offset + header.filter_buf_size..],
            header.records_count,
            header.record_header_size,
        )?;
        self.inner = State::InMemory(bunch);
        self.filter = filter;
        Ok(())
    }

    fn count_inner(self) -> usize {
        if let State::InMemory(index) = self.inner {
            index.len()
        } else {
            self.header.records_count
        }
    }
}

impl Index for Simple {
    fn contains_key(&self, key: &[u8]) -> PinBox<dyn Future<Output = Result<bool>> + Send> {
        self.get(key).map(Self::check_result).boxed()
    }

    fn push(&mut self, h: RecordHeader) -> Push {
        let fut = match &mut self.inner {
            State::InMemory(bunch) => {
                trace!("add header to filter");
                self.filter.add(h.key());
                bunch.push(h);
                future::ok(()).boxed()
            }
            State::OnDisk(_) => future::err(
                ErrorKind::Index("Index is closed, push is unavalaible".to_string()).into(),
            )
            .boxed(),
        };
        trace!("future created");
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
            let buf = Self::serialize_bunch(bunch, &self.filter);
            match buf {
                Ok(buf) => self.dump_in_memory(buf),
                Err(ref e) if e.is(&ErrorKind::EmptyIndexBunch) => Dump(future::ok(()).boxed()),
                Err(e) => Dump(future::err(e).boxed()),
            }
        } else {
            Dump(future::ok(()).boxed())
        }
    }

    fn load(&mut self) -> Load {
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
