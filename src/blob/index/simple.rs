use super::prelude::*;

#[derive(Debug)]
pub(crate) struct Simple {
    header: Header,
    filter: Bloom,
    filter_is_on: bool,
    inner: State,
    name: FileName,
    ioring: Rio,
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
    pub(crate) fn new(name: FileName, ioring: Rio, filter_config: Option<Config>) -> Self {
        let filter_is_on = filter_config.is_some();
        let filter = if let Some(config) = filter_config {
            trace!("create filter with config: {:?}", config);
            Bloom::new(config)
        } else {
            trace!("no config, filter created with default params and won't be used");
            Bloom::default()
        };
        Self {
            header: Header {
                records_count: 0,
                record_header_size: 0,
                filter_buf_size: 0,
            },
            filter_is_on,
            filter,
            inner: State::InMemory(Vec::new()),
            name,
            ioring,
        }
    }

    pub fn check_bloom_key(&self, key: &[u8]) -> Option<bool> {
        if self.filter_is_on {
            Some(self.filter.contains(key))
        } else {
            None
        }
    }

    pub(crate) const fn name(&self) -> &FileName {
        &self.name
    }

    pub(crate) async fn get_all_meta_locations(&self, key: &[u8]) -> AnyResult<Vec<Location>> {
        Ok(match &self.inner {
            State::InMemory(bunch) => Self::extract_matching_meta_locations(bunch, key),
            State::OnDisk(file) => {
                let record_headers = Self::load_records(file).await?;
                Self::extract_matching_meta_locations(&record_headers, key)
            }
        })
    }

    pub(crate) async fn from_file(
        name: FileName,
        filter_is_on: bool,
        ioring: Rio,
    ) -> AnyResult<Self> {
        trace!("open index file");
        let file = File::open(name.to_path(), ioring.clone())
            .await
            .context(format!("failed to open index file: {}", name))?;
        trace!("load index header");
        let mut header_buf = vec![0; Header::serialized_size_default()?.try_into()?];
        trace!("read header into buf: [0; {}]", header_buf.len());
        file.read_at(&mut header_buf, 0).await?;
        trace!("serialize header from bytes");
        let header = Header::from_raw(&header_buf)?;
        trace!("load filter");
        let mut buf = vec![0; header.filter_buf_size];
        trace!("read filter into buf: [0; {}]", buf.len());
        file.read_at(&mut buf, header_buf.len() as u64).await?;
        let filter = Bloom::from_raw(&buf)?;
        trace!("index restored successfuly");
        warn!("@TODO check consistency");
        Ok(Self {
            header,
            inner: State::OnDisk(file),
            name,
            filter,
            filter_is_on,
            ioring,
        })
    }

    pub(crate) fn on_disk(&self) -> bool {
        matches!(&self.inner, State::OnDisk(_))
    }

    pub(crate) fn get_entries<'a, 'b: 'a>(&'b self, key: &'a [u8], file: File) -> Entries<'a> {
        trace!("create iterator");
        Entries::new(&self.inner, key, file)
    }

    pub(crate) async fn get_any(&self, key: &[u8], file: File) -> AnyResult<Option<Entry>> {
        debug!("index get any");
        match &self.inner {
            State::InMemory(headers) => {
                debug!("index get any in memory headers: {}", headers.len());
                if let Some(header) = headers.iter().find(|h| h.key() == key) {
                    debug!("index get any in memory header found");
                    let entry = Entry::new(Meta::default(), header, file);
                    debug!("index get any in memory new entry created");
                    Ok(Some(entry))
                } else {
                    Ok(None)
                }
            }
            State::OnDisk(index_file) => {
                debug!("index get any on disk");
                if let Some(header) = Self::binary_search(index_file.clone(), key.to_vec()).await? {
                    debug!("index get any on disk header found");
                    let entry = Entry::new(Meta::default(), &header, file);
                    debug!("index get any on disk new entry created");
                    Ok(Some(entry))
                } else {
                    Ok(None)
                }
            }
        }
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

    pub async fn load_records(file: &File) -> AnyResult<Vec<RecordHeader>> {
        let buf = file.read_all().await?;
        let headers = if buf.is_empty() {
            debug!("empty index file");
            Vec::new()
        } else {
            trace!("deserialize bunch");
            let header = Self::deserialize_header(&buf)?;
            Self::deserialize_bunch(
                &buf[header.serialized_size()? as usize + header.filter_buf_size..],
                header.records_count,
                header.record_header_size,
            )?
        };
        Ok(headers)
    }

    async fn binary_search(mut file: File, key: Vec<u8>) -> AnyResult<Option<RecordHeader>> {
        debug!("blob index simple binary search");
        let header: Header = Self::read_index_header(&mut file).await?;
        debug!("blob index simple binary search header {:?}", header);

        let mut size = header.records_count;
        if key.is_empty() {
            error!("empty key was provided");
        }

        let mut start = 0;

        while size > 1 {
            let half = size / 2;
            let mid = start + half;
            let mid_record_header = Self::read_at(&mut file, mid, &header).await?;
            debug!(
                "blob index simple binary search mid header: {:?}",
                mid_record_header
            );
            let cmp = mid_record_header.key().cmp(&key);
            debug!("mid read: {:?}, key: {:?}", mid_record_header.key(), key);
            start = match cmp {
                CmpOrdering::Greater => start,
                CmpOrdering::Equal => {
                    return Ok(Some(mid_record_header));
                }
                CmpOrdering::Less => mid,
            };
            size -= half;
        }
        info!("record with key: {:?} not found", key);
        Ok(None)
    }

    async fn read_at(file: &mut File, index: usize, header: &Header) -> AnyResult<RecordHeader> {
        debug!("blob index simple read at");
        let header_size = bincode::serialized_size(&header)?;
        debug!("blob index simple read at header size {}", header_size);
        let offset = header_size
            + header.filter_buf_size as u64
            + (header.record_header_size * index) as u64;
        let mut buf = vec![0; header.record_header_size];
        debug!(
            "blob index simple offset: {}, buf len: {}",
            offset,
            buf.len()
        );
        file.read_at(&mut buf, offset).await?;
        let header = deserialize(&buf)?;
        debug!("blob index simple header: {:?}", header);
        Ok(header)
    }

    async fn read_index_header(file: &mut File) -> AnyResult<Header> {
        let header_size = Header::serialized_size_default()?.try_into()?;
        debug!("header s: {}", header_size);
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        debug!("deserialize header");
        Ok(deserialize(&buf)?)
    }

    fn serialize_bunch(bunch: &mut [RecordHeader], filter: &Bloom) -> Result<Vec<u8>> {
        debug!("blob index simple serialize bunch");
        let record_header = bunch.first().ok_or(ErrorKind::EmptyIndexBunch)?;
        debug!(
            "blob index simple serialize bunch first header {:?}",
            record_header
        );
        let record_header_size = record_header.serialized_size().try_into()?;
        trace!("record header serialized size: {}", record_header_size);
        bunch.sort_by_key(|h| h.key().to_vec());
        let filter_buf = filter.to_raw()?;
        let header = Header {
            record_header_size,
            records_count: bunch.len(),
            filter_buf_size: filter_buf.len(),
        };
        let hs: usize = header.serialized_size()?.try_into().expect("u64 to usize");
        trace!("index header size: {}b", hs);
        let mut buf = Vec::with_capacity(hs + bunch.len() * record_header_size);
        serialize_into(&mut buf, &header)?;
        debug!(
            "blob index simple serialize bunch filter serialized_size: {}, header.filter_buf_size: {}, buf.len: {}",
            filter_buf.len(),
            header.filter_buf_size,
            buf.len()
        );
        buf.extend_from_slice(&filter_buf);
        bunch
            .iter()
            .filter_map(|h| serialize(&h).ok())
            .fold(&mut buf, |acc, h_buf| {
                acc.extend_from_slice(&h_buf);
                acc
            });
        debug!(
            "blob index simple serialize bunch buf len after: {}",
            buf.len()
        );
        Ok(buf)
    }

    fn check_result(res: AnyResult<RecordHeader>) -> AnyResult<bool> {
        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                if let Some(err) = e.source().and_then(|src| src.downcast_ref::<Error>()) {
                    if !(err.is(&ErrorKind::RecordNotFound)) {
                        return Err(e);
                    }
                }
                Ok(false)
            }
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
    ) -> impl Future<Output = AnyResult<RecordHeader>> {
        future::ready(
            bunch
                .iter()
                .find(|h| h.key() == key)
                .cloned()
                .ok_or_else(|| Error::from(ErrorKind::RecordNotFound).into()),
        )
    }

    fn dump_in_memory(&mut self, buf: Vec<u8>, ioring: Rio) -> Dump {
        let fd_res = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.name.to_path())
            .expect("open new index file");
        let file = File::from_std_file(fd_res, ioring).expect("convert std file to own format");
        let inner = State::OnDisk(file.clone());
        self.inner = inner;
        let fut = async move { file.write_at(&buf, 0).await.map_err(Into::into) }.boxed();
        Dump(fut)
    }

    async fn load_in_memory(&mut self, file: File) -> AnyResult<()> {
        let buf = file.read_all().await?;
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
    fn contains_key(&self, key: &[u8]) -> PinBox<dyn Future<Output = AnyResult<bool>> + Send> {
        self.get(key).map(Self::check_result).boxed()
    }

    fn push(&mut self, h: RecordHeader) -> Push {
        debug!("blob index simple push");
        let fut = match &mut self.inner {
            State::InMemory(bunch) => {
                self.filter.add(h.key());
                debug!("blob index simple push key: {:?}", h.key());
                bunch.push(h);
                future::ok(()).boxed()
            }
            State::OnDisk(_) => future::err(
                Error::from(ErrorKind::Index(
                    "Index is closed, push is unavalaible".to_string(),
                ))
                .into(),
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
                let inner = async {
                    if let Some(header) = Self::binary_search(file, cloned_key).await? {
                        Ok(header)
                    } else {
                        Err(Error::from(ErrorKind::RecordNotFound).into())
                    }
                }
                .boxed();
                Get { inner }
            }
        }
    }

    fn dump(&mut self) -> Dump {
        if let State::InMemory(bunch) = &mut self.inner {
            debug!("blob index simple in memory bunch {:?}", bunch);
            let buf = Self::serialize_bunch(bunch, &self.filter);
            trace!("index serialized, errors: {:?}", buf.as_ref().err());
            match buf {
                Ok(buf) => self.dump_in_memory(buf, self.ioring.clone()),
                Err(ref e) if e.is(&ErrorKind::EmptyIndexBunch) => Dump(future::ok(0).boxed()),
                Err(e) => Dump(future::err(e).boxed()),
            }
        } else {
            Dump(future::ok(0).boxed())
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
            State::OnDisk(_) => {
                Self::from_file(self.name.clone(), self.filter_is_on, self.ioring.clone())
                    .map_ok(Self::count_inner)
                    .boxed()
            }
        })
    }
}
