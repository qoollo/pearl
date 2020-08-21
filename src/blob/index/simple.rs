use super::prelude::*;

#[derive(Debug)]
pub(crate) struct Simple {
    header: IndexHeader,
    filter: Bloom,
    filter_is_on: bool,
    inner: State,
    name: FileName,
    ioring: Rio,
}

#[derive(Debug, Deserialize, Default, Serialize, Clone)]
pub(crate) struct IndexHeader {
    pub records_count: usize,
    pub record_header_size: usize,
    pub filter_buf_size: usize,
}

impl IndexHeader {
    fn serialized_size_default() -> bincode::Result<u64> {
        let header = Self::default();
        header.serialized_size()
    }

    #[inline]
    pub fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    #[inline]
    fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(buf)
    }
}

pub type InMemoryIndex = BTreeMap<Vec<u8>, Vec<RecordHeader>>;

#[derive(Debug, Clone)]
pub(crate) enum State {
    InMemory(InMemoryIndex),
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
            header: IndexHeader {
                records_count: 0,
                record_header_size: 0,
                filter_buf_size: 0,
            },
            filter_is_on,
            filter,
            inner: State::InMemory(BTreeMap::new()),
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

    pub(crate) async fn from_file(name: FileName, filter_is_on: bool, ioring: Rio) -> Result<Self> {
        trace!("open index file");
        let mut file = File::open(name.to_path(), ioring.clone())
            .await
            .context(format!("failed to open index file: {}", name))?;
        trace!("load index header");
        let header = Self::read_index_header(&mut file).await?;
        trace!("load filter");
        let mut buf = vec![0; header.filter_buf_size];
        trace!("read filter into buf: [0; {}]", buf.len());
        file.read_at(&mut buf, header.serialized_size()?).await?;
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

    async fn read_index_header(file: &mut File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default()?.try_into()?;
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    fn deserialize_header(buf: &[u8]) -> bincode::Result<IndexHeader> {
        trace!("deserialize header from buf: {}", buf.len());
        deserialize(buf)
    }

    fn deserialize_record_headers(
        buf: &[u8],
        count: usize,
        record_header_size: usize,
    ) -> Result<InMemoryIndex> {
        (0..count).try_fold(InMemoryIndex::new(), |mut headers, i| {
            let offset = i * record_header_size;
            trace!("at: {}", offset);
            let header: RecordHeader = deserialize(&buf[offset..])?;
            if let Some(v) = headers.get_mut(header.key()) {
                v.push(header)
            } else {
                headers.insert(header.key().to_vec(), vec![header]);
            }
            Ok(headers)
        })
    }

    async fn dump_in_memory(&mut self, buf: Vec<u8>) -> Result<usize> {
        let file = File::create(self.name.to_path(), self.ioring.clone())
            .await
            .with_context(|| format!("index dump in memory file open {:?}", self.name.to_path()))?;
        let inner = State::OnDisk(file.clone());
        self.inner = inner;
        file.write_append(&buf).await.map_err(Into::into)
    }

    async fn load_in_memory(&mut self, file: File) -> Result<()> {
        let buf = file.read_all().await?;
        trace!("read total {} bytes", buf.len());
        let header = Self::deserialize_header(&buf)?;
        debug!("header: {:?}", header);
        let offset = header.serialized_size()? as usize;
        trace!("filter offset: {}", offset);
        let buf_ref = &buf[offset..];
        trace!("slice len: {}", buf_ref.len());
        let filter = Bloom::from_raw(buf_ref)?;
        let record_headers = Self::deserialize_record_headers(
            &buf[offset + header.filter_buf_size..],
            header.records_count,
            header.record_header_size,
        )?;
        self.inner = State::InMemory(record_headers);
        self.filter = filter;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Index for Simple {
    async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.get_any(key).await.map(|h| h.is_some())
    }

    fn push(&mut self, h: RecordHeader) -> Result<()> {
        debug!("blob index simple push");
        match &mut self.inner {
            State::InMemory(headers) => {
                debug!("blob index simple push bloom filter add");
                self.filter.add(h.key());
                debug!("blob index simple push key: {:?}", h.key());
                headers.entry(h.key().to_vec()).or_default().push(h);
                self.header.records_count += 1;
                Ok(())
            }
            State::OnDisk(_) => Err(Error::from(ErrorKind::Index(
                "Index is closed, push is unavalaible".to_string(),
            ))
            .into()),
        }
    }

    async fn get_all(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>> {
        match &self.inner {
            State::InMemory(headers) => Ok(headers.get(key).cloned()),
            State::OnDisk(file) => search_all(file, key, &self.header).await,
        }
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        debug!("index get any");
        match &self.inner {
            State::InMemory(headers) => {
                debug!("index get any in memory headers: {}", headers.len());
                Ok(headers.get(key).and_then(|h| h.first()).cloned())
            }
            State::OnDisk(index_file) => {
                debug!("index get any on disk");
                let header = binary_search(index_file, &key.to_vec(), &self.header).await?;
                Ok(header.map(|h| h.0))
            }
        }
    }

    async fn dump(&mut self) -> Result<usize> {
        if let State::InMemory(headers) = &mut self.inner {
            debug!("blob index simple in memory headers {}", headers.len());
            let res = serialize_record_headers(headers, &self.filter)?;
            if let Some((header, buf)) = res {
                self.header = header;
                return self.dump_in_memory(buf).await;
            }
        }
        Ok(0)
    }

    async fn load(&mut self) -> Result<()> {
        match &self.inner {
            State::InMemory(_) => Ok(()),
            State::OnDisk(file) => {
                let file = file.clone();
                self.load_in_memory(file).await
            }
        }
    }

    fn count(&self) -> usize {
        self.header.records_count
    }
}
