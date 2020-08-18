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
struct IndexHeader {
    records_count: usize,
    record_header_size: usize,
    filter_buf_size: usize,
}

impl IndexHeader {
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

    pub(crate) async fn get_all_meta_locations(&self, key: &[u8]) -> Result<Option<Vec<Location>>> {
        debug!("blob index simple get all meta locations");
        match &self.inner {
            State::InMemory(headers) => {
                debug!("blob index simple get all meta locations from in memory state");
                Ok(headers
                    .get(key)
                    .map(|v| Self::meta_locations_from_headers(v)))
            }
            State::OnDisk(file) => {
                debug!("blob index simple get all meta locations from on disk state");
                let headers = Self::search_all(file, key, &self.header)
                    .await
                    .with_context(|| "blob index get all meta locations search all failed")?;
                Ok(headers.map(|headers| Self::meta_locations_from_headers(&headers)))
            }
        }
    }

    fn meta_locations_from_headers(headers: &[RecordHeader]) -> Vec<Location> {
        headers
            .iter()
            .filter_map(Self::try_create_meta_location)
            .collect()
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

    pub(crate) fn get_entries<'a, 'b: 'a>(&'b self, key: &'a [u8], file: File) -> Entries<'a> {
        trace!("create iterator");
        Entries::new(&self.inner, key, file)
    }

    fn try_create_meta_location(h: &RecordHeader) -> Option<Location> {
        Some(Location::new(
            h.blob_offset() + h.serialized_size(),
            h.meta_size().try_into().ok()?,
        ))
    }

    pub async fn load_records(file: &File) -> Result<InMemoryIndex> {
        let buf = file.read_all().await?;
        let headers = if buf.is_empty() {
            debug!("empty index file");
            InMemoryIndex::new()
        } else {
            trace!("deserialize headers");
            let header = Self::deserialize_header(&buf)?;
            Self::deserialize_record_headers(
                &buf[header.serialized_size()? as usize + header.filter_buf_size..],
                header.records_count,
                header.record_header_size,
            )?
        };
        Ok(headers)
    }

    async fn binary_search(
        file: &File,
        key: &[u8],
        header: &IndexHeader,
    ) -> Result<Option<(RecordHeader, usize)>> {
        debug!("blob index simple binary search header {:?}", header);

        if key.is_empty() {
            error!("empty key was provided");
        }

        let mut start = 0;
        let mut end = header.records_count - 1;

        while start <= end {
            let mid = (start + end) / 2;
            let mid_record_header = Self::read_at(file, mid, &header).await?;
            debug!(
                "blob index simple binary search mid header: {:?}",
                mid_record_header
            );
            let cmp = mid_record_header.key().cmp(&key);
            debug!("mid read: {:?}, key: {:?}", mid_record_header.key(), key);
            debug!("before mid: {:?}, start: {:?}, end: {:?}", mid, start, end);
            match cmp {
                CmpOrdering::Greater => end = mid - 1,
                CmpOrdering::Equal => {
                    return Ok(Some((mid_record_header, mid)));
                }
                CmpOrdering::Less => start = mid + 1,
            };
            debug!("after mid: {:?}, start: {:?}, end: {:?}", mid, start, end);
        }
        debug!("record with key: {:?} not found", key);
        Ok(None)
    }

    async fn search_all(
        file: &File,
        key: &[u8],
        index_header: &IndexHeader,
    ) -> Result<Option<Vec<RecordHeader>>> {
        let file2 = file.clone();
        if let Some(header_pos) = Self::binary_search(file, key, index_header)
            .await
            .with_context(|| "blob, index simple, search all, binary search failed")?
        {
            let orig_pos = header_pos.1;
            debug!(
                "blob index simple search all total {}, pos {}",
                index_header.records_count, orig_pos
            );
            let mut headers: Vec<RecordHeader> = vec![header_pos.0];
            // go left
            let mut pos = orig_pos;
            debug!(
                "blob index simple search all headers {}, pos {}",
                headers.len(),
                pos
            );
            while pos > 0 {
                pos -= 1;
                debug!(
                    "blob index simple search all headers {}, pos {}",
                    headers.len(),
                    pos
                );
                let rh = Self::read_at(&file2, pos, &index_header)
                    .await
                    .with_context(|| "blob, index simple, search all, read at failed")?;
                if rh.key() == key {
                    headers.push(rh);
                } else {
                    break;
                }
            }
            debug!(
                "blob index simple search all headers {}, pos {}",
                headers.len(),
                pos
            );
            //go right
            pos = orig_pos + 1;
            while pos < index_header.records_count {
                debug!(
                    "blob index simple search all headers {}, pos {}",
                    headers.len(),
                    pos
                );
                let rh = Self::read_at(&file2, pos, &index_header)
                    .await
                    .with_context(|| "blob, index simple, search all, read at failed")?;
                if rh.key() == key {
                    headers.push(rh);
                    pos += 1;
                } else {
                    break;
                }
            }
            debug!(
                "blob index simple search all headers {}, pos {}",
                headers.len(),
                pos
            );
            Ok(Some(headers))
        } else {
            debug!("Record not found by binary search on disk");
            Ok(None)
        }
    }

    async fn read_at(file: &File, index: usize, header: &IndexHeader) -> Result<RecordHeader> {
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

    async fn read_index_header(file: &mut File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default()?.try_into()?;
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    fn serialize_record_headers(
        headers: &InMemoryIndex,
        filter: &Bloom,
    ) -> Result<Option<(IndexHeader, Vec<u8>)>> {
        debug!("blob index simple serialize headers");
        if let Some(record_header) = headers.values().next().and_then(|v| v.first()) {
            debug!(
                "blob index simple serialize headers first header {:?}",
                record_header
            );
            let record_header_size = record_header.serialized_size().try_into()?;
            trace!("record header serialized size: {}", record_header_size);
            let headers = headers.iter().flat_map(|r| r.1).collect::<Vec<_>>(); // produce sorted
            debug!("blob index simple serialize bunch transform BTreeMap into Vec");
            //bunch.sort_by_key(|h| h.key().to_vec());
            let filter_buf = filter.to_raw()?;
            let header = IndexHeader {
                record_header_size,
                records_count: headers.len(),
                filter_buf_size: filter_buf.len(),
            };
            let hs: usize = header.serialized_size()?.try_into().expect("u64 to usize");
            trace!("index header size: {}b", hs);
            let mut buf = Vec::with_capacity(hs + headers.len() * record_header_size);
            serialize_into(&mut buf, &header)?;
            debug!(
            "blob index simple serialize headers filter serialized_size: {}, header.filter_buf_size: {}, buf.len: {}",
            filter_buf.len(),
            header.filter_buf_size,
            buf.len()
        );
            buf.extend_from_slice(&filter_buf);
            headers
                .iter()
                .filter_map(|h| serialize(&h).ok())
                .fold(&mut buf, |acc, h_buf| {
                    acc.extend_from_slice(&h_buf);
                    acc
                });
            debug!(
                "blob index simple serialize headers buf len after: {}",
                buf.len()
            );
            Ok(Some((header, buf)))
        } else {
            Ok(None)
        }
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
            .with_context(|| {
                format!(
                    "blob index simple dump in memory open index file {:?}",
                    self.name.to_path()
                )
            })?;
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

    fn count_inner(self) -> usize {
        if let State::InMemory(index) = self.inner {
            index.len()
        } else {
            self.header.records_count
        }
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
                if let Some(v) = headers.get_mut(h.key()) {
                    v.push(h);
                    debug!("blob index simple push headers for key: {:?}", v);
                } else {
                    headers.insert(h.key().to_vec(), vec![h]);
                }
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
            State::OnDisk(file) => Self::search_all(file, key, &self.header).await,
        }
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        debug!("index get any");
        match &self.inner {
            State::InMemory(headers) => {
                debug!("index get any in memory headers: {}", headers.len());
                if let Some(header) = headers.get(key).and_then(|h| h.first()) {
                    debug!("index get any in memory header found");
                    Ok(Some(header.clone()))
                } else {
                    Ok(None)
                }
            }
            State::OnDisk(index_file) => {
                debug!("index get any on disk");
                if let Some((header, _)) =
                    Self::binary_search(index_file, &key.to_vec(), &self.header).await?
                {
                    debug!("index get any on disk header found");
                    Ok(Some(header))
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn dump(&mut self) -> Result<usize> {
        if let State::InMemory(headers) = &mut self.inner {
            debug!("blob index simple in memory headers {}", headers.len());
            let res = Self::serialize_record_headers(headers, &self.filter)?;
            match res {
                Some((header, buf)) => {
                    self.header = header;
                    self.dump_in_memory(buf).await
                }
                None => Ok(0),
            }
        } else {
            Ok(0)
        }
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

    async fn count(&self) -> Result<usize> {
        match &self.inner {
            State::InMemory(headers) => Ok(headers.values().fold(0, |acc, x| acc + x.len())),
            State::OnDisk(_) => {
                Self::from_file(self.name.clone(), self.filter_is_on, self.ioring.clone())
                    .map_ok(Self::count_inner)
                    .await
            }
        }
    }
}
