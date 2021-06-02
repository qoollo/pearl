use super::prelude::*;
use std::mem::size_of;

const HEADER_VERSION: u64 = 1;

#[derive(Debug)]
pub(crate) struct Simple {
    header: IndexHeader,
    mem: Option<MemoryAttrs>,
    filter: Bloom,
    filter_is_on: bool,
    inner: State,
    name: FileName,
    ioring: Option<Rio>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct IndexHeader {
    pub records_count: usize,
    // contains serialized size of record headers, which allows to calculate offset in
    // case of `OnDisk` state of indices
    pub record_header_size: usize,
    pub filter_buf_size: usize,
    pub hash: Vec<u8>,
    version: u64,
    written: u8,
}

#[derive(Debug, Default)] // Default can be used to initialize structure with 0
pub(crate) struct MemoryAttrs {
    pub key_size: usize,
    pub btree_entry_size: usize,
    pub records_allocated: usize,
    // contains actual size occupied by record header in RAM (which helps
    // to compute actual size of indices in RAM in `InMemory` state)
    pub record_header_size: usize,
}

impl IndexHeader {
    pub fn new(record_header_size: usize, records_count: usize, filter_buf_size: usize) -> Self {
        Self {
            records_count,
            record_header_size,
            filter_buf_size,
            ..Self::default()
        }
    }

    pub fn with_hash(
        record_header_size: usize,
        records_count: usize,
        filter_buf_size: usize,
        hash: Vec<u8>,
    ) -> Self {
        Self {
            records_count,
            record_header_size,
            filter_buf_size,
            hash,
            ..Self::default()
        }
    }

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

impl Default for IndexHeader {
    fn default() -> Self {
        Self {
            records_count: 0,
            record_header_size: 0,
            filter_buf_size: 0,
            hash: vec![0; ring::digest::SHA256.output_len],
            version: HEADER_VERSION,
            written: 0,
        }
    }
}

pub type InMemoryIndex = BTreeMap<Vec<u8>, Vec<RecordHeader>>;

#[derive(Debug, Clone)]
pub(crate) enum State {
    InMemory(InMemoryIndex),
    OnDisk(File),
}

impl Simple {
    pub(crate) fn new(name: FileName, ioring: Option<Rio>, filter_config: Option<Config>) -> Self {
        let filter_is_on = filter_config.is_some();
        let filter = filter_config.map(Bloom::new).unwrap_or_default();
        let header = IndexHeader::default();
        let mem = Some(Default::default());
        Self {
            header,
            filter_is_on,
            filter,
            inner: State::InMemory(BTreeMap::new()),
            mem,
            name,
            ioring,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.header = IndexHeader::default();
        self.inner = State::InMemory(BTreeMap::new());
        self.mem = Some(Default::default());
        self.filter.clear();
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

    pub(crate) async fn from_file(
        name: FileName,
        filter_is_on: bool,
        ioring: Option<Rio>,
    ) -> Result<Self> {
        trace!("open index file");
        let mut file = File::open(name.to_path(), ioring.clone())
            .await
            .context(format!("failed to open index file: {}", name))?;
        trace!("load index header");
        let header = Self::read_index_header(&mut file).await?;
        if header.written != 1 {
            return Err(Error::validation("Header is corrupt").into());
        }
        trace!("load filter");
        let mut buf = vec![0; header.filter_buf_size];
        trace!("read filter into buf: [0; {}]", buf.len());
        file.read_at(&mut buf, header.serialized_size()?).await?;
        let filter = Bloom::from_raw(&buf)?;
        trace!("index restored successfuly");
        let index = Self {
            header,
            inner: State::OnDisk(file),
            mem: None,
            name,
            filter,
            filter_is_on,
            ioring,
        };
        Ok(index)
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
            let header: RecordHeader = deserialize(&buf[offset..])?;
            // We used get mut instead of entry(..).or_insert(..) because in second case we
            // need to clone key everytime. Whereas in first case - only if we insert new
            // entry.
            if let Some(v) = headers.get_mut(header.key()) {
                v.push(header)
            } else {
                headers.insert(header.key().to_vec(), vec![header]);
            }
            Ok(headers)
        })
    }

    async fn dump_in_memory(&mut self, buf: Vec<u8>) -> Result<usize> {
        let _ = std::fs::remove_file(self.name.to_path());
        let file = File::create(self.name.to_path(), self.ioring.clone())
            .await
            .with_context(|| format!("file open failed {:?}", self.name.to_path()))?;
        let size = file.write_append(&buf).await?;
        self.apply_written_byte(&file).await?;
        file.fsyncdata().await?;
        self.inner = State::OnDisk(file);
        self.mem = None;
        Ok(size)
    }

    async fn apply_written_byte(&mut self, file: &File) -> Result<()> {
        self.header.written = 1;
        let serialized = serialize(&self.header)?;
        file.write_at(0, &serialized).await?;
        Ok(())
    }

    async fn load_in_memory(&mut self, file: File) -> Result<()> {
        let mut buf = file.read_all().await?;
        trace!("read total {} bytes", buf.len());
        let header = Self::deserialize_header(&buf)?;
        if header.written != 1 {
            return Err(Error::validation("header was not written").into());
        }
        if !Self::hash_valid(&header, &mut buf)? {
            return Err(Error::validation("header hash mismatch").into());
        }
        if header.version != HEADER_VERSION {
            return Err(Error::validation("header version mismatch").into());
        }
        debug!("header: {:?}", header);
        let offset = header.serialized_size()? as usize;
        trace!("filter offset: {}", offset);
        let buf_ref = &buf[offset..];
        trace!("slice len: {}", buf_ref.len());
        let record_headers = Self::deserialize_record_headers(
            &buf[offset + header.filter_buf_size..],
            header.records_count,
            header.record_header_size,
        )?;
        self.mem = Some(compute_mem_attrs(&record_headers));
        self.inner = State::InMemory(record_headers);
        self.filter = Bloom::from_raw(buf_ref)?;
        Ok(())
    }

    fn hash_valid(header: &IndexHeader, buf: &mut Vec<u8>) -> Result<bool> {
        let mut header = header.clone();
        let hash = header.hash.clone();
        header.hash = vec![0; ring::digest::SHA256.output_len];
        header.written = 0;
        serialize_into(buf.as_mut_slice(), &header)?;
        let new_hash = get_hash(&buf);
        Ok(hash == new_hash)
    }

    pub(crate) fn memory_used(&self) -> usize {
        if let State::InMemory(data) = &self.inner {
            let mem = self
                .mem
                .as_ref()
                .expect("No memory info in `InMemory` State");
            trace!("record_header_size: {}, records_allocated: {}, data.len(): {}, entry_size (key + vec): {}",
                mem.record_header_size, mem.records_allocated, data.len(), mem.btree_entry_size
            );
            // last minus is neccessary, because allocated but not initialized record headers don't
            // have key allocated on heap
            mem.record_header_size * mem.records_allocated + data.len() * mem.btree_entry_size
                - (mem.records_allocated - self.header.records_count) * mem.key_size
        } else {
            0
        }
    }

    fn mark_all_as_deleted_in_memory(&mut self, key: &[u8]) -> Result<Option<u64>> {
        if let State::InMemory(headers) = &mut self.inner {
            debug!(
                "simple index mark all as deleted in memory headers: {}",
                headers.len()
            );
            if let Some(vec_headers) = headers.get_mut(key) {
                for header in vec_headers.iter_mut() {
                    header.mark_as_deleted()?;
                }
                Ok(Some(vec_headers.len() as u64))
            } else {
                Ok(None)
            }
        } else {
            Err(Error::from(ErrorKind::Index(
                "Failed to load index to memory".to_string(),
            ))
            .into())
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
                let mem = self
                    .mem
                    .as_mut()
                    .expect("No memory info in `InMemory` State");
                // Same reason to use get_mut as in deserialize_record_headers.
                if let Some(v) = headers.get_mut(h.key()) {
                    let old_capacity = v.capacity();
                    v.push(h);
                    trace!("capacity growth: {}", v.capacity() - old_capacity);
                    mem.records_allocated += v.capacity() - old_capacity;
                } else {
                    if self.header.records_count == 0 {
                        // record header contains key as Vec<u8>, h.key().len() - data on the heap
                        mem.record_header_size = size_of::<RecordHeader>() + h.key().len();
                        // every entry also includes data on heap: capacity * size_of::<RecordHeader>()
                        mem.key_size = h.key().len();
                        mem.btree_entry_size =
                            size_of::<Vec<u8>>() + mem.key_size + size_of::<Vec<RecordHeader>>();
                    }
                    let k = h.key().to_vec();
                    let v = vec![h];
                    mem.records_allocated += v.capacity(); // capacity == 1
                    headers.insert(k, v);
                }
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
        Ok(match &self.inner {
            State::InMemory(headers) => headers.get(key).cloned(),
            State::OnDisk(file) => search_all(file, key, &self.header).await?,
        }
        .and_then(|headers| {
            let res = headers
                .into_iter()
                .filter(|header| header.is_deleted())
                .collect::<Vec<_>>();
            if res.is_empty() {
                None
            } else {
                Some(res)
            }
        }))
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        debug!("index get any");
        match &self.inner {
            State::InMemory(headers) => {
                debug!("index get any in memory headers: {}", headers.len());
                Ok(headers
                    .get(key)
                    .and_then(|h| h.iter().find(|h| !h.is_deleted()))
                    .cloned())
            }
            State::OnDisk(index_file) => {
                debug!("index get any on disk");
                let header = binary_search(index_file, &key.to_vec(), &self.header).await?;
                let header = if let Some((header, _)) = header {
                    if header.is_deleted() {
                        search_all(index_file, key, &self.header)
                            .await?
                            .and_then(|headers| {
                                headers.into_iter().find(|header| !header.is_deleted())
                            })
                    } else {
                        Some(header)
                    }
                } else {
                    None
                };
                Ok(header)
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

    async fn mark_all_as_deleted(&mut self, key: &[u8]) -> Result<Option<u64>> {
        debug!("index mark all as deleted all");
        if self.on_disk() {
            debug!("index mark all as deleted all on disk");
            self.load().await?;
            let res = self.mark_all_as_deleted_in_memory(key)?;
            self.dump().await?;
            Ok(res)
        } else {
            debug!("index mark all as deleted all in memory");
            self.mark_all_as_deleted_in_memory(key)
        }
    }
}
