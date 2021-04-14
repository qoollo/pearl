use super::prelude::*;

pub(crate) type Index = IndexStruct<BPTreeFileIndex>;

pub(crate) const HEADER_VERSION: u64 = 1;

#[derive(Debug)]
struct IndexParams {
    filter_is_on: bool,
    recreate_file: bool,
}

impl IndexParams {
    fn new(filter_is_on: bool, recreate_file: bool) -> Self {
        Self {
            filter_is_on,
            recreate_file,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub filter: Option<BloomConfig>,
    pub recreate_index_file: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            filter: None,
            recreate_index_file: true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct IndexStruct<FileIndex: FileIndexTrait> {
    mem: Option<MemoryAttrs>,
    filter: Bloom,
    params: IndexParams,
    inner: State<FileIndex>,
    name: FileName,
    ioring: Option<Rio>,
}

#[derive(Debug, Default)] // Default can be used to initialize structure with 0
pub(crate) struct MemoryAttrs {
    pub(crate) key_size: usize,
    pub(crate) btree_entry_size: usize,
    pub(crate) records_allocated: usize,
    // contains actual size occupied by record header in RAM (which helps
    // to compute actual size of indices in RAM in `InMemory` state)
    pub(crate) record_header_size: usize,
    pub(crate) records_count: usize,
}

pub type InMemoryIndex = BTreeMap<Vec<u8>, Vec<RecordHeader>>;

#[derive(Debug, Clone)]
pub(crate) enum State<FileIndex: FileIndexTrait> {
    InMemory(InMemoryIndex),
    OnDisk(FileIndex),
}

impl<FileIndex: FileIndexTrait> IndexStruct<FileIndex> {
    pub(crate) fn new(name: FileName, ioring: Option<Rio>, config: IndexConfig) -> Self {
        let params = IndexParams::new(config.filter.is_some(), config.recreate_index_file);
        let filter = config.filter.map(Bloom::new).unwrap_or_default();
        let mem = Some(Default::default());
        Self {
            params,
            filter,
            inner: State::InMemory(BTreeMap::new()),
            mem,
            name,
            ioring,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inner = State::InMemory(BTreeMap::new());
        self.mem = Some(Default::default());
        self.filter.clear();
    }

    pub fn check_bloom_key(&self, key: &[u8]) -> Option<bool> {
        if self.params.filter_is_on {
            Some(self.filter.contains(key))
        } else {
            None
        }
    }

    pub(crate) fn name(&self) -> &FileName {
        &self.name
    }

    pub(crate) async fn from_file(
        name: FileName,
        config: IndexConfig,
        ioring: Option<Rio>,
    ) -> Result<Self> {
        let findex = FileIndex::from_file(name.clone(), ioring.clone()).await?;
        findex.validate().with_context(|| "Header is corrupt")?;
        let filter = findex.read_filter().await?;
        let params = IndexParams::new(config.filter.is_some(), config.recreate_index_file);
        trace!("index restored successfuly");
        let index = Self {
            inner: State::OnDisk(findex),
            mem: None,
            name,
            filter,
            params,
            ioring,
        };
        Ok(index)
    }

    pub(crate) fn on_disk(&self) -> bool {
        matches!(&self.inner, State::OnDisk(_))
    }

    async fn dump_in_memory(&mut self) -> Result<usize> {
        if let State::InMemory(headers) = &self.inner {
            if headers.len() == 0 {
                return Ok(0);
            }
            debug!("blob index simple in memory headers {}", headers.len());
            let findex = FileIndex::from_records(
                &self.name.to_path(),
                self.ioring.clone(),
                headers,
                &self.filter,
                self.params.recreate_file,
            )
            .await?;
            let size = findex.file_size() as usize;
            self.inner = State::OnDisk(findex);
            self.mem = None;
            Ok(size)
        } else {
            Ok(0)
        }
    }

    async fn load_in_memory(&mut self, findex: FileIndex) -> Result<()> {
        let (record_headers, records_count) = findex.get_records_headers().await?;
        self.mem = Some(compute_mem_attrs(&record_headers, records_count));
        self.inner = State::InMemory(record_headers);
        self.filter = findex.read_filter().await?;
        Ok(())
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
                - (mem.records_allocated - mem.records_count) * mem.key_size
        } else {
            0
        }
    }
}

#[async_trait::async_trait]
impl<FileIndex: FileIndexTrait + Sync + Send + Clone> IndexTrait for IndexStruct<FileIndex> {
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
                    if mem.records_count == 0 {
                        set_key_related_fields(mem, h.key().len());
                    }
                    let k = h.key().to_vec();
                    let v = vec![h];
                    mem.records_allocated += v.capacity(); // capacity == 1
                    headers.insert(k, v);
                }
                mem.records_count += 1;
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
            State::OnDisk(findex) => findex.find_by_key(key).await,
        }
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        debug!("index get any");
        match &self.inner {
            State::InMemory(headers) => {
                debug!("index get any in memory headers: {}", headers.len());
                Ok(headers.get(key).and_then(|h| h.first()).cloned())
            }
            State::OnDisk(findex) => {
                debug!("index get any on disk");
                let header = findex.get_any(key).await?;
                Ok(header)
            }
        }
    }

    async fn dump(&mut self) -> Result<usize> {
        self.dump_in_memory().await
    }

    async fn load(&mut self) -> Result<()> {
        match &self.inner {
            State::InMemory(_) => Ok(()),
            State::OnDisk(findex) => {
                let findex = findex.clone();
                self.load_in_memory(findex).await
            }
        }
    }

    fn count(&self) -> usize {
        match self.inner {
            State::OnDisk(ref findex) => findex.records_count(),
            State::InMemory(_) => self
                .mem
                .as_ref()
                .map(|mem| mem.records_count)
                .expect("No memory info in `InMemory` State"),
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait FileIndexTrait: Sized {
    async fn from_file(name: FileName, ioring: Option<Rio>) -> Result<Self>;
    async fn from_records(
        path: &Path,
        rio: Option<Rio>,
        headers: &InMemoryIndex,
        filter: &Bloom,
        recreate_index_file: bool,
    ) -> Result<Self>;
    fn file_size(&self) -> u64;
    fn records_count(&self) -> usize;
    async fn read_filter(&self) -> Result<Bloom>;
    async fn find_by_key(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>>;
    async fn get_records_headers(&self) -> Result<(InMemoryIndex, usize)>;
    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>>;
    fn validate(&self) -> Result<()>;
}
