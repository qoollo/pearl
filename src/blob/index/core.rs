use super::prelude::*;
use crate::bloom::BloomDataProvider;
use std::mem::size_of;

pub(crate) type Index = IndexStruct<BPTreeFileIndex>;

pub(crate) const HEADER_VERSION: u8 = 3;

#[derive(Debug)]
struct IndexParams {
    bloom_is_on: bool,
    recreate_file: bool,
}

impl IndexParams {
    fn new(bloom_is_on: bool, recreate_file: bool) -> Self {
        Self {
            bloom_is_on,
            recreate_file,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub bloom_config: Option<BloomConfig>,
    pub recreate_index_file: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            bloom_config: None,
            recreate_index_file: true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct IndexStruct<FileIndex: FileIndexTrait> {
    mem: Option<MemoryAttrs>,
    range_filter: RangeFilter,
    bloom_filter: Bloom,
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
        let params = IndexParams::new(config.bloom_config.is_some(), config.recreate_index_file);
        let filter = config.bloom_config.map(Bloom::new).unwrap_or_default();
        let mem = Some(Default::default());
        Self {
            params,
            bloom_filter: filter,
            range_filter: RangeFilter::new(),
            inner: State::InMemory(BTreeMap::new()),
            mem,
            name,
            ioring,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inner = State::InMemory(BTreeMap::new());
        self.mem = Some(Default::default());
        self.bloom_filter.clear();
        self.range_filter.clear();
    }

    pub fn offload_filter(&mut self) -> usize {
        if self.on_disk() {
            self.bloom_filter.offload_from_memory()
        } else {
            0
        }
    }

    pub(crate) async fn check_filters_key(&self, key: &[u8]) -> Result<FilterResult> {
        if !self.range_filter.contains(key)
            || (self.params.bloom_is_on
                && matches!(self.check_bloom_key(key).await?, FilterResult::NotContains))
        {
            Ok(FilterResult::NotContains)
        } else {
            Ok(FilterResult::NeedAdditionalCheck)
        }
    }

    pub(crate) fn check_filters_in_memory(&self, key: &[u8]) -> FilterResult {
        if !self.range_filter.contains(key)
            || self.check_bloom_key_in_memory(key) == FilterResult::NotContains
        {
            FilterResult::NotContains
        } else {
            FilterResult::NeedAdditionalCheck
        }
    }

    pub fn check_bloom_key_in_memory(&self, key: &[u8]) -> FilterResult {
        if self.params.bloom_is_on {
            self.bloom_filter
                .contains_in_memory(key)
                .unwrap_or_default()
        } else {
            FilterResult::NeedAdditionalCheck
        }
    }

    pub fn get_bloom_filter(&self) -> &Bloom {
        &self.bloom_filter
    }

    pub async fn check_bloom_key(&self, key: &[u8]) -> Result<FilterResult> {
        if self.params.bloom_is_on {
            if let Some(result) = self.bloom_filter.contains_in_memory(key) {
                Ok(result)
            } else {
                Ok(self.bloom_filter.contains_in_file(self, key).await?)
            }
        } else {
            Ok(FilterResult::NeedAdditionalCheck)
        }
    }

    pub fn is_filter_offloaded(&self) -> bool {
        self.bloom_filter.is_offloaded()
    }

    pub fn bloom_memory_allocated(&self) -> usize {
        self.bloom_filter.memory_allocated()
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
        let meta_buf = findex.read_meta().await?;
        let (bloom_filter, range_filter) = Self::deserialize_filters(&meta_buf)?;
        let params = IndexParams::new(config.bloom_config.is_some(), config.recreate_index_file);
        trace!("index restored successfuly");
        let index = Self {
            inner: State::OnDisk(findex),
            mem: None,
            name,
            bloom_filter,
            range_filter,
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
            let (meta_buf, bloom_offset) = self.serialize_filters()?;
            self.bloom_filter.set_offset_in_file(bloom_offset as u64);
            let findex = FileIndex::from_records(
                &self.name.to_path(),
                self.ioring.clone(),
                headers,
                meta_buf,
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

    fn serialize_filters(&self) -> Result<(Vec<u8>, usize)> {
        let range_buf = self.range_filter.to_raw()?;
        let range_buf_size = range_buf.len() as u64;
        let bloom_buf = self.bloom_filter.to_raw()?;
        let mut buf = Vec::with_capacity(size_of::<u64>() + range_buf.len() + bloom_buf.len());
        let bloom_offset = size_of::<u64>() + range_buf.len();
        buf.extend_from_slice(&serialize(&range_buf_size)?);
        buf.extend_from_slice(&range_buf);
        buf.extend_from_slice(&bloom_buf);
        Ok((buf, bloom_offset))
    }

    fn deserialize_filters(buf: &[u8]) -> Result<(Bloom, RangeFilter)> {
        let (range_size_buf, rest_buf) = buf.split_at(size_of::<u64>());
        let range_size = deserialize(&range_size_buf)?;
        let (range_buf, bloom_buf) = rest_buf.split_at(range_size);
        let bloom = Bloom::from_raw(bloom_buf, Some((range_size + size_of::<u64>()) as u64))?;
        let range = RangeFilter::from_raw(range_buf)?;
        Ok((bloom, range))
    }

    async fn load_in_memory(&mut self, findex: FileIndex) -> Result<()> {
        let (record_headers, records_count) = findex.get_records_headers().await?;
        self.mem = Some(compute_mem_attrs(&record_headers, records_count));
        self.inner = State::InMemory(record_headers);
        let meta_buf = findex.read_meta().await?;
        let (bloom_filter, range_filter) = Self::deserialize_filters(&meta_buf)?;
        self.bloom_filter = bloom_filter;
        self.range_filter = range_filter;
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
impl<FileIndex> IndexTrait for IndexStruct<FileIndex>
where
    FileIndex: FileIndexTrait + Clone,
{
    async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.get_any(key).await.map(|h| h.is_some())
    }

    fn push(&mut self, h: RecordHeader) -> Result<()> {
        debug!("blob index simple push");
        match &mut self.inner {
            State::InMemory(headers) => {
                debug!("blob index simple push bloom filter add");
                let _ = self.bloom_filter.add(h.key());
                self.range_filter.add(h.key());
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
pub(crate) trait FileIndexTrait: Sized + Send + Sync {
    async fn from_file(name: FileName, ioring: Option<Rio>) -> Result<Self>;
    async fn from_records(
        path: &Path,
        rio: Option<Rio>,
        headers: &InMemoryIndex,
        meta: Vec<u8>,
        recreate_index_file: bool,
    ) -> Result<Self>;
    fn file_size(&self) -> u64;
    fn records_count(&self) -> usize;
    async fn read_meta(&self) -> Result<Vec<u8>>;
    async fn read_meta_at(&self, i: u64) -> Result<u8>;
    async fn find_by_key(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>>;
    async fn get_records_headers(&self) -> Result<(InMemoryIndex, usize)>;
    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>>;
    fn validate(&self) -> Result<()>;
}

#[async_trait::async_trait]
impl<FileIndex: FileIndexTrait> BloomDataProvider for IndexStruct<FileIndex> {
    async fn read_byte(&self, index: u64) -> Result<u8> {
        match &self.inner {
            State::OnDisk(findex) => findex.read_meta_at(index).await,
            _ => Err(anyhow::anyhow!("Can't read from in-memory index")),
        }
    }
}
