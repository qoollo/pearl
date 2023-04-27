use bytes::BytesMut;

use super::prelude::*;
use crate::filter::{BloomDataProvider, CombinedFilter, FilterTrait};
use std::mem::size_of;

pub(crate) type Index<K> = IndexStruct<BPTreeFileIndex<K>, K>;

pub(crate) const HEADER_VERSION: u8 = 6;
pub(crate) const INDEX_HEADER_MAGIC_BYTE: u64 = 0xacdc_bcde;

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
pub(crate) struct IndexStruct<FileIndex, K>
where
    for<'a> K: Key<'a>,
{
    mem: Option<MemoryAttrs>,
    filter: CombinedFilter<K>,
    bloom_offset: Option<u64>,
    params: IndexParams,
    inner: State<FileIndex, K>,
    name: FileName,
    iodriver: IoDriver,
}

#[derive(Debug, Default)] // Default can be used to initialize structure with 0
pub(crate) struct MemoryAttrs {
    pub(crate) key_size: AtomicUsize,
    pub(crate) btree_entry_size: AtomicUsize,
    // contains actual size occupied by record header in RAM (which helps
    // to compute actual size of indices in RAM in `InMemory` state)
    pub(crate) record_header_size: AtomicUsize,
    pub(crate) records_count: AtomicUsize,
    pub(crate) records_allocated: AtomicUsize,
}

pub type InMemoryIndex<K> = BTreeMap<K, Vec<RecordHeader>>;

#[derive(Debug, Clone)]
pub(crate) enum State<FileIndex, K> {
    InMemory(Arc<std::sync::RwLock<Option<InMemoryIndex<K>>>>),
    OnDisk(FileIndex),
}

impl<FileIndex, K> IndexStruct<FileIndex, K>
where
    FileIndex: FileIndexTrait<K>,
    for<'a> K: Key<'a>,
{
    pub(crate) fn new(name: FileName, iodriver: IoDriver, config: IndexConfig) -> Self {
        let params = IndexParams::new(config.bloom_config.is_some(), config.recreate_index_file);
        let bloom_filter = config.bloom_config.map(|cfg| Bloom::new(cfg));
        let mem = Some(Default::default());
        Self {
            params,
            filter: CombinedFilter::new(bloom_filter, RangeFilter::new()),
            bloom_offset: None,
            inner: State::InMemory(Arc::new(std::sync::RwLock::new(Some(BTreeMap::new())))),
            mem,
            name,
            iodriver,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inner = State::InMemory(Arc::new(std::sync::RwLock::new(Some(BTreeMap::new()))));
        self.mem = Some(Default::default());
        self.filter.clear_filter();
    }

    pub fn offload_filter(&mut self) -> usize {
        if self.on_disk() {
            self.filter.offload_filter()
        } else {
            0
        }
    }

    pub fn get_filter(&self) -> &CombinedFilter<K> {
        &self.filter
    }

    pub(crate) fn name(&self) -> &FileName {
        &self.name
    }

    pub(crate) async fn from_file(
        name: FileName,
        config: IndexConfig,
        iodriver: IoDriver,
        blob_size: u64,
    ) -> Result<Self> {
        let findex = FileIndex::from_file(name.clone(), iodriver.clone()).await?;
        findex
            .validate(blob_size)
            .with_context(|| "Header is corrupt")?;
        let meta_buf = findex.read_meta().await?;
        let (bloom_filter, range_filter, bloom_offset) = Self::deserialize_filters(&meta_buf)?;
        let params = IndexParams::new(config.bloom_config.is_some(), config.recreate_index_file);
        let bloom_filter = if params.bloom_is_on {
            Some(bloom_filter)
        } else {
            None
        };
        trace!("index restored successfuly");
        let index = Self {
            inner: State::OnDisk(findex),
            mem: None,
            name,
            filter: CombinedFilter::new(bloom_filter, range_filter),
            bloom_offset: Some(bloom_offset as u64),
            params,
            iodriver,
        };
        Ok(index)
    }

    pub(crate) fn on_disk(&self) -> bool {
        matches!(&self.inner, State::OnDisk(_))
    }

    async fn dump_in_memory(&mut self, blob_size: u64) -> Result<usize> {
        if let State::InMemory(headers) = &self.inner {
            let headers = {
                let mut headers = headers.write().expect("rwlock");
                headers.take()
            };
            if let Some(headers) = headers {
                if headers.len() == 0 {
                    return Ok(0);
                }
                debug!("blob index simple in memory headers {}", headers.len());
                let (meta_buf, bloom_offset) = self.serialize_filters()?;
                self.bloom_offset = Some(bloom_offset as u64);
                let findex = FileIndex::from_records(
                    &self.name.to_path(),
                    self.iodriver.clone(),
                    &headers,
                    meta_buf,
                    self.params.recreate_file,
                    blob_size,
                )
                .await?;
                let size = findex.file_size() as usize;
                self.inner = State::OnDisk(findex);
                self.mem = None;
                Ok(size)
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    fn serialize_filters(&self) -> Result<(Vec<u8>, usize)> {
        let range_buf = self.filter.range().to_raw()?;
        let range_buf_size = range_buf.len() as u64;
        let bloom_buf = self
            .filter
            .bloom()
            .as_ref()
            .unwrap_or(&Bloom::empty())
            .to_raw()?;
        let mut buf = Vec::with_capacity(size_of::<u64>() + range_buf.len() + bloom_buf.len());
        let bloom_offset = size_of::<u64>() + range_buf.len();
        buf.extend_from_slice(&serialize(&range_buf_size)?);
        buf.extend_from_slice(&range_buf);
        buf.extend_from_slice(&bloom_buf);
        Ok((buf, bloom_offset))
    }

    fn deserialize_filters(buf: &[u8]) -> Result<(Bloom, RangeFilter<K>, usize)> {
        let (range_size_buf, rest_buf) = buf.split_at(size_of::<u64>());
        let range_size = deserialize(&range_size_buf)?;
        let (range_buf, bloom_buf) = rest_buf.split_at(range_size);
        let bloom = Bloom::from_raw(bloom_buf)?;
        let range = RangeFilter::<K>::from_raw(range_buf)?;
        Ok((bloom, range, range_size + size_of::<u64>()))
    }

    async fn load_in_memory(&mut self, findex: FileIndex, blob_size: u64) -> Result<()> {
        let (record_headers, records_count) = findex.get_records_headers(blob_size).await?;
        self.mem = Some(compute_mem_attrs(&record_headers, records_count));
        self.inner = State::InMemory(Arc::new(std::sync::RwLock::new(Some(record_headers))));
        let meta_buf = findex.read_meta().await?;
        let (bloom_filter, range_filter, _) = Self::deserialize_filters(&meta_buf)?;
        let bloom_filter = if self.params.bloom_is_on {
            Some(bloom_filter)
        } else {
            None
        };
        self.filter = CombinedFilter::new(bloom_filter, range_filter);
        self.bloom_offset = None;
        Ok(())
    }

    pub(crate) fn memory_used(&self) -> usize {
        if let State::InMemory(data) = &self.inner {
            let data = data.read().expect("rwloc");
            if let Some(data) = data.as_ref() {
                let mem = self
                    .mem
                    .as_ref()
                    .expect("No memory info in `InMemory` State");
                let record_header_size = mem.record_header_size.load(Ordering::Acquire);
                let records_allocated = mem.records_allocated.load(Ordering::Acquire);
                let records_count = mem.records_count.load(Ordering::Acquire);
                let btree_entry_size = mem.btree_entry_size.load(Ordering::Acquire);
                let key_size = mem.key_size.load(Ordering::Acquire);
                trace!("record_header_size: {}, records_allocated: {}, data.len(): {}, entry_size (key + vec): {}",
                record_header_size, records_allocated, data.len(), btree_entry_size
                );
                // last minus is neccessary, because allocated but not initialized record headers don't
                // have key allocated on heap
                record_header_size * records_allocated + data.len() * btree_entry_size
                    - (records_allocated - records_count) * key_size
            } else {
                0
            }
        } else {
            0
        }
    }

    pub(crate) fn disk_used(&self) -> u64 {
        if let State::OnDisk(file) = &self.inner {
            file.file_size()
        } else {
            0
        }
    }
}

#[async_trait::async_trait]
impl<FileIndex, K> IndexTrait<K> for IndexStruct<FileIndex, K>
where
    FileIndex: FileIndexTrait<K> + Clone,
    for<'a> K: Key<'a>,
{
    async fn contains_key(&self, key: &K) -> Result<ReadResult<BlobRecordTimestamp>> {
        self.get_any(key)
            .await
            .map(|h| h.map(|h| BlobRecordTimestamp::new(h.created())))
    }

    fn push(&self, key: &K, h: RecordHeader) -> Result<()> {
        debug!("blob index simple push");
        match &self.inner {
            State::InMemory(headers) => {
                let mut headers = headers.write().expect("rwlock");
                if let Some(headers) = headers.as_mut() {
                    debug!("blob index simple push bloom filter add");
                    self.filter.add(key);
                    debug!("blob index simple push key: {:?}", h.key());
                    let mem = self
                        .mem
                        .as_ref()
                        .expect("No memory info in `InMemory` State");
                    if let Some(v) = headers.get_mut(key) {
                        let old_capacity = v.capacity();
                        v.push(h);
                        trace!("capacity growth: {}", v.capacity() - old_capacity);
                        mem.records_allocated
                            .fetch_add(v.capacity() - old_capacity, Ordering::Release);
                    } else {
                        if mem.records_count.load(Ordering::Acquire) == 0 {
                            set_key_related_fields::<K>(mem);
                        }
                        let v = vec![h];
                        mem.records_allocated
                            .fetch_add(v.capacity(), Ordering::Release); // capacity == 1
                        headers.insert(key.clone(), v);
                    }
                    mem.records_count.fetch_add(1, Ordering::Release);
                    Ok(())
                } else {
                    Err(Error::from(ErrorKind::Index(
                        "Index is in the process of closing, push is unavailable".to_string(),
                    ))
                    .into())
                }
            }
            State::OnDisk(_) => Err(Error::from(ErrorKind::Index(
                "Index is closed, push is unavalaible".to_string(),
            ))
            .into()),
        }
    }

    async fn get_all(&self, key: &K) -> Result<Vec<RecordHeader>> {
        let mut with_deletion = self.get_all_with_deletion_marker(key).await?;
        if let Some(h) = with_deletion.last() {
            if h.is_deleted() {
                with_deletion.truncate(with_deletion.len() - 1);
            }
        }
        Ok(with_deletion)
    }

    async fn get_all_with_deletion_marker(&self, key: &K) -> Result<Vec<RecordHeader>> {
        let headers = match &self.inner {
            State::InMemory(headers) => {
                Ok(headers.read().expect("rwlock").as_ref().and_then(|h| {
                    h.get(key).cloned().map(|mut hs| {
                        if hs.len() > 1 {
                            hs.reverse();
                        }
                        hs
                    })
                }))
            }
            State::OnDisk(findex) => findex.find_by_key(key).await,
        }?;
        if let Some(mut hs) = headers {
            let first_del = hs.iter().position(|h| h.is_deleted());
            if let Some(first_del) = first_del {
                hs.truncate(first_del + 1);
            }
            Ok(hs)
        } else {
            Ok(vec![])
        }
    }

    async fn get_any(&self, key: &K) -> Result<ReadResult<RecordHeader>> {
        debug!("index get any");
        let result = match &self.inner {
            State::InMemory(headers) => {
                let headers = headers.read().expect("rwlock");
                headers.as_ref().and_then(|headers| {
                    debug!("index get any in memory headers: {}", headers.len());
                    // in memory indexes with same key are stored in ascending order, so the last
                    // by adding time record is last in list (in b+tree disk index it's first)
                    headers.get(key).and_then(|h| h.last()).cloned()
                })
            }
            State::OnDisk(findex) => {
                debug!("index get any on disk");
                findex.get_any(key).await?
            }
        };
        Ok(match result {
            Some(header) if header.is_deleted() => {
                ReadResult::Deleted(BlobRecordTimestamp::new(header.created()))
            }
            Some(header) => ReadResult::Found(header),
            None => ReadResult::NotFound,
        })
    }

    async fn dump(&mut self, blob_size: u64) -> Result<usize> {
        self.dump_in_memory(blob_size).await
    }

    async fn load(&mut self, blob_size: u64) -> Result<()> {
        match &self.inner {
            State::InMemory(_) => Ok(()),
            State::OnDisk(findex) => {
                let findex = findex.clone();
                self.load_in_memory(findex, blob_size).await
            }
        }
    }

    fn count(&self) -> usize {
        match self.inner {
            State::OnDisk(ref findex) => findex.records_count(),
            State::InMemory(_) => self
                .mem
                .as_ref()
                .map(|mem| mem.records_count.load(Ordering::Acquire))
                .expect("No memory info in `InMemory` State"),
        }
    }

    fn push_deletion(&mut self, key: &K, header: RecordHeader) -> Result<()> {
        debug!("mark all as deleted by {:?} key", key);
        assert!(header.is_deleted());
        assert!(header.data_size() == 0);
        self.push(key, header)
    }
}

#[async_trait::async_trait]
pub(crate) trait FileIndexTrait<K>: Sized + Send + Sync {
    async fn from_file(name: FileName, iodriver: IoDriver) -> Result<Self>;
    async fn from_records(
        path: &Path,
        iodriver: IoDriver,
        headers: &InMemoryIndex<K>,
        meta: Vec<u8>,
        recreate_index_file: bool,
        blob_size: u64,
    ) -> Result<Self>;
    fn file_size(&self) -> u64;
    fn records_count(&self) -> usize;
    fn blob_size(&self) -> u64;
    async fn read_meta(&self) -> Result<BytesMut>;
    async fn read_meta_at(&self, i: u64) -> Result<u8>;
    async fn find_by_key(&self, key: &K) -> Result<Option<Vec<RecordHeader>>>;
    async fn get_records_headers(&self, blob_size: u64) -> Result<(InMemoryIndex<K>, usize)>;
    async fn get_any(&self, key: &K) -> Result<Option<RecordHeader>>;
    fn validate(&self, blob_size: u64) -> Result<()>;
}

#[async_trait::async_trait]
impl<FileIndex, K> BloomDataProvider for IndexStruct<FileIndex, K>
where
    FileIndex: FileIndexTrait<K>,
    for<'a> K: Key<'a>,
{
    async fn read_byte(&self, index: u64) -> Result<u8> {
        match &self.inner {
            State::OnDisk(findex) => {
                findex
                    .read_meta_at(index + self.bloom_offset.expect("should be set after dump"))
                    .await
            }
            _ => Err(anyhow::anyhow!("Can't read from in-memory index")),
        }
    }
}
