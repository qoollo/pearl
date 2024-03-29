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
    filter: CombinedFilter<K>,
    bloom_offset: Option<u64>,
    params: IndexParams,
    inner: State<FileIndex, K>,
    name: FileName,
    iodriver: IoDriver,
}

#[derive(Debug, Default)] // Default can be used to initialize structure with 0
struct MemoryAttrs<K> {
    records_count: usize,
    records_allocated: usize,
    marker: PhantomData<K>,
}

const BTREE_B_FACTOR: usize = 6;
const BTREE_VALUES_LEN: usize = BTREE_B_FACTOR * 2 - 1;
const BTREE_EDGES_LEN: usize = BTREE_B_FACTOR * 2;

impl<K> MemoryAttrs<K>
where
    for<'a> K: Key<'a>,
{
    const BTREE_ENTRY_SIZE: usize = K::MEM_SIZE + size_of::<Vec<RecordHeader>>();
    const RECORD_HEADER_SIZE: usize = size_of::<RecordHeader>() + K::LEN as usize;
    // Each node in BTreeMap contains preallocated vectors of 11 values and 12 edges.
    // Although count of nodes can't be determined without reimplementing insertion algorithm,
    // we can use approximation of overhead size added per one key
    const BTREE_DATA_NODE_SIZE: usize = size_of::<Option<std::ptr::NonNull<()>>>() +                     // ptr to parent
                                        size_of::<u16>() * 2 +                                           // metadata
                                        MemoryAttrs::<K>::BTREE_ENTRY_SIZE * BTREE_VALUES_LEN;           // data
    const BTREE_DATA_NODE_RATIO: f64 = 1.0 / BTREE_VALUES_LEN as f64;
    const BTREE_INTERNAL_NODE_OVERHEAD: usize = size_of::<std::ptr::NonNull<()>>() * BTREE_EDGES_LEN;    // edges
    const BTREE_INTERNAL_NODE_RATIO: f64 = (1 +
                                            BTREE_EDGES_LEN +
                                            BTREE_EDGES_LEN.pow(2) +
                                            BTREE_EDGES_LEN.pow(3) +
                                            BTREE_EDGES_LEN.pow(4)) as f64
                                           / (BTREE_VALUES_LEN * BTREE_EDGES_LEN.pow(5)) as f64;
    const BTREE_SIZE_MULTIPLIER: f64 =
        (MemoryAttrs::<K>::BTREE_DATA_NODE_SIZE as f64 * MemoryAttrs::<K>::BTREE_DATA_NODE_RATIO) +
        (MemoryAttrs::<K>::BTREE_INTERNAL_NODE_OVERHEAD as f64 * MemoryAttrs::<K>::BTREE_INTERNAL_NODE_RATIO);
}

pub type InMemoryIndex<K> = BTreeMap<K, Vec<RecordHeader>>;

#[derive(Debug, Default)]
pub(crate) struct InMemoryData<K> {
    headers: InMemoryIndex<K>,
    mem: MemoryAttrs<K>,
}

impl<K> InMemoryData<K>
where
    for<'a> K: Key<'a>,
{
    fn new(headers: InMemoryIndex<K>, count: usize) -> Self {
        let mem = MemoryAttrs {
            records_allocated: headers.values().fold(0, |acc, v| acc + v.capacity()),
            records_count: count,
            marker: PhantomData,
        };

        Self { headers, mem }
    }

    fn memory_used(&self) -> usize {
        let Self { mem, .. } = &self;
        let MemoryAttrs {
            records_count,
            records_allocated,
            ..
        } = &mem;
        let len = self.headers.len();
        trace!("len: {}, records_allocated: {}, records_count: {}",
                len, records_allocated, records_count);
        // last minus is neccessary, because allocated but not initialized record
        // headers don't have key allocated on heap
        MemoryAttrs::<K>::RECORD_HEADER_SIZE * records_allocated
            + (len as f64 * MemoryAttrs::<K>::BTREE_SIZE_MULTIPLIER) as usize
            - (records_allocated - records_count) * K::LEN as usize
    }

    fn records_count(&self) -> usize {
        self.mem.records_count
    }

    fn register_record_allocation(&mut self, records_allocated: usize) {
        self.mem.records_allocated += records_allocated;
        self.mem.records_count += 1;
    }
}

#[derive(Debug)]
pub(crate) enum State<FileIndex, K> {
    InMemory(SRwLock<InMemoryData<K>>),
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
        Self {
            params,
            filter: CombinedFilter::new(bloom_filter, RangeFilter::new()),
            bloom_offset: None,
            inner: State::InMemory(SRwLock::default()),
            name,
            iodriver,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inner = State::InMemory(SRwLock::default());
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

    /// Fast check for key presence. None - can't perform fast check (disk access required)
    pub(crate) fn contains_key_fast(&self, key: &K) -> Option<bool> {
        match &self.inner {
            State::InMemory(index) => Some(index.read().expect("read lock acquired").headers.contains_key(key)),
            State::OnDisk(_) => None
        }
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
        let meta_buf = findex.read_meta().await.map_err(|err| err.into_bincode_if_unexpected_eof())?;
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
                std::mem::take(&mut *headers).headers
            };
            if headers.len() == 0 {
                return Ok(0);
            }
            debug!("blob index simple in memory headers {}", headers.len());
            let (meta_buf, bloom_offset) = self.serialize_filters()?;
            self.bloom_offset = Some(bloom_offset as u64);
            let findex = FileIndex::from_records(
                self.name.as_path(),
                self.iodriver.clone(),
                &headers,
                meta_buf,
                self.params.recreate_file,
                blob_size,
            )
            .await?;
            let size = findex.file_size() as usize;
            self.inner = State::OnDisk(findex);
            return Ok(size);
        }
        Ok(0)
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
        self.inner = State::InMemory(SRwLock::new(InMemoryData::new(record_headers, records_count)));
        let meta_buf = findex.read_meta().await.map_err(|err| err.into_bincode_if_unexpected_eof())?;
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
        match &self.inner {
            State::InMemory(data) => data.read().expect("rwlock").memory_used(),
            State::OnDisk(file) => file.memory_used(),
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
        self.get_latest(key)
            .await
            .map(|h| h.map(|h| BlobRecordTimestamp::new(h.timestamp())))
    }

    fn push(&self, key: &K, h: RecordHeader) -> Result<()> {
        debug!("blob index simple push");
        match &self.inner {
            State::InMemory(headers) => {
                let mut data = headers.write().expect("rwlock");
                debug!("blob index simple push bloom filter add");
                self.filter.add(key);
                debug!("blob index simple push key: {:?}", h.key());
                let records_allocated;
                if let Some(v) = data.headers.get_mut(key) {
                    let old_capacity = v.capacity();
                    // Keep ordered by timestamp
                    let mut pos = 0;
                    if v.len() > 4 {
                        // Use binary search when len > 4. For smaller len sequential search will be faster
                        pos = v.binary_search_by(|item| item.timestamp().cmp(&h.timestamp())).unwrap_or_else(|e| e);
                    }
                    // Skip records with timestamp less or equal to our (our should be the latest)
                    while pos < v.len() && v[pos].timestamp() <= h.timestamp() {
                        pos += 1;
                    }
                    v.insert(pos, h);
                    trace!("capacity growth: {}", v.capacity() - old_capacity);
                    records_allocated = v.capacity() - old_capacity;
                } else {
                    let v = vec![h];
                    records_allocated = v.capacity(); // capacity == 1
                    data.headers.insert(key.clone(), v);
                }
                data.register_record_allocation(records_allocated);
                Ok(())
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
            State::InMemory(data) => {
                let data = data.read().expect("rwlock");
                Ok(data.headers.get(key).cloned().map(|mut hs| {
                    if hs.len() > 1 {
                        hs.reverse();
                    }
                    hs
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

    async fn get_latest(&self, key: &K) -> Result<ReadResult<RecordHeader>> {
        debug!("index get any");
        let result = match &self.inner {
            State::InMemory(headers) => {
                let data = headers.read().expect("rwlock");
                debug!("index get any in memory headers: {}", data.headers.len());
                // in memory indexes with same key are stored in ascending order, so the last
                // by adding time record is last in list (in b+tree disk index it's first)
                data.headers.get(key).and_then(|h| h.last()).cloned()
            }
            State::OnDisk(findex) => {
                debug!("index get any on disk");
                findex.get_latest(key).await?
            }
        };
        Ok(match result {
            Some(header) if header.is_deleted() => {
                ReadResult::Deleted(BlobRecordTimestamp::new(header.timestamp()))
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
        match &self.inner {
            State::OnDisk(ref findex) => findex.records_count(),
            State::InMemory(d) => d.read().expect("rwlock").records_count(),
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
    async fn get_latest(&self, key: &K) -> Result<Option<RecordHeader>>;
    fn validate(&self, blob_size: u64) -> Result<()>;
    fn memory_used(&self) -> usize;
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
