/// structure of b+-tree index file from the beginning:
/// 1. Header
/// 2. Bloom filter
/// 3. Sorted by keys array of record headers (serialized size of one record header is 61 bytes)
/// 4. Metadata
/// 5. Sorted by keys array of pairs `key - offset of record header in file` (these are leaf nodes
///    of our B+ tree; as opposed to using record headers as leaves, this approach allows to include
///    more elements in one block and reducing height of the tree (but increasing on one blocks
///    that we need to read); this also will be useful for scanning queries: instead of scanning
///    61 bytes headers (in case of 4-bytes key) we scan 8 bytes pairs (in case 4-bytes key and
///    4-bytes offset) which allows to read less pages in almost 8 times)
/// 6. Non-leaf nodes of btree in format
///    `childrens_amount - pointer - key - pointer - key - ... - pointer`,
///    where pointer - is offset in file on underlying node with searched key
use super::prelude::*;

const BLOCK_SIZE: usize = 512;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TreeMeta {
    root_offset: u64,
    leaves_offset: u64,
}

impl TreeMeta {
    fn new(root_offset: u64, leaves_offset: u64) -> Self {
        Self {
            root_offset,
            leaves_offset,
        }
    }

    pub(crate) fn serialized_size_default() -> bincode::Result<u64> {
        let meta = Self::default();
        meta.serialized_size()
    }

    #[inline]
    pub fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    #[inline]
    pub(crate) fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(buf)
    }
}

struct NodeMeta {
    childrens_amount: u64,
}

impl NodeMeta {
    fn new(childrens_amount: u64) -> Self {
        Self { childrens_amount }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BPTreeFileIndex {
    file: File,
    header: IndexHeader,
    metadata: TreeMeta,
}

#[async_trait::async_trait]
impl FileIndexTrait for BPTreeFileIndex {
    async fn from_file(name: FileName, ioring: Option<Rio>) -> Result<Self> {
        trace!("open index file");
        let file = File::open(name.to_path(), ioring)
            .await
            .context(format!("failed to open index file: {}", name))?;
        let header = Self::read_index_header(&file).await?;
        let metadata = Self::read_tree_meta(&file, &header).await?;

        Ok(Self {
            file,
            header,
            metadata,
        })
    }

    async fn from_records(
        path: &Path,
        ioring: Option<Rio>,
        headers: &InMemoryIndex,
        filter: &Bloom,
    ) -> Result<Self> {
        let res = Self::serialize(headers, filter)?;
        if res.is_none() {
            error!("Indices are empty!");
            return Err(anyhow!("empty in-memory indices".to_string()));
        }
        let (mut header, metadata, buf) = res.expect("None case is checked");
        let _ = std::fs::remove_file(path);
        let file = File::create(path, ioring)
            .await
            .with_context(|| format!("file open failed {:?}", path))?;
        file.write_append(&buf).await?;
        header.written = 1;
        let serialized_header = serialize(&header)?;
        file.write_at(0, &serialized_header).await?;
        file.fsyncdata().await?;
        Ok(Self {
            file,
            metadata,
            header,
        })
    }

    fn file_size(&self) -> u64 {
        self.file.size()
    }

    fn records_count(&self) -> usize {
        self.header.records_count
    }

    async fn read_filter(&self) -> Result<Bloom> {
        trace!("load filter");
        let mut buf = vec![0; self.header.filter_buf_size];
        trace!("read filter into buf: [0; {}]", buf.len());
        self.file
            .read_at(&mut buf, self.header.serialized_size()? as u64)
            .await?;
        Bloom::from_raw(&buf)
    }

    // TODO: implement
    async fn find_by_key(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>> {
        Ok(None)
    }

    // TODO: implement
    async fn get_records_headers(&self) -> Result<(InMemoryIndex, usize)> {
        Err(Error::not_found().into())
    }

    // TODO: implement
    async fn get_any(&self, key: &[u8]) -> Result<Option<(RecordHeader, usize)>> {
        Ok(None)
    }

    fn get_index_header(&self) -> &IndexHeader {
        &self.header
    }
}

impl BPTreeFileIndex {
    async fn read_index_header(file: &File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default()? as usize;
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    async fn read_tree_meta(file: &File, header: &IndexHeader) -> Result<TreeMeta> {
        let meta_size = TreeMeta::serialized_size_default()? as usize;
        let mut buf = vec![0; meta_size];
        file.read_at(&mut buf, 0).await?;
        TreeMeta::from_raw(&buf).map_err(Into::into)
    }

    fn serialize(
        headers_btree: &InMemoryIndex,
        filter: &Bloom,
    ) -> Result<Option<(IndexHeader, TreeMeta, Vec<u8>)>> {
        debug!("blob index bptree serialize headers");
        if let Some(record_header) = headers_btree.values().next().and_then(|v| v.first()) {
            debug!("index bptree serialize headers first: {:?}", record_header);
            let record_header_size = record_header.serialized_size().try_into()?;
            trace!("record header serialized size: {}", record_header_size);
            let filter_buf = filter.to_raw()?;
            let headers = headers_btree.iter().flat_map(|r| r.1).collect::<Vec<_>>();
            let header = IndexHeader::new(record_header_size, headers.len(), filter_buf.len());
            let header_buf = serialize(&header)?;
            let hs = header_buf.len() as usize;
            trace!("index header size: {}b", hs);
            let fsize = header.filter_buf_size;
            let msize: usize = TreeMeta::serialized_size_default()?
                .try_into()
                .expect("u64 to usize");
            let headers_start_offset = (hs + fsize) as u64;
            let headers_size = headers.len() * record_header_size;
            let leaves_offset = headers_start_offset + headers_size as u64;
            // leaf contains pair: (key, offset in file for first record's header with this key)
            let leaf_size = record_header.key().len() + std::mem::size_of::<u64>();
            let leaves_buf = Self::serialize_leaves(
                headers_btree,
                headers_start_offset,
                leaf_size,
                record_header_size,
            );
            let keys = headers_btree.keys().collect();
            let (root_offset, tree_buf) = Self::serialize_bptree(keys, leaves_offset);
            let metadata = TreeMeta::new(root_offset, leaves_offset);
            let meta_buf = serialize(&metadata)?;

            let data_size = hs + fsize + headers_size + msize + leaves_buf.len() + tree_buf.len();
            let mut buf = Vec::with_capacity(data_size);
            serialize_into(&mut buf, &header)?;
            buf.extend_from_slice(&filter_buf);
            headers
                .iter()
                .filter_map(|h| serialize(&h).ok())
                .fold(&mut buf, |buf, h_buf| {
                    buf.extend_from_slice(&h_buf);
                    buf
                });
            buf.extend_from_slice(&meta_buf);
            buf.extend_from_slice(&leaves_buf);
            buf.extend_from_slice(&tree_buf);
            let hash = get_hash(&buf);
            let header =
                IndexHeader::with_hash(record_header_size, headers.len(), filter_buf.len(), hash);
            serialize_into(buf.as_mut_slice(), &header)?;
            Ok(Some((header, metadata, buf)))
        } else {
            Ok(None)
        }
    }

    // TODO: implement
    fn serialize_bptree(keys: Vec<&Vec<u8>>, leaves_offset: u64) -> (u64, Vec<u8>) {
        (0, Vec::new())
    }

    fn serialize_leaves(
        headers_btree: &InMemoryIndex,
        headers_start_offset: u64,
        leaf_size: usize,
        record_header_size: usize,
    ) -> Vec<u8> {
        let mut leaves_buf = Vec::with_capacity(leaf_size * headers_btree.len());
        headers_btree.iter().fold(
            (headers_start_offset, &mut leaves_buf),
            |(offset, leaves_buf), (leaf, hds)| {
                let leafb = serialize(&leaf).expect("serialize u64");
                leaves_buf.extend_from_slice(&leafb);
                let offsetb = serialize(&offset).expect("serialize u64");
                leaves_buf.extend_from_slice(&offsetb);
                (offset + (hds.len() * record_header_size) as u64, leaves_buf)
            },
        );
        leaves_buf
    }
}
