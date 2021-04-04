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
///    `NodeMeta - pointer - key - pointer - key - ... - pointer`,
///    where pointer - is offset in file on underlying node with searched key
use super::prelude::*;
use std::cmp::min;

const BLOCK_SIZE: usize = 4096;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TreeMeta {
    root_offset: u64,
    leaves_offset: u64,
    tree_offset: u64,
}

impl TreeMeta {
    fn new(root_offset: u64, leaves_offset: u64, tree_offset: u64) -> Self {
        Self {
            root_offset,
            leaves_offset,
            tree_offset,
        }
    }

    fn serialized_size_default() -> bincode::Result<u64> {
        let meta = Self::default();
        meta.serialized_size()
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

#[derive(Debug, Serialize, Deserialize, Default)]
struct NodeMeta {
    size: u64,
}

impl NodeMeta {
    fn new(size: u64) -> Self {
        Self { size }
    }

    #[inline]
    fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    fn serialized_size_default() -> bincode::Result<u64> {
        let meta = Self::default();
        meta.serialized_size()
    }
}

#[derive(Debug)]
struct Node {
    keys: Vec<Vec<u8>>,
    offsets: Vec<u64>,
}

impl Node {
    fn new(keys: Vec<Vec<u8>>, offsets: Vec<u64>) -> Self {
        Self { keys, offsets }
    }

    fn key_offset(&self, key: &[u8]) -> u64 {
        let mut left = 0i32;
        let mut right = self.keys.len() as i32 - 1;
        while left <= right {
            let mid = (left + right) / 2;
            match key.cmp(&self.keys[mid as usize]) {
                CmpOrdering::Equal => return mid as u64 + 1,
                CmpOrdering::Greater => left = mid + 1,
                CmpOrdering::Less => right = mid - 1,
            }
        }
        if left == 0 || key.cmp(&self.keys[left as usize - 1]) != CmpOrdering::Less {
            self.offsets[left as usize] as u64
        } else {
            self.offsets[(left + 1) as usize] as u64
        }
    }
}

fn serialize_node(node: &Node) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let meta = NodeMeta::new(node.keys.len() as u64);
    buf.extend_from_slice(&serialize(&meta)?);
    node.keys.iter().fold(&mut buf, |acc, key| {
        acc.extend_from_slice(&key);
        acc
    });
    node.offsets.iter().fold(&mut buf, |acc, offset| {
        // FIXME: to result
        acc.extend_from_slice(&serialize(offset).expect("u64 to bytes"));
        acc
    });
    Ok(buf)
}

fn deserialize_node(buf: &[u8], key_size: u64) -> Result<Node> {
    let meta_size = NodeMeta::serialized_size_default()?;
    let (meta_buf, data_buf) = buf.split_at(meta_size as usize);
    let meta: NodeMeta = deserialize(&meta_buf)?;
    let keys_buf_size = meta.size * key_size;
    let (keys_buf, rest_buf) = data_buf.split_at(keys_buf_size as usize);
    let keys = keys_buf
        .chunks(key_size as usize)
        .map(|key| key.to_vec())
        .collect();
    let offsets_buf_size = (meta.size + 1) as usize * std::mem::size_of::<u64>();
    let (offsets_buf, _rest_buf) = rest_buf.split_at(offsets_buf_size);
    let offsets = offsets_buf
        .chunks(std::mem::size_of::<u64>())
        // FIXME: to result
        .map(|bytes| deserialize::<u64>(bytes).expect("bytes to u64"))
        .collect();
    Ok(Node::new(keys, offsets))
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
        let root_offset = self.metadata.root_offset;
        let mut buf = [0u8; BLOCK_SIZE];
        let leaf_offset = self.find_leaf_node(key, root_offset, &mut buf).await?;
        if let Some((fh_offset, amount)) =
            self.find_first_header(leaf_offset, key, &mut buf).await?
        {
            let headers = self.read_headers(fh_offset, amount as usize).await?;
            Ok(Some(headers))
        } else {
            Ok(None)
        }
    }

    async fn get_records_headers(&self) -> Result<(InMemoryIndex, usize)> {
        let mut buf = self.file.read_all().await?;
        self.validate_header(&mut buf).await?;
        let offset = self.header.filter_buf_size + self.header.serialized_size()? as usize;
        let records_end = self.metadata.leaves_offset as usize;
        let records_buf = &buf[offset..records_end];
        (0..self.header.records_count)
            .try_fold(InMemoryIndex::new(), |mut headers, i| {
                let offset = i * self.header.record_header_size;
                let header: RecordHeader = deserialize(&records_buf[offset..])?;
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
            .map(|headers| (headers, self.header.records_count))
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        let root_offset = self.metadata.root_offset;
        let mut buf = [0u8; BLOCK_SIZE];
        let leaf_offset = self.find_leaf_node(key, root_offset, &mut buf).await?;
        if let Some((first_header_offset, _amount)) =
            self.find_first_header(leaf_offset, key, &mut buf).await?
        {
            let header = self.read_header(first_header_offset).await?;
            Ok(Some(header))
        } else {
            Ok(None)
        }
    }

    fn get_index_header(&self) -> &IndexHeader {
        &self.header
    }
}

impl BPTreeFileIndex {
    async fn find_leaf_node(&self, key: &[u8], mut offset: u64, buf: &mut [u8]) -> Result<u64> {
        let key_size = key.len() as u64;
        while offset >= self.metadata.tree_offset {
            self.file.read_at(buf, offset).await?;
            let node = deserialize_node(buf, key_size)?;
            offset = node.key_offset(&key);
        }
        Ok(offset)
    }

    async fn find_first_header(
        &self,
        leaf_offset: u64,
        key: &[u8],
        buf: &mut [u8],
    ) -> Result<Option<(u64, u64)>> {
        let leaf_size = key.len() + std::mem::size_of::<u64>();
        let buf_size = BLOCK_SIZE - (BLOCK_SIZE % leaf_size);
        let buf_size = min(buf_size, (self.metadata.tree_offset - leaf_offset) as usize);
        let (buf, _rest) = buf.split_at_mut(buf_size as usize);
        self.file.read_at(buf, leaf_offset).await?;
        let header_pointers: Vec<(Vec<u8>, u64)> = buf
            .chunks(leaf_size)
            .map(|bytes| {
                let (key, offset) = bytes.split_at(key.len());
                // FIXME: remove expect
                (
                    key.to_vec(),
                    deserialize::<u64>(offset).expect("deserialize u64"),
                )
            })
            .collect();
        let mut left = 0;
        let mut right = header_pointers.len() as i32 - 1;
        while left <= right {
            let mid = (left + right) / 2;
            let cur_key = &header_pointers[mid as usize].0;
            match key.cmp(cur_key) {
                CmpOrdering::Equal => {
                    let val = self.with_amount(
                        header_pointers,
                        mid as usize,
                        leaf_offset + buf_size as u64,
                    );
                    return Ok(Some(val));
                }
                CmpOrdering::Less => right = mid - 1,
                CmpOrdering::Greater => left = mid + 1,
            }
        }
        Ok(None)
    }

    fn with_amount(
        &self,
        header_pointers: Vec<(Vec<u8>, u64)>,
        mid: usize,
        buf_end: u64,
    ) -> (u64, u64) {
        let last_rec = mid >= header_pointers.len() - 2 && (buf_end >= self.metadata.tree_offset);
        let cur_offset = header_pointers[mid].1;
        let next_offset = if last_rec {
            self.metadata.leaves_offset
        } else {
            println!("mid = {}, len = {};", mid, header_pointers.len());
            header_pointers[mid + 1].1
        };
        let amount = (next_offset - cur_offset) / self.header.record_header_size as u64;
        (cur_offset, amount)
    }

    async fn read_header(&self, offset: u64) -> Result<RecordHeader> {
        let mut buf = vec![0u8; self.header.record_header_size];
        self.file.read_at(&mut buf, offset).await?;
        deserialize(&buf).map_err(Into::into)
    }

    async fn read_headers(&self, offset: u64, amount: usize) -> Result<Vec<RecordHeader>> {
        let mut buf = vec![0u8; self.header.record_header_size * amount];
        self.file.read_at(&mut buf, offset).await?;
        let headers = buf.chunks(self.header.record_header_size).fold(
            Vec::with_capacity(amount),
            |mut acc, bytes| {
                acc.push(deserialize(&bytes).expect("deserialize header"));
                acc
            },
        );
        Ok(headers)
    }

    async fn validate_header(&self, buf: &mut Vec<u8>) -> Result<()> {
        if self.header.written != 1 {
            return Err(Error::validation("header was not written").into());
        }
        if !Self::hash_valid(&self.header, buf)? {
            return Err(Error::validation("header hash mismatch").into());
        }
        if self.header.version != HEADER_VERSION {
            return Err(Error::validation("header version mismatch").into());
        }
        Ok(())
    }

    fn hash_valid(header: &IndexHeader, buf: &mut Vec<u8>) -> Result<bool> {
        let hash = header.hash.clone();
        let mut header = header.clone();
        header.hash = vec![0; ring::digest::SHA256.output_len];
        header.written = 0;
        serialize_into(buf.as_mut_slice(), &header)?;
        let new_hash = get_hash(&buf);
        Ok(hash == new_hash)
    }

    async fn read_index_header(file: &File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default()? as usize;
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    async fn read_tree_meta(file: &File, header: &IndexHeader) -> Result<TreeMeta> {
        let meta_size = TreeMeta::serialized_size_default()? as usize;
        let mut buf = vec![0; meta_size];
        let fsize = header.filter_buf_size as u64;
        let hs = header.serialized_size()?;
        let meta_offset = hs + fsize + (header.records_count * header.record_header_size) as u64;
        file.read_at(&mut buf, meta_offset).await?;
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
            let headers_len = headers_btree
                .iter()
                .fold(0, |acc, (_key, value)| acc + value.len());
            let header = IndexHeader::new(record_header_size, headers_len, filter_buf.len());
            let header_buf = serialize(&header)?;
            let hs = header_buf.len() as usize;
            trace!("index header size: {}b", hs);
            let fsize = header.filter_buf_size;
            let msize: usize = TreeMeta::serialized_size_default()?
                .try_into()
                .expect("u64 to usize");
            let headers_start_offset = (hs + fsize) as u64;
            let headers_size = headers_len * record_header_size;
            let leaves_offset = headers_start_offset + headers_size as u64 + msize as u64;
            // leaf contains pair: (key, offset in file for first record's header with this key)
            let leaf_size = record_header.key().len() + std::mem::size_of::<u64>();
            let leaves_buf = Self::serialize_leaves(
                headers_btree,
                headers_start_offset,
                leaf_size,
                record_header_size,
            );
            let keys = headers_btree.keys().collect();
            let tree_offset = leaves_offset + leaves_buf.len() as u64;
            let (root_offset, tree_buf) =
                Self::serialize_bptree(keys, leaves_offset, leaf_size as u64, tree_offset);
            let metadata = TreeMeta::new(root_offset, leaves_offset, tree_offset);
            let meta_buf = serialize(&metadata)?;

            let data_size = hs + fsize + headers_size + msize + leaves_buf.len() + tree_buf.len();
            let mut buf = Vec::with_capacity(data_size);
            serialize_into(&mut buf, &header)?;
            buf.extend_from_slice(&filter_buf);
            headers_btree
                .iter()
                .flat_map(|r| r.1)
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
                IndexHeader::with_hash(record_header_size, headers_len, filter_buf.len(), hash);
            serialize_into(buf.as_mut_slice(), &header)?;
            Ok(Some((header, metadata, buf)))
        } else {
            Ok(None)
        }
    }

    fn serialize_bptree(
        keys: Vec<&Vec<u8>>,
        leaves_offset: u64,
        leaf_size: u64,
        tree_offset: u64,
    ) -> (u64, Vec<u8>) {
        let max_amount = Self::max_leaf_node_capacity(keys[0].len());
        let nodes_amount = (keys.len() - 1) / max_amount + 1;
        let elems_in_node = (keys.len() / nodes_amount) as u64;
        trace!(
            "last node has {} less keys",
            elems_in_node - elems_in_node % nodes_amount as u64
        );

        let leaf_nodes = keys
            .chunks(elems_in_node as usize)
            .enumerate()
            .map(|(i, keys)| {
                let cur_offset = leaves_offset + i as u64 * elems_in_node * leaf_size;
                (keys[0].clone(), cur_offset)
            })
            .collect::<Vec<(Vec<u8>, u64)>>();

        let mut buf = Vec::new();
        let root_offset = Self::build_tree(leaf_nodes, tree_offset, &mut buf);
        (root_offset, buf)
    }

    fn build_tree(nodes_arr: Vec<(Vec<u8>, u64)>, tree_offset: u64, buf: &mut Vec<u8>) -> u64 {
        if nodes_arr.len() == 1 {
            return Self::prep_root(nodes_arr[0].1, tree_offset, buf);
        }
        let max_amount = Self::max_nonleaf_node_capacity(nodes_arr[0].0.len());
        let nodes_amount = (nodes_arr.len() - 1) / max_amount + 1;
        let elems_in_node = nodes_arr.len() / nodes_amount;
        let new_nodes = nodes_arr
            .chunks(elems_in_node)
            .map(|keys| {
                let offset = tree_offset + buf.len() as u64;
                let min_key = keys[0].0.clone();
                let offsets = keys.iter().map(|(_, offset)| *offset).collect();
                let keys = keys.iter().skip(1).map(|(key, _)| key.clone()).collect();
                let node = Node::new(keys, offsets);
                // FIXME: change on result
                buf.extend_from_slice(&serialize_node(&node).expect("failed to serialize node"));
                (min_key, offset)
            })
            .collect();
        Self::build_tree(new_nodes, tree_offset, buf)
    }

    fn prep_root(offset: u64, tree_offset: u64, buf: &mut Vec<u8>) -> u64 {
        let root_node_size = if offset < tree_offset {
            (tree_offset - offset) as usize
        } else {
            buf.len() - (offset - tree_offset) as usize
        };
        let appendix_size = BLOCK_SIZE - root_node_size;
        buf.resize(buf.len() + appendix_size, 0);
        offset
    }

    fn max_leaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        BLOCK_SIZE / (key_size + offset_size) - 1
    }

    fn max_nonleaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        let meta_size = std::mem::size_of::<NodeMeta>();
        (BLOCK_SIZE - meta_size - offset_size) / (key_size + offset_size) + 1
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
                leaves_buf.extend_from_slice(&leaf);
                let offsetb = serialize(&offset).expect("serialize u64");
                leaves_buf.extend_from_slice(&offsetb);
                let res = (offset + (hds.len() * record_header_size) as u64, leaves_buf);
                res
            },
        );
        leaves_buf
    }
}

#[tokio::test]
async fn build_tree_test() {
    let nodes_arr = (1..27u8)
        .into_iter()
        .enumerate()
        .map(|(i, key)| (vec![key], i as u64 * 8))
        .collect();
    let tree_offset = 2000;
    let mut buf = Vec::new();
    BPTreeFileIndex::build_tree(nodes_arr, tree_offset, &mut buf);
}

#[tokio::test]
async fn node_serialize_deserialize() {
    let keys = (1u8..20).map(|e| vec![e]).collect();
    let offsets = (1u64..21).collect();
    let node = Node::new(keys, offsets);
    let buf = serialize_node(&node).expect("Can't serialize");
    let node_deserialized = deserialize_node(&buf, 1).expect("Can't deserialize");
    assert_eq!(node.keys, node_deserialized.keys);
    assert_eq!(node.offsets, node_deserialized.offsets);
}

#[tokio::test]
async fn indices_serialize_deserialize() {
    let mut inmem = InMemoryIndex::new();
    (0..10000u32)
        .map(|i| serialize(&i).expect("can't serialize"))
        .for_each(|key| {
            let rh = RecordHeader::new(key.clone(), 1, 1, 1);
            inmem.insert(key, vec![rh]);
        });
    let filter = Bloom::new(Config::default());
    let findex =
        BPTreeFileIndex::from_records(&Path::new("/tmp/bptree_index.b"), None, &inmem, &filter)
            .await
            .expect("Can't create file index");
    let (inmem_after, _size) = findex
        .get_records_headers()
        .await
        .expect("Can't get InMemoryIndex");
    assert_eq!(inmem, inmem_after);
}

#[tokio::test]
async fn check_get_any() {
    let mut inmem = InMemoryIndex::new();
    (100..9000)
        .map(|i| serialize(&i).expect("can't serialize"))
        .for_each(|key| {
            let rh = RecordHeader::new(key.clone(), 1, 1, 1);
            inmem.insert(key, vec![rh]);
        });
    let filter = Bloom::new(Config::default());
    let findex =
        BPTreeFileIndex::from_records(&Path::new("/tmp/get_bptree_index.b"), None, &inmem, &filter)
            .await
            .expect("Can't create file index");
    let keys_to_check = [100u32, 112, 113, 8999];
    for key in keys_to_check.iter().map(|k| serialize(&k).unwrap()) {
        assert_eq!(findex.get_any(&key).await.unwrap().unwrap(), inmem[&key][0]);
    }
    assert_eq!(
        None,
        findex.get_any(&serialize(&100000).unwrap()).await.unwrap()
    );
}
