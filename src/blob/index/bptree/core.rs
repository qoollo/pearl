/// structure of b+-tree index file from the beginning:
/// 1. Header
/// 2. Bloom filter
/// 3. Sorted by keys array of record headers (serialized size of one record header is 61 bytes)
/// 4. Metadata
/// 5. Sorted by keys array of pairs `(key, offset of record header in file)` (these are leaf nodes
///    of our B+ tree; as opposed to using record headers as leaves, this approach allows to include
///    more elements in one block and reducing height of the tree (but increasing on one block
///    that we need to read); this also will be useful for scanning queries: instead of scanning
///    61 bytes headers (in case of 4-bytes key) we scan 12 bytes pairs (in case of 4-bytes key and
///    8-bytes offset) which allows to read less bytes in 5 times)
/// 6. Non-leaf nodes of btree in format
///    `NodeMeta | keys_arr | pointers_arr`,
///    where pointer is offset in file of underlying node with searched key
use super::prelude::*;
use std::mem::size_of;

pub(super) const BLOCK_SIZE: usize = 512;

#[derive(Debug, Clone)]
pub(crate) struct BPTreeFileIndex {
    file: File,
    header: IndexHeader,
    metadata: TreeMeta,
    root_node: [u8; BLOCK_SIZE],
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
        let root_node = Self::read_root(&file, metadata.root_offset).await?;

        Ok(Self {
            file,
            header,
            metadata,
            root_node,
        })
    }

    async fn from_records(
        path: &Path,
        ioring: Option<Rio>,
        headers: &InMemoryIndex,
        meta: Vec<u8>,
        recreate_index_file: bool,
    ) -> Result<Self> {
        clean_file(path, recreate_index_file)?;
        let res = Self::serialize(headers, meta)?;
        let (mut header, metadata, buf) = res;
        let file = File::create(path, ioring)
            .await
            .with_context(|| format!("file open failed {:?}", path))?;
        file.write_append(&buf).await?;
        header.set_written(true);
        let serialized_header = serialize(&header)?;
        file.write_at(0, &serialized_header).await?;
        file.fsyncdata().await?;
        let root_node = Self::read_root(&file, metadata.root_offset).await?;
        Ok(Self {
            file,
            metadata,
            header,
            root_node,
        })
    }

    fn file_size(&self) -> u64 {
        self.file.size()
    }

    fn records_count(&self) -> usize {
        self.header.records_count
    }

    async fn read_meta(&self) -> Result<Vec<u8>> {
        trace!("load meta");
        let mut buf = vec![0; self.header.meta_size];
        trace!("read meta into buf: [0; {}]", buf.len());
        self.file
            .read_at(&mut buf, self.header.serialized_size()? as u64)
            .await?;
        Ok(buf)
    }

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
        let offset = self.header.meta_size + self.header.serialized_size()? as usize;
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
            let header = self.read_headers(first_header_offset, 1).await?.remove(0);
            Ok(Some(header))
        } else {
            Ok(None)
        }
    }

    fn validate(&self) -> Result<()> {
        if self.header.is_written() {
            Ok(())
        } else {
            Err(Error::validation("Index Header is not valid").into())
        }
    }
}

impl BPTreeFileIndex {
    async fn find_leaf_node(&self, key: &[u8], mut offset: u64, buf: &mut [u8]) -> Result<u64> {
        while offset >= self.metadata.tree_offset {
            offset = if offset >= self.metadata.root_offset {
                Node::key_offset_serialized(&self.root_node, key)?
            } else {
                self.file.read_at(buf, offset).await?;
                Node::key_offset_serialized(buf, key)?
            };
        }
        Ok(offset)
    }

    async fn find_first_header(
        &self,
        leaf_offset: u64,
        key: &[u8],
        buf: &mut [u8],
    ) -> Result<Option<(u64, u64)>> {
        let leaf_size = key.len() + size_of::<u64>();
        let buf_size = self.leaf_node_buf_size(leaf_size, leaf_offset);
        let pointers_size = self.file.read_at(&mut buf[..buf_size], leaf_offset).await?;
        self.search_header_pointer(&buf[..pointers_size], key, buf_size as u64 + leaf_offset)
    }

    fn leaf_node_buf_size(&self, leaf_size: usize, leaf_offset: u64) -> usize {
        let buf_size = (self.metadata.tree_offset - leaf_offset) as usize;
        buf_size
            .min(BLOCK_SIZE - (BLOCK_SIZE % leaf_size))
            .min((self.metadata.tree_offset - leaf_offset) as usize)
    }

    fn search_header_pointer(
        &self,
        header_pointers: &[u8],
        key: &[u8],
        absolute_buf_end: u64,
    ) -> Result<Option<(u64, u64)>> {
        let leaf_size = key.len() + size_of::<u64>();
        let mut l = 0i32;
        let mut r: i32 = (header_pointers.len() / leaf_size) as i32 - 1;
        while l <= r {
            let m = (l + r) / 2;
            let m_off = leaf_size * m as usize;
            match key.cmp(&header_pointers[m_off..(m_off + key.len())]) {
                CmpOrdering::Less => r = m - 1,
                CmpOrdering::Greater => l = m + 1,
                CmpOrdering::Equal => {
                    return Ok(Some(self.with_amount(
                        header_pointers,
                        m as usize,
                        absolute_buf_end,
                        key.len(),
                    )?));
                }
            }
        }
        Ok(None)
    }

    fn with_amount(
        &self,
        header_pointers: &[u8],
        mid: usize,
        buf_end: u64,
        key_size: usize,
    ) -> Result<(u64, u64)> {
        let leaf_size = key_size + size_of::<u64>();
        let is_last_rec = (mid + 1) * leaf_size >= header_pointers.len()
            && (buf_end >= self.metadata.tree_offset);
        let mid_offset = mid * leaf_size + key_size;
        let cur_offset =
            deserialize::<u64>(&header_pointers[mid_offset..(mid_offset + size_of::<u64>())])?;
        let next_offset = if is_last_rec {
            self.metadata.leaves_offset
        } else {
            let next_offset = mid_offset + leaf_size;
            deserialize(&header_pointers[next_offset..(next_offset + size_of::<u64>())])?
        };
        let amount = (next_offset - cur_offset) / self.header.record_header_size as u64;
        Ok((cur_offset, amount))
    }

    async fn read_headers(&self, offset: u64, amount: usize) -> Result<Vec<RecordHeader>> {
        let mut buf = vec![0u8; self.header.record_header_size * amount];
        self.file.read_at(&mut buf, offset).await?;
        buf.chunks(self.header.record_header_size).try_fold(
            Vec::with_capacity(amount),
            |mut acc, bytes| {
                acc.push(deserialize(&bytes)?);
                Ok(acc)
            },
        )
    }

    async fn validate_header(&self, buf: &mut Vec<u8>) -> Result<()> {
        self.validate()?;
        if !Self::hash_valid(&self.header, buf)? {
            return Err(Error::validation("header hash mismatch").into());
        }
        if self.header.version() != HEADER_VERSION {
            return Err(Error::validation("header version mismatch").into());
        }
        Ok(())
    }

    fn hash_valid(header: &IndexHeader, buf: &mut Vec<u8>) -> Result<bool> {
        let hash = header.hash.clone();
        let mut header = header.clone();
        header.hash = vec![0; ring::digest::SHA256.output_len];
        header.set_written(false);
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

    async fn read_root(file: &File, root_offset: u64) -> Result<[u8; BLOCK_SIZE]> {
        let mut buf = [0; BLOCK_SIZE];
        file.read_at(&mut buf, root_offset).await?;
        Ok(buf)
    }

    async fn read_tree_meta(file: &File, header: &IndexHeader) -> Result<TreeMeta> {
        let meta_size = TreeMeta::serialized_size_default()? as usize;
        let mut buf = vec![0; meta_size];
        let fsize = header.meta_size as u64;
        let hs = header.serialized_size()?;
        let meta_offset = hs + fsize + (header.records_count * header.record_header_size) as u64;
        file.read_at(&mut buf, meta_offset).await?;
        TreeMeta::from_raw(&buf).map_err(Into::into)
    }

    fn serialize(
        headers_btree: &InMemoryIndex,
        meta: Vec<u8>,
    ) -> Result<(IndexHeader, TreeMeta, Vec<u8>)> {
        Serializer::new(headers_btree)
            .header_stage(meta)?
            .leaves_stage()?
            .tree_stage()?
            .build()
    }
}
