use crate::error::ValidationErrorKind;

/// structure of b+-tree index file from the beginning:
/// 1. Header
/// 2. b+-tree user buffer (now Bloom filter is stored as this buffer)
/// 3. Tree metadata (tree_offset [4] (tree starts from root, so that is also root_offset),
///    and leaves_offset [5])
/// 4. Tree: Non-leaf nodes of bptree in format
///    `NodeMeta | keys_arr | pointers_arr`,
///    where pointer is offset in file of underlying node with searched key
/// 5. Sorted by key array of record headers (coupled leaf nodes)
use super::prelude::*;

pub(super) const BLOCK_SIZE: usize = 4096;

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
        let root_node = Self::read_root(&file, metadata.tree_offset).await?;

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
        file.flush().await?;
        let root_node = Self::read_root(&file, metadata.tree_offset).await?;
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
        let root_offset = self.metadata.tree_offset;
        let mut buf = [0u8; BLOCK_SIZE];
        let leaf_offset = self.find_leaf_node(key, root_offset, &mut buf).await?;
        self.read_headers(leaf_offset, key, &mut buf).await
    }

    async fn get_records_headers(&self) -> Result<(InMemoryIndex, usize)> {
        let mut buf = self.file.read_all().await?;
        self.validate_header(&mut buf).await?;
        let offset = self.metadata.leaves_offset as usize;
        let records_end = self.file_size() as usize;
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
        let root_offset = self.metadata.tree_offset;
        let mut buf = [0u8; BLOCK_SIZE];
        let leaf_offset = self.find_leaf_node(key, root_offset, &mut buf).await?;
        self.read_header(leaf_offset, key, &mut buf).await
    }

    fn validate(&self) -> Result<()> {
        // FIXME: check hash here?
        if !self.header.is_written() {
            let param = ValidationErrorKind::IndexIsWritten;
            return Err(Error::validation(param, "Index Header version is not valid").into());
        }
        if self.header.version() != HEADER_VERSION {
            let param = ValidationErrorKind::IndexVersion;
            return Err(Error::validation(param, "Index Header version is not valid").into());
        }
        Ok(())
    }
}

impl BPTreeFileIndex {
    async fn find_leaf_node(&self, key: &[u8], mut offset: u64, buf: &mut [u8]) -> Result<u64> {
        while offset < self.metadata.leaves_offset {
            offset = if offset == self.metadata.tree_offset {
                Node::key_offset_serialized(&self.root_node, key)?
            } else {
                self.file.read_at(buf, offset).await?;
                Node::key_offset_serialized(buf, key)?
            };
        }
        Ok(offset)
    }

    async fn read_header(
        &self,
        leaf_offset: u64,
        key: &[u8],
        buf: &mut [u8],
    ) -> Result<Option<RecordHeader>> {
        let buf_size = self.leaf_node_buf_size(leaf_offset);
        let read_buf_size = self.file.read_at(&mut buf[..buf_size], leaf_offset).await?;
        if read_buf_size != buf_size {
            Err(anyhow!("Can't read entire leaf node"))
        } else {
            if let Some((record_header, offset)) =
                self.read_header_buf(&buf[..buf_size], key, self.header.record_header_size)?
            {
                let leftmost_header = self.get_leftmost(
                    &buf[..buf_size],
                    key,
                    offset as usize,
                    record_header,
                    self.header.record_header_size,
                )?;
                Ok(Some(leftmost_header))
            } else {
                Ok(None)
            }
        }
    }

    fn get_leftmost(
        &self,
        raw_headers_buf: &[u8],
        key: &[u8],
        mut offset: usize,
        mut prev_header: RecordHeader, // it's expected, that this header is from raw_headers_buf[offset..]
        record_header_size: usize,
    ) -> Result<RecordHeader> {
        while offset > 0 {
            offset = offset.saturating_sub(record_header_size);
            let record_end = offset + record_header_size;
            let current_header: RecordHeader = deserialize(&raw_headers_buf[offset..record_end])?;
            if !current_header.key().eq(key) {
                return Ok(prev_header);
            }
            prev_header = current_header;
        }
        Ok(prev_header)
    }

    fn leaf_node_buf_size(&self, leaf_offset: u64) -> usize {
        // if we read last leaf, it may be shorter
        ((self.file_size() - leaf_offset) as usize).min(BLOCK_SIZE)
    }

    fn read_header_buf(
        &self,
        raw_headers_buf: &[u8],
        key: &[u8],
        record_header_size: usize,
    ) -> Result<Option<(RecordHeader, usize)>> {
        let mut l = 0i32;
        let mut r: i32 = (raw_headers_buf.len() / record_header_size) as i32 - 1;
        while l <= r {
            let m = (l + r) / 2;
            let m_off = record_header_size * m as usize;
            let record_end = m_off + record_header_size;
            let record_header: RecordHeader = deserialize(&raw_headers_buf[m_off..record_end])?;
            match key.cmp(record_header.key()) {
                CmpOrdering::Less => r = m - 1,
                CmpOrdering::Greater => l = m + 1,
                CmpOrdering::Equal => return Ok(Some((record_header, m_off))),
            }
        }
        Ok(None)
    }

    async fn read_headers(
        &self,
        leaf_offset: u64,
        key: &[u8],
        buf: &mut [u8],
    ) -> Result<Option<Vec<RecordHeader>>> {
        let buf_size = self.leaf_node_buf_size(leaf_offset);
        let read_buf_size = self.file.read_at(&mut buf[..buf_size], leaf_offset).await?;
        let rh_size = self.header.record_header_size;
        if read_buf_size != buf_size {
            return Err(anyhow!("Can't read entire leaf node"));
        }
        if let Some((header, offset)) = self.read_header_buf(&buf[..buf_size], key, rh_size)? {
            let mut headers = vec![header];
            self.go_left(&mut headers, &buf[..buf_size], offset).await?;
            self.go_right(&mut headers, &buf[..buf_size], offset, leaf_offset)
                .await?;
            Ok(Some(headers))
        } else {
            Ok(None)
        }
    }

    async fn go_right(
        &self,
        headers: &mut Vec<RecordHeader>,
        buf: &[u8],
        mut offset: usize,
        leaf_offset: u64,
    ) -> Result<()> {
        let record_header_size = self.header.record_header_size;
        let records_size = self.header.record_header_size * self.header.records_count;
        let leaves_end = self.metadata.leaves_offset as usize + records_size;
        let right_bound = std::cmp::min(leaves_end - leaf_offset as usize, buf.len());
        offset += record_header_size;
        while offset + record_header_size < right_bound {
            let record_end = offset + record_header_size;
            let rh: RecordHeader = deserialize(&buf[offset..record_end])?;
            if rh.key() == headers[0].key() {
                headers.push(rh);
            } else {
                return Ok(());
            }
            offset = record_end;
        }
        self.go_right_file(headers, leaf_offset + offset as u64)
            .await?;
        Ok(())
    }

    async fn go_right_file(&self, headers: &mut Vec<RecordHeader>, mut offset: u64) -> Result<()> {
        // TODO: read headers one by one from file may be inefficient
        let record_header_size = self.header.record_header_size as u64;
        let records_size = self.header.record_header_size * self.header.records_count;
        let leaves_end = self.metadata.leaves_offset + records_size as u64;
        let mut buf = vec![0; record_header_size as usize];
        while offset + record_header_size <= leaves_end {
            let read_buf_size = self.file.read_at(&mut buf, offset).await?;
            if read_buf_size != buf.len() {
                return Err(anyhow!("Can't read header from file"));
            }
            let header: RecordHeader = deserialize(&buf)?;
            if header.key() == headers[0].key() {
                headers.push(header);
            } else {
                return Ok(());
            }
            offset += record_header_size;
        }
        Ok(())
    }

    // notice that every node starts from first header with key (i.e. there is no way for record
    // with the same key to be on the left side from the start of leaf node, where it is)
    async fn go_left(
        &self,
        headers: &mut Vec<RecordHeader>,
        buf: &[u8],
        mut offset: usize,
    ) -> Result<()> {
        let record_header_size = self.header.record_header_size;
        while offset >= record_header_size {
            let record_start = offset - record_header_size;
            let rh: RecordHeader = deserialize(&buf[record_start..offset])?;
            if rh.key() == headers[0].key() {
                headers.push(rh);
            } else {
                return Ok(());
            }
            offset = record_start;
        }
        Ok(())
    }

    async fn validate_header(&self, buf: &mut Vec<u8>) -> Result<()> {
        self.validate()?;
        if !Self::hash_valid(&self.header, buf)? {
            let param = ValidationErrorKind::IndexChecksum;
            return Err(Error::validation(param, "header hash mismatch").into());
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
        let buf_size = std::cmp::min((file.size() - root_offset) as usize, BLOCK_SIZE);
        file.read_at(&mut buf[..buf_size], root_offset).await?;
        Ok(buf)
    }

    async fn read_tree_meta(file: &File, header: &IndexHeader) -> Result<TreeMeta> {
        let meta_size = TreeMeta::serialized_size_default()? as usize;
        let mut buf = vec![0; meta_size];
        let fsize = header.meta_size as u64;
        let hs = header.serialized_size()?;
        let meta_offset = hs + fsize;
        file.read_at(&mut buf, meta_offset).await?;
        TreeMeta::from_raw(&buf).map_err(Into::into)
    }

    fn serialize(
        headers_btree: &InMemoryIndex,
        meta: Vec<u8>,
    ) -> Result<(IndexHeader, TreeMeta, Vec<u8>)> {
        Serializer::new(headers_btree)
            .header_stage(meta)?
            .tree_stage()?
            .build()
    }
}
