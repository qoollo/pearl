use bytes::{BufMut, BytesMut};

use super::storage::Key;
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
pub(crate) struct BPTreeFileIndex<K> {
    file: File,
    header: IndexHeader,
    metadata: TreeMeta,
    root_node: BytesMut,
    key_type_marker: PhantomData<K>,
}

#[async_trait::async_trait]
impl<K> FileIndexTrait<K> for BPTreeFileIndex<K>
where
    for<'a> K: Key<'a> + 'static,
{
    async fn from_file(name: FileName, iodriver: IoDriver) -> Result<Self> {
        trace!("open index file");
        let file = iodriver
            .open(name.to_path())
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
            key_type_marker: PhantomData,
        })
    }

    async fn from_records(
        path: &Path,
        iodriver: IoDriver,
        headers: &InMemoryIndex<K>,
        meta: Vec<u8>,
        recreate_index_file: bool,
        blob_size: u64,
    ) -> Result<Self> {
        clean_file(path, recreate_index_file)?;
        let res = Self::serialize(headers, meta, blob_size)?;
        let (mut header, metadata, buf) = res;
        let file = iodriver
            .create(path)
            .await
            .with_context(|| format!("file open failed {:?}", path))?;
        file.write_append_all(buf.freeze()).await?;
        header.set_written(true);
        let size = header.serialized_size();
        let mut serialized_header = BytesMut::with_capacity(size as usize);
        serialize_into((&mut serialized_header).writer(), &header)?;
        file.write_all_at(0, serialized_header.freeze()).await?;
        file.fsyncdata().await?;
        let root_node = Self::read_root(&file, metadata.tree_offset).await?;
        Ok(Self {
            file,
            metadata,
            header,
            root_node,
            key_type_marker: PhantomData,
        })
    }

    fn file_size(&self) -> u64 {
        self.file.size()
    }

    fn records_count(&self) -> usize {
        self.header.records_count
    }

    fn blob_size(&self) -> u64 {
        self.header.blob_size()
    }

    async fn read_meta(&self) -> Result<BytesMut> {
        trace!("load meta");
        trace!("read meta into buf: [0; {}]", self.header.meta_size);
        self.file
            .read_exact_at_allocate(self.header.meta_size, self.header.serialized_size())
            .await
    }

    async fn read_meta_at(&self, i: u64) -> Result<u8> {
        trace!("load byte from meta");
        if i >= self.header.meta_size as u64 {
            return Err(anyhow::anyhow!("read meta out of range"));
        }
        let buf = self
            .file
            .read_exact_at_allocate(1, self.header.serialized_size() + i)
            .await?;
        Ok(buf[0])
    }

    async fn find_by_key(&self, key: &K) -> Result<Option<Vec<RecordHeader>>> {
        let root_offset = self.metadata.tree_offset;
        let buf = BytesMut::zeroed(BLOCK_SIZE);
        let (buf, leaf_offset) = self.find_leaf_node(key, root_offset, buf).await?;
        self.read_headers(leaf_offset, key, buf).await
    }

    async fn get_records_headers(&self, blob_size: u64) -> Result<(InMemoryIndex<K>, usize)> {
        let mut buf = self.file.read_all().await?;
        self.validate_header(&mut buf, blob_size).await?;
        let offset = self.metadata.leaves_offset as usize;
        let records_end = FileIndexTrait::<K>::file_size(self) as usize;
        let records_buf = &buf[offset..records_end];
        (0..self.header.records_count)
            .try_fold(InMemoryIndex::new(), |mut headers, i| {
                let offset = i * self.header.record_header_size;
                let header: RecordHeader = deserialize(&records_buf[offset..])?;
                // We use get mut instead of entry(..).or_insert(..) because in second case we
                // need to clone header.
                let key = header.key().to_vec().into();
                if let Some(v) = headers.get_mut(&key) {
                    v.push(header)
                } else {
                    headers.insert(key, vec![header]);
                }
                Ok(headers)
            })
            .map(|mut headers| {
                for val in headers.values_mut() {
                    if val.len() > 1 {
                        val.reverse();
                    }
                }
                headers
            })
            .map(|headers| (headers, self.header.records_count))
    }

    async fn get_any(&self, key: &K) -> Result<Option<RecordHeader>> {
        let root_offset = self.metadata.tree_offset;
        let buf = BytesMut::zeroed(BLOCK_SIZE);
        let (buf, leaf_offset) = self.find_leaf_node(key, root_offset, buf).await?;
        self.read_header(leaf_offset, key, buf).await
    }

    fn validate(&self, blob_size: u64) -> Result<()> {
        // FIXME: check hash here?
        if !self.header.is_written() {
            let param = ValidationErrorKind::IndexNotWritten;
            return Err(
                Error::validation(param, "Index is incomplete (no 'is_written' flag)").into(),
            );
        }
        if self.header.version() != HEADER_VERSION {
            let param = ValidationErrorKind::IndexVersion;
            return Err(Error::validation(param, "Index Header version is not valid").into());
        }
        if self.header.key_size() != K::LEN {
            let param = ValidationErrorKind::IndexKeySize;
            return Err(Error::validation(
                param,
                "Index header key_size is not equal to pearl compile-time key size",
            )
            .into());
        }
        if self.header.blob_size() != blob_size {
            let param = ValidationErrorKind::IndexBlobSize;
            return Err(Error::validation(
                param,
                format!(
                    "Index Header is for blob of size {}, but actual blob size is {}",
                    self.header.blob_size(),
                    blob_size
                ),
            )
            .into());
        }
        if self.header.magic_byte() != INDEX_HEADER_MAGIC_BYTE {
            let param = ValidationErrorKind::IndexMagicByte;
            return Err(Error::validation(param, "Index magic byte is not valid").into());
        }
        Ok(())
    }

    fn memory_used(&self) -> usize {
        std::mem::size_of::<Self>() + self.root_node.len()
    }
}

impl<K> BPTreeFileIndex<K>
where
    for<'a> K: Key<'a> + 'static,
{
    async fn find_leaf_node(
        &self,
        key: &K,
        mut offset: u64,
        mut buf: BytesMut,
    ) -> Result<(BytesMut, u64)> {
        while offset < self.metadata.leaves_offset {
            offset = if offset == self.metadata.tree_offset {
                Node::key_offset_serialized(&self.root_node, key)?
            } else {
                buf = self.file.read_exact_at(buf, offset).await.map_err(unexpected_eof_converter)?;
                Node::key_offset_serialized(&buf, key)?
            };
        }
        Ok((buf, offset))
    }

    async fn read_header(
        &self,
        leaf_offset: u64,
        key: &K,
        mut buf: BytesMut,
    ) -> Result<Option<RecordHeader>> {
        let buf_size = self.leaf_node_buf_size(leaf_offset);
        assert!(buf_size <= BLOCK_SIZE);
        buf.resize(buf_size, 0);
        buf = self.file.read_exact_at(buf, leaf_offset).await.map_err(unexpected_eof_converter)?;
        if let Some((record_header, offset)) =
            self.read_header_buf(&buf, key, self.header.record_header_size)?
        {
            let leftmost_header = self.get_leftmost(
                &buf,
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

    fn get_leftmost(
        &self,
        raw_headers_buf: &[u8],
        key: &K,
        mut offset: usize,
        mut prev_header: RecordHeader, // it's expected, that this header is from raw_headers_buf[offset..]
        record_header_size: usize,
    ) -> Result<RecordHeader> {
        while offset > 0 {
            offset = offset.saturating_sub(record_header_size);
            let record_end = offset + record_header_size;
            let current_header: RecordHeader = deserialize(&raw_headers_buf[offset..record_end])?;
            if !current_header.key().eq(key.as_ref()) {
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
        key: &K,
        record_header_size: usize,
    ) -> Result<Option<(RecordHeader, usize)>> {
        let mut l = 0i32;
        let mut r: i32 = (raw_headers_buf.len() / record_header_size) as i32 - 1;
        while l <= r {
            let m = (l + r) / 2;
            let m_off = record_header_size * m as usize;
            let record_end = m_off + record_header_size;
            let record_header: RecordHeader = deserialize(&raw_headers_buf[m_off..record_end])?;
            let cmp_res = key.as_ref_key().cmp(&record_header.key().into());
            match cmp_res {
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
        key: &K,
        mut buf: BytesMut,
    ) -> Result<Option<Vec<RecordHeader>>> {
        let buf_size = self.leaf_node_buf_size(leaf_offset);
        assert!(buf_size <= BLOCK_SIZE);
        buf.resize(buf_size, 0);
        buf = self.file.read_exact_at(buf, leaf_offset).await.map_err(unexpected_eof_converter)?;
        let rh_size = self.header.record_header_size;
        if let Some((header, offset)) = self.read_header_buf(&buf, key, rh_size)? {
            let mut headers = Vec::with_capacity(1);
            self.go_left(header.key(), &mut headers, &buf, offset)
                .await?;
            if headers.len() > 1 {
                headers.reverse();
            }
            headers.push(header);
            self.go_right(&mut headers, &buf, offset, leaf_offset)
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
        let mut buf = BytesMut::zeroed(record_header_size as usize);
        while offset + record_header_size <= leaves_end {
            buf = self.file.read_exact_at(buf, offset).await.map_err(unexpected_eof_converter)?;
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
        key: &[u8],
        headers: &mut Vec<RecordHeader>,
        buf: &[u8],
        mut offset: usize,
    ) -> Result<()> {
        let record_header_size = self.header.record_header_size;
        while offset >= record_header_size {
            let record_start = offset - record_header_size;
            let rh: RecordHeader = deserialize(&buf[record_start..offset])?;
            if rh.key() == key {
                headers.push(rh);
            } else {
                return Ok(());
            }
            offset = record_start;
        }
        Ok(())
    }

    async fn validate_header(&self, buf: &mut [u8], blob_size: u64) -> Result<()> {
        self.validate(blob_size)?;
        if !Self::hash_valid(&self.header, buf)? {
            let param = ValidationErrorKind::IndexChecksum;
            return Err(Error::validation(param, "header hash mismatch").into());
        }
        Ok(())
    }

    fn hash_valid(header: &IndexHeader, buf: &mut [u8]) -> Result<bool> {
        let hash = &header.hash;
        let mut header = header.clone();
        header.reset_hash();
        header.set_written(false);
        serialize_into(&mut buf[..], &header)?;
        let new_hash = IndexHashCalculator::get_hash(&buf);
        Ok(*hash == new_hash)
    }

    async fn read_index_header(file: &File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default() as usize;
        let buf = file.read_exact_at_allocate(header_size, 0).await
            .map_err(unexpected_eof_converter)?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    async fn read_root(file: &File, root_offset: u64) -> Result<BytesMut> {
        let buf_size = std::cmp::min((file.size() - root_offset) as usize, BLOCK_SIZE);
        let mut buf = BytesMut::zeroed(BLOCK_SIZE);
        buf.resize(buf_size, 0);
        let mut buf = file.read_exact_at(buf, root_offset).await?;
        buf.resize(BLOCK_SIZE, 0);
        Ok(buf)
    }

    async fn read_tree_meta(file: &File, header: &IndexHeader) -> Result<TreeMeta> {
        let meta_size = TreeMeta::serialized_size_default()? as usize;
        let fsize = header.meta_size as u64;
        let hs = header.serialized_size();
        let meta_offset = hs + fsize;
        let buf = file.read_exact_at_allocate(meta_size, meta_offset).await
            .map_err(unexpected_eof_converter)?;
        TreeMeta::from_raw(&buf).map_err(Into::into)
    }

    fn serialize(
        headers_btree: &InMemoryIndex<K>,
        meta: Vec<u8>,
        blob_size: u64,
    ) -> Result<(IndexHeader, TreeMeta, BytesMut)> {
        Serializer::new(headers_btree)
            .header_stage(meta, blob_size)?
            .tree_stage()?
            .build()
    }
}
