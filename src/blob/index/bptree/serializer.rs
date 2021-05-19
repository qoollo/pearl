use super::prelude::*;

pub(super) struct HeaderStage<'a> {
    headers_btree: &'a InMemoryIndex,
    header: IndexHeader,
    meta: Vec<u8>,
}

pub(super) struct LeavesStage<'a> {
    headers_btree: &'a InMemoryIndex,
    header: IndexHeader,
    leaves_offset: u64,
    headers_size: usize,
    meta: Vec<u8>,
}

pub(super) struct TreeStage<'a> {
    headers_btree: &'a InMemoryIndex,
    metadata: TreeMeta,
    header: IndexHeader,
    meta_buf: Vec<u8>,
    tree_buf: Vec<u8>,
    headers_size: usize,
    meta: Vec<u8>,
}

pub(super) struct Serializer<'a> {
    headers_btree: &'a InMemoryIndex,
}

impl<'a> Serializer<'a> {
    pub(super) fn new(headers_btree: &'a InMemoryIndex) -> Self {
        Self { headers_btree }
    }
    pub(super) fn header_stage(self, meta: Vec<u8>) -> Result<HeaderStage<'a>> {
        if let Some(record_header) = self.headers_btree.values().next().and_then(|v| v.first()) {
            let record_header_size = record_header.serialized_size().try_into()?;
            let headers_len = self
                .headers_btree
                .iter()
                .fold(0, |acc, (_k, v)| acc + v.len());
            let header = IndexHeader::new(record_header_size, headers_len, meta.len());
            Ok(HeaderStage {
                headers_btree: self.headers_btree,
                header,
                meta,
            })
        } else {
            Err(anyhow!("BTree is empty, can't find info about key len!"))
        }
    }
}

impl<'a> HeaderStage<'a> {
    pub(super) fn leaves_stage(self) -> Result<LeavesStage<'a>> {
        let hs = self.header.serialized_size()? as usize;
        let fsize = self.header.meta_size;
        let headers_start_offset = (hs + fsize) as u64;
        let headers_size = self.header.records_count * self.header.record_header_size;
        let leaves_offset = headers_start_offset;
        // leaf contains pair: (key, offset in file for first record's header with this key)
        Ok(LeavesStage {
            headers_btree: self.headers_btree,
            leaves_offset,
            headers_size,
            meta: self.meta,
            header: self.header,
        })
    }
}

impl<'a> LeavesStage<'a> {
    pub(super) fn tree_stage(self) -> Result<TreeStage<'a>> {
        let tree_offset =
            self.leaves_offset + TreeMeta::serialized_size_default()? + self.headers_size as u64;
        let (root_offset, tree_buf) = Self::serialize_bptree(
            self.headers_btree,
            self.leaves_offset,
            tree_offset,
            self.header.record_header_size as u64,
        )?;
        let metadata = TreeMeta::new(root_offset, self.leaves_offset, tree_offset);
        let meta_buf = serialize(&metadata)?;
        Ok(TreeStage {
            headers_btree: self.headers_btree,
            metadata,
            meta_buf,
            tree_buf,
            header: self.header,
            headers_size: self.headers_size,
            meta: self.meta,
        })
    }

    fn serialize_bptree(
        btree: &InMemoryIndex,
        leaves_offset: u64,
        tree_offset: u64,
        record_header_size: u64,
    ) -> Result<(u64, Vec<u8>)> {
        let mut leaf_nodes_compressed = Vec::new();
        let mut offset = leaves_offset;
        let mut left = BLOCK_SIZE as u64;
        let mut min_k = btree.keys().next().unwrap().clone();
        let mut min_o = offset;
        for (k, v) in btree.iter() {
            if left < record_header_size {
                leaf_nodes_compressed.push((min_k, min_o));
                min_k = k.clone();
                min_o = offset;
                left = BLOCK_SIZE as u64;
            }
            let delta_size = v.len() as u64 * record_header_size;
            offset += delta_size;
            left = left.saturating_sub(delta_size);
        }
        leaf_nodes_compressed.push((min_k, min_o));
        let mut buf = Vec::new();
        let root_offset = Self::build_tree(leaf_nodes_compressed, tree_offset, &mut buf)?;
        Ok((root_offset, buf))
    }

    fn process_keys_portion(
        buf: &mut Vec<u8>,
        tree_offset: u64,
        nodes_portion: &[(Vec<u8>, u64)],
    ) -> Result<(Vec<u8>, u64)> {
        let offset = tree_offset + buf.len() as u64;
        let min_key = nodes_portion[0].0.clone();
        let offsets_iter = nodes_portion.iter().map(|(_, offset)| *offset);
        let keys_iter = nodes_portion[1..].iter().map(|(k, _)| k.as_ref());
        let node_buf = Node::new_serialized(
            keys_iter,
            offsets_iter,
            nodes_portion[0].0.len(),
            nodes_portion.len() - 1,
        )?;
        buf.extend_from_slice(&node_buf);
        Ok((min_key, offset))
    }

    pub(super) fn build_tree(
        nodes_arr: Vec<(Vec<u8>, u64)>,
        tree_offset: u64,
        buf: &mut Vec<u8>,
    ) -> Result<u64> {
        if nodes_arr.len() == 1 {
            return Ok(Self::prep_root(nodes_arr[0].1, tree_offset, buf));
        }
        let max_amount = Self::max_nonleaf_node_capacity(nodes_arr[0].0.len());
        let min_amount = (max_amount - 1) / 2 + 1;
        let mut current = 0;
        let mut new_nodes = Vec::new();
        while nodes_arr.len() - current > max_amount {
            // amount == min_amount at least (if nodes_arr.len() - current == max_amount + 1)
            // and min operation is necessary to have at least min_amount nodes left
            let amount = std::cmp::min(max_amount, nodes_arr.len() - current - min_amount);
            let nodes_portion = &nodes_arr[current..(current + amount)];
            current += amount;
            let compressed_node = Self::process_keys_portion(buf, tree_offset, nodes_portion)?;
            new_nodes.push(compressed_node);
        }
        // min_amount <= nodes left <= max_amount
        let nodes_portion = &nodes_arr[current..];
        new_nodes.push(Self::process_keys_portion(
            buf,
            tree_offset,
            &nodes_portion,
        )?);
        Self::build_tree(new_nodes, tree_offset, buf)
    }

    fn max_nonleaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        let meta_size =
            NodeMeta::serialized_size_default().expect("Can't retrieve default serialized size");
        (BLOCK_SIZE - meta_size as usize - offset_size) / (key_size + offset_size) + 1
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
}

impl<'a> TreeStage<'a> {
    pub(super) fn build(self) -> Result<(IndexHeader, TreeMeta, Vec<u8>)> {
        let hs = self.header.serialized_size()? as usize;
        let fsize = self.header.meta_size;
        let msize = self.meta_buf.len();
        let data_size = hs + fsize + self.headers_size + msize + self.tree_buf.len();
        let mut buf = Vec::with_capacity(data_size);
        serialize_into(&mut buf, &self.header)?;
        buf.extend_from_slice(&self.meta);
        Self::append_headers(self.headers_btree, &mut buf)?;
        buf.extend_from_slice(&self.meta_buf);
        buf.extend_from_slice(&self.tree_buf);
        let hash = get_hash(&buf);
        let header = IndexHeader::with_hash(
            self.header.record_header_size,
            self.header.records_count,
            self.meta.len(),
            hash,
        );
        serialize_into(buf.as_mut_slice(), &header)?;
        Ok((header, self.metadata, buf))
    }

    fn append_headers(headers_btree: &InMemoryIndex, buf: &mut Vec<u8>) -> Result<()> {
        headers_btree
            .iter()
            .flat_map(|r| r.1)
            .map(|h| serialize(&h))
            .try_fold(buf, |buf, h_buf| -> Result<_> {
                buf.extend_from_slice(&h_buf?);
                Ok(buf)
            })?;
        Ok(())
    }
}
