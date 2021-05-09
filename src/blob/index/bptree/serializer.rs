use super::prelude::*;

pub(super) struct HeaderStage<'a> {
    headers_btree: &'a InMemoryIndex,
    header: IndexHeader,
    key_size: usize,
    meta: Vec<u8>,
}

pub(super) struct LeavesStage<'a> {
    headers_btree: &'a InMemoryIndex,
    header: IndexHeader,
    leaf_size: usize,
    leaves_buf: Vec<u8>,
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
    leaves_buf: Vec<u8>,
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
            let key_size = record_header.key().len();
            Ok(HeaderStage {
                headers_btree: self.headers_btree,
                header,
                key_size,
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
        let msize: usize = TreeMeta::serialized_size_default()? as usize;
        let headers_start_offset = (hs + fsize) as u64;
        let headers_size = self.header.records_count * self.header.record_header_size;
        let leaves_offset = headers_start_offset + (headers_size + msize) as u64;
        // leaf contains pair: (key, offset in file for first record's header with this key)
        let leaf_size = self.key_size + std::mem::size_of::<u64>();
        let leaves_buf = Self::serialize_leaves(
            self.headers_btree,
            headers_start_offset,
            leaf_size,
            self.header.record_header_size,
        );
        Ok(LeavesStage {
            headers_btree: self.headers_btree,
            leaf_size,
            leaves_buf,
            leaves_offset,
            headers_size,
            meta: self.meta,
            header: self.header,
        })
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

impl<'a> LeavesStage<'a> {
    pub(super) fn tree_stage(self) -> Result<TreeStage<'a>> {
        let tree_offset = self.leaves_offset + self.leaves_buf.len() as u64;
        let (root_offset, tree_buf) = Self::serialize_bptree(
            self.headers_btree,
            self.leaves_offset,
            self.leaf_size as u64,
            tree_offset,
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
            leaves_buf: self.leaves_buf,
            meta: self.meta,
        })
    }

    fn serialize_bptree(
        btree: &InMemoryIndex,
        leaves_offset: u64,
        leaf_size: u64,
        tree_offset: u64,
    ) -> Result<(u64, Vec<u8>)> {
        let max_amount = Self::max_leaf_node_capacity(btree.keys().next().unwrap().len());
        let nodes_amount = (btree.len() - 1) / max_amount + 1;
        let elems_in_node = ((btree.len() - 1) / nodes_amount + 1) as u64;
        let mut leaf_nodes_compressed = Vec::with_capacity(nodes_amount);
        let mut offset = leaves_offset;
        for k in btree.keys().step_by(elems_in_node as usize) {
            leaf_nodes_compressed.push((k.clone(), offset));
            offset += elems_in_node * leaf_size;
        }
        let mut buf = Vec::new();
        let root_offset = Self::build_tree(leaf_nodes_compressed, tree_offset, &mut buf)?;
        Ok((root_offset, buf))
    }

    fn process_keys_portion(
        buf: &mut Vec<u8>,
        tree_offset: u64,
        nodes_portion: &[(Vec<u8>, u64)],
    ) -> (Vec<u8>, u64) {
        let offset = tree_offset + buf.len() as u64;
        let min_key = nodes_portion[0].0.clone();
        let offsets = nodes_portion.iter().map(|(_, offset)| *offset).collect();
        let keys = nodes_portion[1..]
            .iter()
            .map(|(key, _)| key.clone())
            .collect();
        let node = Node::new(keys, offsets);
        buf.extend_from_slice(&node.serialize().expect("failed to serialize node"));
        (min_key, offset)
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
        let nodes_amount = (nodes_arr.len() - 1) / max_amount + 1;
        let elems_in_node = (nodes_arr.len() - 1) / nodes_amount + 1;
        let mut current = 0;
        let mut new_nodes = if nodes_arr.len() % elems_in_node == 1 {
            // special case: last node must have at least 2 children, so if it hasn't in current
            // distribution, we'll borrow one child in 1st portion
            let nodes_portion = &nodes_arr[current..(current + elems_in_node - 1)];
            current += elems_in_node;
            let compressed_node = Self::process_keys_portion(buf, tree_offset, nodes_portion);
            vec![compressed_node]
        } else {
            Vec::new()
        };
        while current < nodes_arr.len() {
            let right_bound = std::cmp::min(nodes_arr.len(), current + elems_in_node);
            let nodes_portion = &nodes_arr[current..right_bound];
            current = right_bound;
            let compressed_node = Self::process_keys_portion(buf, tree_offset, nodes_portion);
            new_nodes.push(compressed_node);
        }
        Self::build_tree(new_nodes, tree_offset, buf)
    }

    fn max_leaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        BLOCK_SIZE / (key_size + offset_size) - 1
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
        let data_size =
            hs + fsize + self.headers_size + msize + self.leaves_buf.len() + self.tree_buf.len();
        let mut buf = Vec::with_capacity(data_size);
        serialize_into(&mut buf, &self.header)?;
        buf.extend_from_slice(&self.meta);
        Self::append_headers(self.headers_btree, &mut buf)?;
        buf.extend_from_slice(&self.meta_buf);
        buf.extend_from_slice(&self.leaves_buf);
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
