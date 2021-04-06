use super::prelude::*;

struct HeaderStage {
    header: IndexHeader,
    key_size: usize,
    filter_buf: Vec<u8>,
}

struct LeavesStage {
    leaf_size: usize,
    leaves_buf: Vec<u8>,
    leaves_offset: u64,
    headers_size: usize,
}

struct TreeStage {
    metadata: TreeMeta,
    meta_buf: Vec<u8>,
    tree_buf: Vec<u8>,
}

pub(super) struct Serializer<'a> {
    headers_btree: &'a InMemoryIndex,
    header_stage: Option<HeaderStage>,
    leaves_stage: Option<LeavesStage>,
    tree_stage: Option<TreeStage>,
}

impl<'a> Serializer<'a> {
    pub(super) fn new(headers_btree: &'a InMemoryIndex) -> Self {
        Self {
            headers_btree,
            header_stage: None,
            leaves_stage: None,
            tree_stage: None,
        }
    }

    pub(super) fn build(self) -> Result<(IndexHeader, TreeMeta, Vec<u8>)> {
        let HeaderStage {
            header,
            filter_buf,
            key_size: _,
        } = self
            .header_stage
            .ok_or_else(|| anyhow!("Previous stages were not done"))?;
        let LeavesStage {
            leaves_buf,
            leaves_offset: _,
            leaf_size: _,
            headers_size,
        } = self
            .leaves_stage
            .ok_or_else(|| anyhow!("Previous stages were not done"))?;
        let TreeStage {
            metadata,
            tree_buf,
            meta_buf,
        } = self
            .tree_stage
            .ok_or_else(|| anyhow!("Previous stages were not done"))?;

        let hs = header.serialized_size()? as usize;
        let fsize = header.filter_buf_size;
        let msize = meta_buf.len();
        let data_size = hs + fsize + headers_size + msize + leaves_buf.len() + tree_buf.len();
        let mut buf = Vec::with_capacity(data_size);
        serialize_into(&mut buf, &header)?;
        buf.extend_from_slice(&filter_buf);
        Self::append_headers(self.headers_btree, &mut buf)?;
        buf.extend_from_slice(&meta_buf);
        buf.extend_from_slice(&leaves_buf);
        buf.extend_from_slice(&tree_buf);
        let hash = get_hash(&buf);
        let header = IndexHeader::with_hash(
            header.record_header_size,
            header.records_count,
            filter_buf.len(),
            hash,
        );
        serialize_into(buf.as_mut_slice(), &header)?;
        Ok((header, metadata, buf))
    }

    pub(super) fn header_stage(mut self, filter: &Bloom) -> Result<Self> {
        if let Some(record_header) = self.headers_btree.values().next().and_then(|v| v.first()) {
            let record_header_size = record_header.serialized_size().try_into()?;
            let filter_buf = filter.to_raw()?;
            let headers_len = self
                .headers_btree
                .iter()
                .fold(0, |acc, (_k, v)| acc + v.len());
            let header = IndexHeader::new(record_header_size, headers_len, filter_buf.len());
            let key_size = record_header.key().len();
            self.header_stage = Some(HeaderStage {
                header,
                key_size,
                filter_buf,
            });
            Ok(self)
        } else {
            Err(anyhow!("BTree is empty, can't find info about key len!"))
        }
    }

    pub(super) fn leaves_stage(mut self) -> Result<Self> {
        let HeaderStage {
            header,
            filter_buf: _,
            key_size,
        } = self
            .header_stage
            .as_ref()
            .ok_or_else(|| anyhow!("Previous stages were not done"))?;
        let hs = header.serialized_size()? as usize;
        let fsize = header.filter_buf_size;
        let msize: usize = TreeMeta::serialized_size_default()? as usize;
        let headers_start_offset = (hs + fsize) as u64;
        let headers_size = header.records_count * header.record_header_size;
        let leaves_offset = headers_start_offset + (headers_size + msize) as u64;
        // leaf contains pair: (key, offset in file for first record's header with this key)
        let leaf_size = key_size + std::mem::size_of::<u64>();
        let leaves_buf = Self::serialize_leaves(
            self.headers_btree,
            headers_start_offset,
            leaf_size,
            header.record_header_size,
        );
        self.leaves_stage = Some(LeavesStage {
            leaf_size,
            leaves_buf,
            leaves_offset,
            headers_size,
        });
        Ok(self)
    }

    pub(super) fn tree_stage(mut self) -> Result<Self> {
        let LeavesStage {
            leaves_offset,
            leaves_buf,
            leaf_size,
            headers_size: _,
        } = self
            .leaves_stage
            .as_ref()
            .ok_or_else(|| anyhow!("Previous stages were not done"))?;
        let keys = self.headers_btree.keys().collect();
        let tree_offset = leaves_offset + leaves_buf.len() as u64;
        let (root_offset, tree_buf) =
            Self::serialize_bptree(keys, *leaves_offset, *leaf_size as u64, tree_offset)?;
        let metadata = TreeMeta::new(root_offset, *leaves_offset, tree_offset);
        let meta_buf = serialize(&metadata)?;
        self.tree_stage = Some(TreeStage {
            metadata,
            meta_buf,
            tree_buf,
        });
        Ok(self)
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

    fn serialize_bptree(
        keys: Vec<&Vec<u8>>,
        leaves_offset: u64,
        leaf_size: u64,
        tree_offset: u64,
    ) -> Result<(u64, Vec<u8>)> {
        let max_amount = Self::max_leaf_node_capacity(keys[0].len());
        let nodes_amount = (keys.len() - 1) / max_amount + 1;
        let elems_in_node = (keys.len() / nodes_amount) as u64;
        trace!(
            "last node has {} less keys",
            elems_in_node - elems_in_node % nodes_amount as u64
        );

        let leaf_nodes_compressed = keys
            .chunks(elems_in_node as usize)
            .enumerate()
            .map(|(i, keys)| {
                let cur_offset = leaves_offset + elems_in_node * leaf_size * i as u64;
                (keys[0].clone(), cur_offset)
            })
            .collect::<Vec<(Vec<u8>, u64)>>();

        let mut buf = Vec::new();
        let root_offset = Self::build_tree(leaf_nodes_compressed, tree_offset, &mut buf)?;
        Ok((root_offset, buf))
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
        let elems_in_node = nodes_arr.len() / nodes_amount;
        let new_nodes = nodes_arr
            .chunks(elems_in_node)
            .map(|keys| {
                let offset = tree_offset + buf.len() as u64;
                let min_key = keys[0].0.clone();
                let offsets = keys.iter().map(|(_, offset)| *offset).collect();
                let keys = keys.iter().skip(1).map(|(key, _)| key.clone()).collect();
                let node = Node::new(keys, offsets);
                buf.extend_from_slice(&node.serialize().expect("failed to serialize node"));
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
