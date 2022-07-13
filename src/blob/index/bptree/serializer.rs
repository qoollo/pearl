use super::prelude::*;

type MinKeyWithOffset = (Vec<u8>, u64);
type NodesWithLayerSize = (Vec<MinKeyWithOffset>, u64);

pub(super) struct HeaderStage<'a, K>
where
    for<'b> K: Key<'b>,
{
    headers_btree: &'a InMemoryIndex<K>,
    header: IndexHeader,
    meta: Vec<u8>,
}

pub(super) struct TreeStage<'a, K>
where
    for<'b> K: Key<'b>,
{
    headers_btree: &'a InMemoryIndex<K>,
    metadata: TreeMeta,
    header: IndexHeader,
    meta_buf: Vec<u8>,
    tree_buf: Vec<u8>,
    headers_size: usize,
    meta: Vec<u8>,
}

pub(super) struct Serializer<'a, K>
where
    for<'b> K: Key<'b>,
{
    headers_btree: &'a InMemoryIndex<K>,
}

impl<'a, K> Serializer<'a, K>
where
    for<'b> K: Key<'b>,
{
    pub(super) fn new(headers_btree: &'a InMemoryIndex<K>) -> Self {
        Self { headers_btree }
    }
    pub(super) fn header_stage(self, meta: Vec<u8>, blob_size: u64) -> Result<HeaderStage<'a, K>> {
        if let Some(record_header) = self.headers_btree.values().next().and_then(|v| v.first()) {
            let record_header_size = record_header.serialized_size().try_into()?;
            let headers_len = self
                .headers_btree
                .iter()
                .fold(0, |acc, (_k, v)| acc + v.len());
            let header = IndexHeader::new(record_header_size, headers_len, meta.len(), blob_size);
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

impl<'a, K> HeaderStage<'a, K>
where
    for<'b> K: Key<'b> + 'static,
{
    pub(super) fn tree_stage(self) -> Result<TreeStage<'a, K>> {
        let hs = self.header.serialized_size()? as usize;
        let external_buf_size = self.header.meta_size;
        let meta_and_buf_end = (hs + external_buf_size) as u64;
        let headers_size = self.header.records_count * self.header.record_header_size;
        let tree_offset = meta_and_buf_end + TreeMeta::serialized_size_default()?;
        let tree_buf = Self::serialize_bptree(
            self.headers_btree,
            tree_offset,
            self.header.record_header_size as u64,
        )?;
        let leaves_offset = tree_offset + tree_buf.len() as u64;
        let metadata = TreeMeta::new(leaves_offset, tree_offset);
        let meta_buf = serialize(&metadata)?;
        Ok(TreeStage {
            headers_btree: self.headers_btree,
            metadata,
            meta_buf,
            tree_buf,
            header: self.header,
            headers_size,
            meta: self.meta,
        })
    }

    fn serialize_bptree(
        btree: &InMemoryIndex<K>,
        tree_offset: u64,
        record_header_size: u64,
    ) -> Result<Vec<u8>> {
        let mut leaf_nodes_compressed = Vec::new();
        let mut offset = 0;
        let mut remainder = BLOCK_SIZE as u64;
        let mut min_k = btree.keys().next().unwrap().clone();
        let mut min_o = offset;
        for (k, v) in btree.iter() {
            if remainder < record_header_size {
                leaf_nodes_compressed.push((min_k.to_vec(), min_o));
                min_k = k.clone();
                min_o = offset;
                remainder = BLOCK_SIZE as u64;
            }
            let delta_size = v.len() as u64 * record_header_size;
            offset += delta_size;
            remainder = remainder.saturating_sub(delta_size);
        }
        leaf_nodes_compressed.push((min_k.to_vec(), min_o));
        let mut buf = Vec::new();
        Self::build_tree(leaf_nodes_compressed, tree_offset, &mut buf)?;
        Ok(buf)
    }

    fn process_keys_portion(
        buf: &mut Vec<u8>,
        nodes_portion: &[MinKeyWithOffset],
        shift: u64,
    ) -> Result<()> {
        let offsets_iter = nodes_portion.iter().map(|(_, offset)| *offset + shift);
        let keys_iter = nodes_portion[1..].iter().map(|(k, _)| k.as_ref());
        let node_buf = Node::new_serialized(
            keys_iter,
            offsets_iter,
            nodes_portion[0].0.len(),
            nodes_portion.len() - 1,
        )?;
        buf.extend_from_slice(&node_buf);
        Ok(())
    }

    pub(super) fn build_tree(
        nodes_arr: Vec<MinKeyWithOffset>,
        tree_offset: u64,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        assert!(
            !nodes_arr.is_empty(),
            "Impossible to build tree without nodes"
        );
        if nodes_arr.len() == 1 {
            return Ok(());
        }
        let max_amount = Self::max_nonleaf_node_capacity(nodes_arr[0].0.len());
        let min_amount = (max_amount - 1) / 2 + 1;
        let (new_nodes, layer_size) =
            Self::collect_next_layer_nodes(&nodes_arr, (min_amount, max_amount))?;
        Self::build_tree(new_nodes, tree_offset, buf)?;
        let base_offset = tree_offset + layer_size + buf.len() as u64;
        Self::shift_all_and_write(buf, &nodes_arr, base_offset, (min_amount, max_amount))
    }

    fn shift_all_and_write(
        buf: &mut Vec<u8>,
        nodes_arr: &[MinKeyWithOffset],
        base_offset: u64,
        (min_amount, max_amount): (usize, usize),
    ) -> Result<()> {
        let mut current = 0;
        while nodes_arr.len() - current > max_amount {
            // amount == min_amount at least (if nodes_arr.len() - current == max_amount + 1)
            // and min operation is necessary to have at least min_amount nodes left
            let amount = std::cmp::min(max_amount, nodes_arr.len() - current - min_amount);
            let nodes_portion = &nodes_arr[current..(current + amount)];
            current += amount;
            Self::process_keys_portion(buf, nodes_portion, base_offset)?;
        }
        // min_amount <= nodes left <= max_amount
        let nodes_portion = &nodes_arr[current..];
        Self::process_keys_portion(buf, nodes_portion, base_offset)?;
        Ok(())
    }

    fn collect_next_layer_nodes(
        nodes_arr: &[MinKeyWithOffset],
        (min_amount, max_amount): (usize, usize),
    ) -> Result<NodesWithLayerSize> {
        let mut new_nodes = Vec::new();
        let mut current = 0;
        let mut current_offset = 0;
        while nodes_arr.len() - current > max_amount {
            // amount == min_amount at least (if nodes_arr.len() - current == max_amount + 1)
            // and min operation is necessary to have at least min_amount nodes left
            let amount = std::cmp::min(max_amount, nodes_arr.len() - current - min_amount);
            let nodes_portion = &nodes_arr[current..(current + amount)];
            current += amount;
            let compressed_node = (nodes_portion[0].0.clone(), current_offset);
            new_nodes.push(compressed_node);
            current_offset +=
                Node::serialized_size_with_keys(nodes_portion[0].0.len(), nodes_portion.len() - 1)?;
        }
        // min_amount <= nodes left <= max_amount
        let nodes_portion = &nodes_arr[current..];
        new_nodes.push((nodes_portion[0].0.clone(), current_offset));
        let layer_size = current_offset
            + Node::serialized_size_with_keys(nodes_portion[0].0.len(), nodes_portion.len() - 1)?;
        Ok((new_nodes, layer_size))
    }

    fn max_nonleaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        let meta_size =
            NodeMeta::serialized_size_default().expect("Can't retrieve default serialized size");
        (BLOCK_SIZE - meta_size as usize - offset_size) / (key_size + offset_size) + 1
    }
}

impl<'a, K> TreeStage<'a, K>
where
    for<'b> K: Key<'b>,
{
    pub(super) fn build(self) -> Result<(IndexHeader, TreeMeta, Vec<u8>)> {
        let hs = self.header.serialized_size()? as usize;
        let fsize = self.header.meta_size;
        let msize = self.meta_buf.len();
        let data_size = hs + fsize + self.headers_size + msize + self.tree_buf.len();
        let mut buf = Vec::with_capacity(data_size);
        serialize_into(&mut buf, &self.header)?;
        buf.extend_from_slice(&self.meta);
        buf.extend_from_slice(&self.meta_buf);
        buf.extend_from_slice(&self.tree_buf);
        Self::append_headers(self.headers_btree, &mut buf)?;
        let hash = get_hash(&buf);
        let header = IndexHeader::with_hash(
            self.header.record_header_size,
            self.header.records_count,
            self.meta.len(),
            hash,
            self.header.blob_size
        );
        serialize_into(buf.as_mut_slice(), &header)?;
        Ok((header, self.metadata, buf))
    }

    fn append_headers(headers_btree: &InMemoryIndex<K>, buf: &mut Vec<u8>) -> Result<()> {
        // headers are pushed in reversed order because it helps to perform something like update
        // operation: the latest written (the first after reverse) record will be retrieved from file
        headers_btree
            .iter()
            .flat_map(|r| r.1.iter().rev())
            .map(|h| serialize(&h))
            .try_fold(buf, |buf, h_buf| -> Result<_> {
                buf.extend_from_slice(&h_buf?);
                Ok(buf)
            })?;
        Ok(())
    }
}
