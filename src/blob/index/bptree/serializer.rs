use super::prelude::*;
use serde::Serialize;

pub(super) struct HeaderStage<
    'a,
    K: AsRef<[u8]>,
    V: Serialize + 'a,
    It: Iterator<Item = (K, &'a Vec<V>)> + Clone,
> {
    headers_iter: It,
    header: IndexHeader,
    key_size: usize,
    meta: Vec<u8>,
}

pub(super) struct LeavesStage<
    'a,
    K: AsRef<[u8]>,
    V: Serialize + 'a,
    It: Iterator<Item = (K, &'a Vec<V>)> + Clone,
> {
    headers_iter: It,
    header: IndexHeader,
    leaf_size: usize,
    leaves_buf: Vec<u8>,
    leaves_offset: u64,
    headers_size: usize,
    meta: Vec<u8>,
}

pub(super) struct TreeStage<
    'a,
    K: AsRef<[u8]>,
    V: Serialize + 'a,
    It: Iterator<Item = (K, &'a Vec<V>)> + Clone,
> {
    headers_iter: It,
    metadata: TreeMeta,
    header: IndexHeader,
    meta_buf: Vec<u8>,
    tree_buf: Vec<u8>,
    leaves_buf: Vec<u8>,
    headers_size: usize,
    meta: Vec<u8>,
}

pub(super) struct Serializer<
    'a,
    K: AsRef<[u8]>,
    V: Serialize + 'a,
    It: Iterator<Item = (K, &'a Vec<V>)> + Clone,
> {
    headers_iter: It,
}

impl<'a, K: AsRef<[u8]>, V: Serialize + 'a, It: Iterator<Item = (K, &'a Vec<V>)> + Clone>
    Serializer<'a, K, V, It>
{
    pub(super) fn new(headers_iter: It) -> Self {
        Self { headers_iter }
    }
    pub(super) fn header_stage(self, meta: Vec<u8>) -> Result<HeaderStage<'a, K, V, It>> {
        if let Some((key, Some(record_header))) = self
            .headers_iter
            .clone()
            .next()
            .and_then(|(k, v)| Some((k, v.first())))
        {
            let record_header_size = bincode::serialized_size(record_header).map(|s| s as usize)?;
            let headers_len = self
                .headers_iter
                .clone()
                .fold(0, |acc, (_k, v)| acc + v.len());
            let header = IndexHeader::new(record_header_size, headers_len, meta.len());
            let key_size = key.as_ref().len();
            Ok(HeaderStage {
                headers_iter: self.headers_iter,
                header,
                key_size,
                meta,
            })
        } else {
            Err(anyhow!("BTree is empty, can't find info about key len!"))
        }
    }
}

impl<'a, K: AsRef<[u8]>, V: Serialize + 'a, It: Iterator<Item = (K, &'a Vec<V>)> + Clone>
    HeaderStage<'a, K, V, It>
{
    pub(super) fn leaves_stage(self) -> Result<LeavesStage<'a, K, V, It>> {
        let hs = self.header.serialized_size()? as usize;
        let fsize = self.header.meta_size;
        let msize: usize = TreeMeta::serialized_size_default()? as usize;
        let headers_start_offset = (hs + fsize) as u64;
        let headers_size = self.header.records_count * self.header.record_header_size;
        let leaves_offset = headers_start_offset + (headers_size + msize) as u64;
        // leaf contains pair: (key, offset in file for first record's header with this key)
        let leaf_size = self.key_size + std::mem::size_of::<u64>();
        let leaves_buf = Self::serialize_leaves(
            self.headers_iter.clone(),
            headers_start_offset,
            leaf_size,
            self.header.record_header_size,
        );
        Ok(LeavesStage {
            headers_iter: self.headers_iter,
            leaf_size,
            leaves_buf,
            leaves_offset,
            headers_size,
            meta: self.meta,
            header: self.header,
        })
    }

    fn serialize_leaves(
        headers_iter: It,
        headers_start_offset: u64,
        leaf_size: usize,
        record_header_size: usize,
    ) -> Vec<u8> {
        let mut leaves_buf = Vec::with_capacity(leaf_size * headers_iter.clone().count());
        headers_iter.fold(
            (headers_start_offset, &mut leaves_buf),
            |(offset, leaves_buf), (leaf, hds)| {
                leaves_buf.extend_from_slice(leaf.as_ref());
                let offsetb = serialize(&offset).expect("serialize u64");
                leaves_buf.extend_from_slice(&offsetb);
                let res = (offset + (hds.len() * record_header_size) as u64, leaves_buf);
                res
            },
        );
        leaves_buf
    }
}

impl<'a, K: AsRef<[u8]>, V: Serialize + 'a, It: Iterator<Item = (K, &'a Vec<V>)> + Clone>
    LeavesStage<'a, K, V, It>
{
    pub(super) fn tree_stage(self) -> Result<TreeStage<'a, K, V, It>> {
        // FIXME: remove collect (now it is used to iterate through `chunks` in `serialize_bptree`)
        let keys: Vec<K> = self.headers_iter.clone().map(|e| e.0).collect();
        let tree_offset = self.leaves_offset + self.leaves_buf.len() as u64;
        let (root_offset, tree_buf) =
            Self::serialize_bptree(keys, self.leaves_offset, self.leaf_size as u64, tree_offset)?;
        let metadata = TreeMeta::new(
            root_offset,
            self.leaves_offset,
            tree_offset,
            (self.leaf_size - std::mem::size_of::<u64>()) as u64,
        );
        let meta_buf = serialize(&metadata)?;
        Ok(TreeStage {
            headers_iter: self.headers_iter,
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
        keys: Vec<K>,
        leaves_offset: u64,
        leaf_size: u64,
        tree_offset: u64,
    ) -> Result<(u64, Vec<u8>)> {
        let max_amount = Self::max_leaf_node_capacity(keys[0].as_ref().len());
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
                (keys[0].as_ref().to_vec(), cur_offset)
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

    fn max_leaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        BLOCK_SIZE / (key_size + offset_size) - 1
    }

    fn max_nonleaf_node_capacity(key_size: usize) -> usize {
        let offset_size = std::mem::size_of::<u64>();
        let meta_size = std::mem::size_of::<NodeMeta>();
        (BLOCK_SIZE - meta_size - offset_size) / (key_size + offset_size) + 1
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

impl<'a, K: AsRef<[u8]>, V: Serialize + 'a, It: Iterator<Item = (K, &'a Vec<V>)> + Clone>
    TreeStage<'a, K, V, It>
{
    pub(super) fn build(self) -> Result<(IndexHeader, TreeMeta, Vec<u8>)> {
        let hs = self.header.serialized_size()? as usize;
        let fsize = self.header.meta_size;
        let msize = self.meta_buf.len();
        let data_size =
            hs + fsize + self.headers_size + msize + self.leaves_buf.len() + self.tree_buf.len();
        let mut buf = Vec::with_capacity(data_size);
        serialize_into(&mut buf, &self.header)?;
        buf.extend_from_slice(&self.meta);
        Self::append_headers(self.headers_iter, &mut buf)?;
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

    fn append_headers(headers_iter: It, buf: &mut Vec<u8>) -> Result<()> {
        headers_iter
            .flat_map(|r| r.1)
            .map(|h| serialize(&h))
            .try_fold(buf, |buf, h_buf| -> Result<_> {
                buf.extend_from_slice(&h_buf?);
                Ok(buf)
            })?;
        Ok(())
    }
}
