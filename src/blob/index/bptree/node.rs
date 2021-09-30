use super::prelude::*;
use std::mem::size_of;

#[derive(Debug, PartialEq, Clone)]
pub(super) struct Node {
    keys: Vec<Vec<u8>>,
    offsets: Vec<u64>,
}

impl Node {
    pub(super) fn new(keys: Vec<Vec<u8>>, offsets: Vec<u64>) -> Self {
        assert!(!keys.is_empty());
        assert_eq!(keys.len() + 1, offsets.len());
        Self { keys, offsets }
    }

    pub(super) fn new_serialized<'a>(
        keys: impl Iterator<Item = &'a [u8]>,
        offsets: impl Iterator<Item = u64>,
        key_size: usize,
        keys_amount: usize,
    ) -> Result<Vec<u8>> {
        let meta_buf = serialize(&NodeMeta::new(keys_amount as u64))?;
        let buf_size = Self::serialized_size_with_keys(key_size, keys_amount)?;
        let mut buf = Vec::with_capacity(buf_size as usize);
        buf.extend_from_slice(&meta_buf);
        keys.for_each(|k| buf.extend_from_slice(k));
        offsets
            .map(|off| serialize(&off))
            .try_for_each(|res| Result::<_>::Ok(buf.extend_from_slice(&res?)))?;
        Ok(buf)
    }

    pub(super) fn serialized_size_with_keys(key_size: usize, keys_amount: usize) -> Result<u64> {
        let meta_size = NodeMeta::serialized_size_default()?;
        let keys_buf_size = key_size * keys_amount;
        let offsets_buf_size = (keys_amount + 1) * size_of::<u64>();
        Ok(meta_size + (keys_buf_size + offsets_buf_size) as u64)
    }

    pub(super) fn binary_search_serialized(key: &[u8], buf: &[u8]) -> Result<usize, usize> {
        let mut l = 0i32;
        let mut r: i32 = (buf.len() / key.len() - 1) as i32;
        while l <= r {
            let m = (l + r) / 2;
            let offset = m as usize * key.len();
            match key.cmp(&buf[offset..(offset + key.len())]) {
                CmpOrdering::Less => r = m - 1,
                CmpOrdering::Greater => l = m + 1,
                CmpOrdering::Equal => return Ok(m as usize),
            }
        }
        Err(l as usize)
    }

    pub(super) fn key_offset_serialized(buf: &[u8], key: &[u8]) -> Result<u64> {
        let meta_size = NodeMeta::serialized_size_default()? as usize;
        let node_size = deserialize::<NodeMeta>(&buf[..meta_size])?.size as usize;
        let offsets_offset = meta_size + node_size * key.len();
        let ind = match Self::binary_search_serialized(key, &buf[meta_size..offsets_offset]) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };
        let offset = offsets_offset + ind * size_of::<u64>();
        deserialize(&buf[offset..(offset + size_of::<u64>())]).map_err(Into::into)
    }

    #[allow(dead_code)]
    pub(super) fn key_offset(&self, key: &[u8]) -> u64 {
        match self.keys.binary_search_by(|elem| elem.as_slice().cmp(key)) {
            Ok(pos) => self.offsets[pos + 1],
            Err(pos) => self.offsets[pos],
        }
    }

    #[allow(dead_code)]
    pub(super) fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let meta = NodeMeta::new(self.keys.len() as u64);
        buf.extend_from_slice(&serialize(&meta)?);
        buf.extend(self.keys.iter().flatten());
        self.offsets.iter().try_fold(buf, |mut acc, offset| {
            acc.extend_from_slice(&serialize(offset)?);
            Ok(acc)
        })
    }

    #[allow(dead_code)]
    pub(super) fn deserialize(buf: &[u8], key_size: u64) -> Result<Self> {
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
            .map(|bytes| deserialize::<u64>(bytes).map_err(Into::into))
            .collect::<Result<Vec<u64>>>()?;
        Ok(Node::new(keys, offsets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;

    type KeyType = usize;

    fn create_node(amount: usize, kf: impl Fn(usize) -> KeyType, of: impl Fn(u64) -> u64) -> Node {
        let keys = (0..amount).map(|e| serialize(&kf(e)).unwrap()).collect();
        let offsets = (0u64..(amount as u64 + 1)).map(of).collect();
        Node::new(keys, offsets)
    }

    #[test]
    fn serialize_deserialize_node() {
        const KEYS_AMOUNTS: [usize; 3] = [1, 2, 100];
        for &keys_amount in KEYS_AMOUNTS.iter() {
            let node = create_node(keys_amount, |e| e as KeyType, |o| o * 2);
            let buf = node.serialize().unwrap();
            let node_deserialized =
                Node::deserialize(&buf, std::mem::size_of::<KeyType>() as u64).unwrap();
            assert_eq!(node, node_deserialized);
        }
    }

    fn check_offset(keys: &[Vec<u8>], key: &[u8], offset: u64, off_to_index: impl Fn(u64) -> u64) {
        let offset_index = off_to_index(offset) as usize;
        if offset_index != 0 {
            assert!(key.cmp(&keys[offset_index - 1]).is_ge());
        }
        if offset_index != keys.len() {
            assert!(key.cmp(keys[offset_index].as_ref()).is_lt())
        }
    }

    #[test]
    fn node_key_offset() {
        const SCALE: u64 = 113;
        const REQUESTS: Range<usize> = 0..100;
        const KEYS_AMOUNTS: [usize; 8] = [1, 2, 13, 13, 14, 14, 1, 2];

        for (i, &keys_amount) in KEYS_AMOUNTS.iter().enumerate() {
            let node = create_node(keys_amount, |e| (e * i) as KeyType, |o| o * SCALE);
            for k in REQUESTS.map(|v| serialize(&v).unwrap()) {
                let offset = node.key_offset(&k);
                check_offset(&node.keys, &k, offset, |o| o / SCALE);
            }
        }
    }
}
