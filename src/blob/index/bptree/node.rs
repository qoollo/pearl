use super::prelude::*;

#[derive(Debug, PartialEq)]
pub(super) struct Node {
    keys: Vec<Vec<u8>>,
    offsets: Vec<u64>,
}

impl Node {
    pub(super) fn new(keys: Vec<Vec<u8>>, offsets: Vec<u64>) -> Self {
        assert!(keys.len() != 0);
        assert_eq!(keys.len() + 1, offsets.len());
        Self { keys, offsets }
    }

    pub(super) fn key_offset(&self, key: &[u8]) -> u64 {
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

    pub(super) fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let meta = NodeMeta::new(self.keys.len() as u64);
        buf.extend_from_slice(&serialize(&meta)?);
        self.keys.iter().fold(&mut buf, |acc, key| {
            acc.extend_from_slice(&key);
            acc
        });
        self.offsets.iter().try_fold(buf, |mut acc, offset| {
            acc.extend_from_slice(&serialize(offset)?);
            Ok(acc)
        })
    }

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

    fn create_node(amount: usize, f: impl Fn(usize) -> KeyType) -> Node {
        let keys = (0..amount).map(|e| serialize(&f(e)).unwrap()).collect();
        let offsets = (0u64..(amount as u64 + 1)).collect();
        Node::new(keys, offsets)
    }

    #[test]
    fn serialize_deserialize_test() {
        const KEYS_AMOUNTS: [usize; 3] = [1, 2, 100];
        for &keys_amount in KEYS_AMOUNTS.iter() {
            let node = create_node(keys_amount, |e| e as KeyType);
            let buf = node.serialize().unwrap();
            let node_deserialized =
                Node::deserialize(&buf, std::mem::size_of::<KeyType>() as u64).unwrap();
            assert_eq!(node, node_deserialized);
        }
    }

    fn check_offset(keys: &[Vec<u8>], key: &[u8], offset: usize) {
        if offset != 0 {
            assert!(key >= &keys[offset - 1]);
        }
        if offset != keys.len() {
            assert!(key < &keys[offset])
        }
    }

    #[test]
    fn key_offset_test() {
        const REQUESTS: Range<usize> = 0..100;
        const KEYS_AMOUNTS: [usize; 8] = [1, 2, 13, 13, 14, 14, 1, 2];
        for (i, &keys_amount) in KEYS_AMOUNTS.iter().enumerate() {
            let node = create_node(keys_amount, |e| (e * i) as KeyType);
            for k in REQUESTS.map(|v| serialize(&v).unwrap()) {
                let offset = node.key_offset(&k);
                check_offset(&node.keys, &k, offset as usize);
            }
        }
    }
}
