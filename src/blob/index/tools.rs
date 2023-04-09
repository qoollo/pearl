use super::prelude::*;
use std::mem::size_of;

/// Hash calculation helper for index
pub(crate) struct IndexHashCalculator;

impl IndexHashCalculator {
    pub(crate) const HASH_LENGTH: usize = 32; 

    pub(crate) fn get_hash(buf: &[u8]) -> Vec<u8> {
        use sha2::{Sha256, Digest};
        let digest = Sha256::digest(buf);
        digest.to_vec()
    }
}

// if there is no elements, data will be wrong (because we can't get key_size),
// BUT it will be computed correctly during first push
pub(crate) fn compute_mem_attrs<K>(
    record_headers: &InMemoryIndex<K>,
    records_count: usize,
) -> MemoryAttrs
where
    for<'a> K: Key<'a>,
{
    let mut attrs: MemoryAttrs = Default::default();
    set_key_related_fields::<K>(&mut attrs);
    attrs.records_allocated = record_headers.values().fold(0, |acc, v| acc + v.capacity());
    attrs.records_count = records_count;
    attrs
}

pub(crate) fn set_key_related_fields<K>(attrs: &mut MemoryAttrs)
where
    for<'a> K: Key<'a>,
{
    let key_size = K::LEN as usize;
    attrs.key_size = key_size;
    attrs.btree_entry_size = size_of::<Vec<u8>>() + key_size + size_of::<Vec<RecordHeader>>();
    attrs.record_header_size = size_of::<RecordHeader>() + key_size;
}

pub(crate) fn clean_file(path: impl AsRef<Path>, recreate_index_file: bool) -> Result<()> {
    if !path.as_ref().exists() {
        Ok(())
    } else if recreate_index_file {
        StdFile::create(path).map(|_| ()).map_err(Into::into)
    } else {
        let msg = "Clean file is not permitted";
        error!("{}", msg);
        Err(anyhow!(msg))
    }
}


#[cfg(test)]
mod tests {
    use super::IndexHashCalculator;

    #[test]
    pub fn test_hash_compatibility() {
        let data_vec: Vec<u8> = (0..1024).into_iter().map(|i| (i % 256) as u8).collect();
        // SHA256 hash calculated with ring crate
        let expected_hash = vec![120, 91, 7, 81, 252, 44, 83, 220, 20, 164, 206, 61, 128, 14, 105, 239, 156, 225, 0, 158, 179, 39, 204, 244, 88, 175, 224, 156, 36, 44, 38, 201];
        let actual_hash = IndexHashCalculator::get_hash(&data_vec);

        assert_eq!(expected_hash, actual_hash);
    }
}