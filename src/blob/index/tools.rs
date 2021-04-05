use super::prelude::*;
use std::mem::size_of;

pub(crate) fn get_hash(buf: &[u8]) -> Vec<u8> {
    use ring::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(buf);
    let digest = context.finish();
    digest.as_ref().to_vec()
}

// if there is no elements, data will be wrong (because we can't get key_size),
// BUT it will be computed correctly during first push
pub(crate) fn compute_mem_attrs(
    record_headers: &InMemoryIndex,
    records_count: usize,
) -> MemoryAttrs {
    let key_size = record_headers.keys().next().map_or_else(|| 0, |v| v.len());
    let btree_entry_size = size_of::<Vec<u8>>() + key_size + size_of::<Vec<RecordHeader>>();
    let records_allocated = record_headers.values().fold(0, |acc, v| acc + v.capacity());
    let record_header_size = size_of::<RecordHeader>() + key_size;
    MemoryAttrs {
        key_size,
        btree_entry_size,
        records_allocated,
        records_count,
        record_header_size,
    }
}

pub(crate) fn clean_file(path: impl AsRef<Path>, recreate_index_file: bool) -> Result<()> {
    if recreate_index_file {
        StdFile::create(path).map(|_| ()).map_err(Into::into)
    } else {
        let msg = "Clean file not permitted";
        error!("{}", msg);
        Err(anyhow!(msg))
    }
}
