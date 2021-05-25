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
    let key_size = record_headers.keys().next().map_or_else(
        || 0,
        |k| {
            assert!(!k.is_empty(), "key can't be empty!");
            k.len()
        },
    );
    let mut attrs: MemoryAttrs = Default::default();
    set_key_related_fields(&mut attrs, key_size);
    attrs.records_allocated = record_headers.values().fold(0, |acc, v| acc + v.capacity());
    attrs.records_count = records_count;
    attrs
}

pub(crate) fn set_key_related_fields(attrs: &mut MemoryAttrs, key_size: usize) {
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
