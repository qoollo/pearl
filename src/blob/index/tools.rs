use super::prelude::*;
use std::mem::size_of;

pub(crate) async fn read_at(
    file: &File,
    index: usize,
    header: &IndexHeader,
) -> Result<RecordHeader> {
    debug!("blob index simple read at");
    let header_size = bincode::serialized_size(&header)?;
    debug!("blob index simple read at header size {}", header_size);
    let offset =
        header_size + header.filter_buf_size as u64 + (header.record_header_size * index) as u64;
    let mut buf = vec![0; header.record_header_size];
    debug!(
        "blob index simple offset: {}, buf len: {}",
        offset,
        buf.len()
    );
    file.read_at(&mut buf, offset).await?;
    let header = deserialize(&buf)?;
    debug!("blob index simple header: {:?}", header);
    Ok(header)
}

pub(crate) fn serialize_record_headers(
    headers: &InMemoryIndex,
    filter: &Bloom,
) -> Result<Option<(IndexHeader, Vec<u8>)>> {
    debug!("blob index simple serialize headers");
    if let Some(record_header) = headers.values().next().and_then(|v| v.first()) {
        debug!("index simple serialize headers first: {:?}", record_header);
        let record_header_size = record_header.serialized_size().try_into()?;
        trace!("record header serialized size: {}", record_header_size);
        let headers = headers.iter().flat_map(|r| r.1).collect::<Vec<_>>(); // produce sorted
        debug!("blob index simple serialize bunch transform BTreeMap into Vec");
        //bunch.sort_by_key(|h| h.key().to_vec());
        let filter_buf = filter.to_raw()?;
        let header = IndexHeader::new(record_header_size, headers.len(), filter_buf.len());
        let hs: usize = header.serialized_size()?.try_into().expect("u64 to usize");
        trace!("index header size: {}b", hs);
        let fsize = header.filter_buf_size;
        let mut buf = Vec::with_capacity(hs + fsize + headers.len() * record_header_size);
        serialize_into(&mut buf, &header)?;
        debug!(
            "serialize headers filter serialized_size: {}, header.filter_buf_size: {}, buf.len: {}",
            filter_buf.len(),
            header.filter_buf_size,
            buf.len()
        );
        buf.extend_from_slice(&filter_buf);
        headers
            .iter()
            .filter_map(|h| serialize(&h).ok())
            .fold(&mut buf, |acc, h_buf| {
                acc.extend_from_slice(&h_buf);
                acc
            });
        debug!(
            "blob index simple serialize headers buf len after: {}",
            buf.len()
        );
        let hash = get_hash(&buf);
        let header =
            IndexHeader::with_hash(record_header_size, headers.len(), filter_buf.len(), hash);
        serialize_into(buf.as_mut_slice(), &header)?;
        Ok(Some((header, buf)))
    } else {
        Ok(None)
    }
}

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
