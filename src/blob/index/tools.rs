use super::prelude::*;

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

pub(crate) async fn binary_search(
    file: &File,
    key: &[u8],
    header: &IndexHeader,
) -> Result<Option<(RecordHeader, usize)>> {
    debug!("blob index simple binary search header {:?}", header);

    if key.is_empty() {
        error!("empty key was provided");
    }

    let mut start = 0;
    let mut end = header.records_count - 1;
    debug!("loop init values: start: {:?}, end: {:?}", start, end);

    while start <= end {
        let mid = (start + end) / 2;
        let mid_record_header = read_at(file, mid, &header).await?;
        debug!(
            "blob index simple binary search mid header: {:?}",
            mid_record_header
        );
        let cmp = mid_record_header.key().cmp(&key);
        debug!("mid read: {:?}, key: {:?}", mid_record_header.key(), key);
        debug!("before mid: {:?}, start: {:?}, end: {:?}", mid, start, end);
        match cmp {
            CmpOrdering::Greater if mid > 0 => end = mid - 1,
            CmpOrdering::Equal => {
                return Ok(Some((mid_record_header, mid)));
            }
            CmpOrdering::Less => start = mid + 1,
            other => {
                debug!("binary search not found, cmp: {:?}, mid: {}", other, mid);
                return Ok(None);
            }
        };
        debug!("after mid: {:?}, start: {:?}, end: {:?}", mid, start, end);
    }
    debug!("record with key: {:?} not found", key);
    Ok(None)
}

pub(crate) async fn search_all(
    file: &File,
    key: &[u8],
    index_header: &IndexHeader,
) -> Result<Option<Vec<RecordHeader>>> {
    if let Some(header_pos) = binary_search(file, key, index_header)
        .await
        .with_context(|| "blob, index simple, search all, binary search failed")?
    {
        let orig_pos = header_pos.1;
        debug!(
            "blob index simple search all total {}, pos {}",
            index_header.records_count, orig_pos
        );
        let mut headers = vec![header_pos.0];
        // go left
        let mut pos = orig_pos;
        debug!(
            "blob index simple search all headers {}, pos {}",
            headers.len(),
            pos
        );
        while pos > 0 {
            pos -= 1;
            debug!(
                "blob index simple search all headers {}, pos {}",
                headers.len(),
                pos
            );
            let rh = read_at(file, pos, &index_header)
                .await
                .with_context(|| "blob, index simple, search all, read at failed")?;
            if rh.key() == key {
                headers.push(rh);
            } else {
                break;
            }
        }
        debug!(
            "blob index simple search all headers {}, pos {}",
            headers.len(),
            pos
        );
        //go right
        pos = orig_pos + 1;
        while pos < index_header.records_count {
            debug!(
                "blob index simple search all headers {}, pos {}",
                headers.len(),
                pos
            );
            let rh = read_at(file, pos, &index_header)
                .await
                .with_context(|| "blob, index simple, search all, read at failed")?;
            if rh.key() == key {
                headers.push(rh);
                pos += 1;
            } else {
                break;
            }
        }
        debug!(
            "blob index simple search all headers {}, pos {}",
            headers.len(),
            pos
        );
        Ok(Some(headers))
    } else {
        debug!("Record not found by binary search on disk");
        Ok(None)
    }
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
        let mut buf = Vec::with_capacity(hs + headers.len() * record_header_size);
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
        let new_header = IndexHeader::with_hash(record_header_size, headers.len(), filter_buf.len(), hash);
        serialize_into(buf.as_mut_slice(), &new_header)?;
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
