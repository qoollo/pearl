use bytes::{BufMut, BytesMut};

use crate::error::ValidationErrorKind;

use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct SimpleFileIndex {
    file: File,
    header: IndexHeader,
}

#[async_trait::async_trait]
impl<K> FileIndexTrait<K> for SimpleFileIndex
where
    for<'a> K: Key<'a>,
{
    async fn from_file(name: FileName, iodriver: IoDriver) -> Result<Self> {
        trace!("open index file");
        let file = iodriver
            .open(name.to_path())
            .await
            .context(format!("failed to open index file: {}", name))?;
        let header = Self::read_index_header(&file).await?;

        Ok(Self { file, header })
    }

    fn file_size(&self) -> u64 {
        self.file.size()
    }

    fn records_count(&self) -> usize {
        self.header.records_count
    }

    fn blob_size(&self) -> u64 {
        self.header.blob_size()
    }

    async fn read_meta(&self) -> Result<BytesMut> {
        trace!("load meta");
        trace!("read meta into buf: [0; {}]", self.header.meta_size);
        self.file
            .read_exact_at_allocate(self.header.meta_size, self.header.serialized_size())
            .await
    }

    async fn read_meta_at(&self, i: u64) -> Result<u8> {
        trace!("load byte from meta");
        if i >= self.header.meta_size as u64 {
            return Err(anyhow::anyhow!("read meta out of range"));
        }
        let buf = self
            .file
            .read_exact_at_allocate(1, self.header.serialized_size() + i)
            .await?;
        Ok(buf[0])
    }

    async fn find_by_key(&self, key: &K) -> Result<Option<Vec<RecordHeader>>> {
        Self::search_all(&self.file, key, &self.header).await
    }

    async fn from_records(
        path: &Path,
        iodriver: IoDriver,
        headers: &InMemoryIndex<K>,
        meta: Vec<u8>,
        recreate_index_file: bool,
        blob_size: u64,
    ) -> Result<Self> {
        let res = Self::serialize(headers, meta, blob_size)?;
        if res.is_none() {
            error!("Indices are empty!");
            return Err(anyhow!("empty in-memory indices".to_string()));
        }
        let (mut header, buf) = res.expect("None case is checked");
        clean_file(path, recreate_index_file)?;
        let file = iodriver
            .create(path)
            .await
            .with_context(|| format!("file open failed {:?}", path))?;
        file.write_append_all(buf.freeze()).await?;
        header.set_written(true);
        let size = header.serialized_size();
        let mut serialized_header = BytesMut::with_capacity(size as usize);
        serialize_into((&mut serialized_header).writer(), &header)?;
        file.write_all_at(0, serialized_header.freeze()).await?;
        file.fsyncdata().await?;
        Ok(Self { file, header })
    }

    async fn get_records_headers(&self, blob_size: u64) -> Result<(InMemoryIndex<K>, usize)> {
        let mut buf = self.file.read_all().await?;
        self.validate_header::<K>(&mut buf, blob_size).await?;
        let offset = self.header.meta_size + self.header.serialized_size() as usize;
        let records_buf = &buf[offset..];
        (0..self.header.records_count)
            .try_fold(InMemoryIndex::new(), |mut headers, i| {
                let offset = i * self.header.record_header_size;
                let header: RecordHeader = deserialize(&records_buf[offset..])?;
                let key = header.key().to_vec().into();
                // We use get mut instead of entry(..).or_insert(..) because in second case we
                // need to clone header.
                if let Some(v) = headers.get_mut(&key) {
                    v.push(header)
                } else {
                    headers.insert(key, vec![header]);
                }
                Ok(headers)
            })
            .map(|headers| (headers, self.header.records_count))
    }

    async fn get_any(&self, key: &K) -> Result<Option<RecordHeader>> {
        Self::binary_search(&self.file, key, &self.header)
            .await
            .map(|res| res.map(|h| h.0))
    }

    fn validate(&self, blob_size: u64) -> Result<()> {
        // FIXME: check hash here?
        if !self.header.is_written() {
            let param = ValidationErrorKind::IndexNotWritten;
            return Err(
                Error::validation(param, "Index is incomplete (no 'is_written' flag)").into(),
            );
        }
        if self.header.version() != HEADER_VERSION {
            let param = ValidationErrorKind::IndexVersion;
            return Err(Error::validation(param, "Index Header version is not valid").into());
        }
        if self.header.key_size() != K::LEN {
            let param = ValidationErrorKind::IndexKeySize;
            return Err(Error::validation(
                param,
                "Index header key_size is not equal to pearl compile-time key size",
            )
            .into());
        }
        if self.header.blob_size() != blob_size {
            let param = ValidationErrorKind::IndexBlobSize;
            return Err(Error::validation(
                param,
                format!(
                    "Index Header is for blob of size {}, but actual blob size is {}",
                    self.header.blob_size(),
                    blob_size
                ),
            )
            .into());
        }
        if self.header.magic_byte() != INDEX_HEADER_MAGIC_BYTE {
            let param = ValidationErrorKind::IndexMagicByte;
            return Err(Error::validation(param, "Index magic byte is not valid").into());
        }
        Ok(())
    }

    fn memory_used(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

// helpers
impl SimpleFileIndex {
    fn hash_valid(header: &IndexHeader, buf: &mut [u8]) -> Result<bool> {
        let hash = &header.hash;
        let mut header = header.clone();
        header.reset_hash();
        header.set_written(false);
        serialize_into(&mut buf[..], &header)?;
        let new_hash = IndexHashCalculator::get_hash(&buf);
        Ok(*hash == new_hash)
    }

    async fn read_index_header(file: &File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default() as usize;
        let buf = file.read_exact_at_allocate(header_size, 0).await
            .map_err(unexpected_eof_converter)?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    async fn search_all<K>(
        file: &File,
        key: &K,
        index_header: &IndexHeader,
    ) -> Result<Option<Vec<RecordHeader>>>
    where
        for<'a> K: Key<'a>,
    {
        if let Some(header_pos) = Self::binary_search(file, key, index_header)
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
                let rh = Self::read_at(file, pos, &index_header)
                    .await
                    .with_context(|| "blob, index simple, search all, read at failed")?;
                if rh.key() == key.as_ref() {
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
                let rh = Self::read_at(file, pos, &index_header)
                    .await
                    .with_context(|| "blob, index simple, search all, read at failed")?;
                if rh.key() == key.as_ref() {
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

    async fn binary_search<K>(
        file: &File,
        key: &K,
        header: &IndexHeader,
    ) -> Result<Option<(RecordHeader, usize)>>
    where
        for<'a> K: Key<'a>,
    {
        debug!("blob index simple binary search header {:?}", header);

        let mut start = 0;
        let mut end = header.records_count - 1;
        debug!("loop init values: start: {:?}, end: {:?}", start, end);

        while start <= end {
            let mid = (start + end) / 2;
            let mid_record_header = Self::read_at(file, mid, &header).await?;
            debug!(
                "blob index simple binary search mid header: {:?}",
                mid_record_header
            );
            let cmp = key.as_ref_key().cmp(&mid_record_header.key().into());
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

    async fn validate_header<K>(&self, buf: &mut [u8], blob_size: u64) -> Result<()>
    where
        for<'a> K: Key<'a>,
    {
        FileIndexTrait::<K>::validate(self, blob_size)?;
        if !Self::hash_valid(&self.header, buf)? {
            let param = ValidationErrorKind::IndexChecksum;
            return Err(Error::validation(param, "header hash mismatch").into());
        }
        Ok(())
    }

    fn serialize<K>(
        headers: &InMemoryIndex<K>,
        meta: Vec<u8>,
        blob_size: u64,
    ) -> Result<Option<(IndexHeader, BytesMut)>>
    where
        for<'a> K: Key<'a>,
    {
        debug!("blob index simple serialize headers");
        if let Some(record_header) = headers.values().next().and_then(|v| v.first()) {
            debug!("index simple serialize headers first: {:?}", record_header);
            let record_header_size = record_header.serialized_size().try_into()?;
            trace!("record header serialized size: {}", record_header_size);
            let headers = headers.iter().flat_map(|r| r.1).collect::<Vec<_>>(); // produce sorted
            let header = IndexHeader::new(
                record_header_size,
                headers.len(),
                meta.len(),
                K::LEN,
                blob_size,
            );
            let hs: usize = header.serialized_size().try_into().expect("u64 to usize");
            trace!("index header size: {}b", hs);
            let fsize = header.meta_size;
            let mut buf = BytesMut::with_capacity(hs + fsize + headers.len() * record_header_size);
            serialize_into((&mut buf).writer(), &header)?;
            debug!(
                "serialize headers meta size: {}, header.meta_size: {}, buf.len: {}",
                meta.len(),
                header.meta_size,
                buf.len()
            );
            buf.extend_from_slice(&meta);
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
            let hash = IndexHashCalculator::get_hash(&buf);
            let header = IndexHeader::with_hash(
                record_header_size,
                headers.len(),
                meta.len(),
                header.key_size(),
                blob_size,
                hash,
            );
            serialize_into((&mut buf).writer(), &header)?;
            Ok(Some((header, buf)))
        } else {
            Ok(None)
        }
    }

    async fn read_at(file: &File, index: usize, header: &IndexHeader) -> Result<RecordHeader> {
        debug!("blob index simple read at");
        let header_size = bincode::serialized_size(&header)?;
        debug!("blob index simple read at header size {}", header_size);
        let offset =
            header_size + header.meta_size as u64 + (header.record_header_size * index) as u64;
        debug!(
            "blob index simple offset: {}, buf len: {}",
            offset, header.record_header_size
        );
        let buf = file
            .read_exact_at_allocate(header.record_header_size, offset)
            .await
            .map_err(|e| unexpected_eof_converter_ctx(e, 
                format!("failed to read, offset: {}", offset)))?;
        let header = deserialize(&buf)?;
        debug!("blob index simple header: {:?}", header);
        Ok(header)
    }
}
