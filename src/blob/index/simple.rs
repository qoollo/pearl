use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct SimpleFileIndex {
    file: File,
    header: IndexHeader,
}

#[async_trait::async_trait]
impl FileIndexTrait for SimpleFileIndex {
    async fn from_file(name: FileName, ioring: Option<Rio>) -> Result<Self> {
        trace!("open index file");
        let file = File::open(name.to_path(), ioring)
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

    async fn read_meta(&self) -> Result<Vec<u8>> {
        trace!("load meta");
        let mut buf = vec![0; self.header.meta_size];
        trace!("read meta into buf: [0; {}]", buf.len());
        self.file
            .read_at(&mut buf, self.header.serialized_size()? as u64)
            .await?;
        Ok(buf)
    }

    async fn read_meta_at(&self, i: u64) -> Result<u8> {
        trace!("load byte from meta");
        if i >= self.header.meta_size as u64 {
            return Err(anyhow::anyhow!("read meta out of range"));
        }
        let mut buf = vec![0; 1];
        self.file
            .read_at(&mut buf, self.header.serialized_size()? + i)
            .await?;
        Ok(buf[0])
    }

    async fn find_by_key(&self, key: &[u8]) -> Result<Option<Vec<RecordHeader>>> {
        Self::search_all(&self.file, key, &self.header).await
    }

    async fn from_records(
        path: &Path,
        ioring: Option<Rio>,
        headers: &InMemoryIndex,
        meta: Vec<u8>,
        recreate_index_file: bool,
    ) -> Result<Self> {
        let res = Self::serialize(headers, meta)?;
        if res.is_none() {
            error!("Indices are empty!");
            return Err(anyhow!("empty in-memory indices".to_string()));
        }
        let (mut header, buf) = res.expect("None case is checked");
        clean_file(path, recreate_index_file)?;
        let file = File::create(path, ioring)
            .await
            .with_context(|| format!("file open failed {:?}", path))?;
        file.write_append(&buf).await?;
        header.set_written(true);
        let serialized_header = serialize(&header)?;
        file.write_at(0, &serialized_header).await?;
        file.fsyncdata().await?;
        Ok(Self { file, header })
    }

    async fn get_records_headers(&self) -> Result<(InMemoryIndex, usize)> {
        let mut buf = self.file.read_all().await?;
        self.validate_header(&mut buf).await?;
        let offset = self.header.meta_size + self.header.serialized_size()? as usize;
        let records_buf = &buf[offset..];
        (0..self.header.records_count)
            .try_fold(InMemoryIndex::new(), |mut headers, i| {
                let offset = i * self.header.record_header_size;
                let header: RecordHeader = deserialize(&records_buf[offset..])?;
                // We used get mut instead of entry(..).or_insert(..) because in second case we
                // need to clone key everytime. Whereas in first case - only if we insert new
                // entry.
                if let Some(v) = headers.get_mut(header.key()) {
                    v.push(header)
                } else {
                    headers.insert(header.key().to_vec(), vec![header]);
                }
                Ok(headers)
            })
            .map(|headers| (headers, self.header.records_count))
    }

    async fn get_any(&self, key: &[u8]) -> Result<Option<RecordHeader>> {
        Self::binary_search(&self.file, key, &self.header)
            .await
            .map(|res| res.map(|h| h.0))
    }

    fn validate(&self) -> Result<()> {
        if self.header.is_written() {
            Ok(())
        } else {
            Err(Error::validation("Index Header is not valid").into())
        }
    }
}

#[async_trait::async_trait]
impl BloomDataProvider for SimpleFileIndex {
    async fn read_byte(&self, index: u64) -> Result<u8> {
        self.read_meta_at(index).await
    }

    async fn read_all(&self) -> Result<Vec<u8>> {
        self.read_meta().await
    }
}

// helpers
impl SimpleFileIndex {
    fn hash_valid(header: &IndexHeader, buf: &mut Vec<u8>) -> Result<bool> {
        let hash = header.hash.clone();
        let mut header = header.clone();
        header.hash = vec![0; ring::digest::SHA256.output_len];
        header.set_written(false);
        serialize_into(buf.as_mut_slice(), &header)?;
        let new_hash = get_hash(&buf);
        Ok(hash == new_hash)
    }

    async fn read_index_header(file: &File) -> Result<IndexHeader> {
        let header_size = IndexHeader::serialized_size_default()? as usize;
        let mut buf = vec![0; header_size];
        file.read_at(&mut buf, 0).await?;
        IndexHeader::from_raw(&buf).map_err(Into::into)
    }

    async fn search_all(
        file: &File,
        key: &[u8],
        index_header: &IndexHeader,
    ) -> Result<Option<Vec<RecordHeader>>> {
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
                let rh = Self::read_at(file, pos, &index_header)
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

    async fn binary_search(
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
            let mid_record_header = Self::read_at(file, mid, &header).await?;
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

    async fn validate_header(&self, buf: &mut Vec<u8>) -> Result<()> {
        self.validate()?;
        if !Self::hash_valid(&self.header, buf)? {
            return Err(Error::validation("header hash mismatch").into());
        }
        if self.header.version() != HEADER_VERSION {
            return Err(Error::validation("header version mismatch").into());
        }
        Ok(())
    }

    fn serialize(headers: &InMemoryIndex, meta: Vec<u8>) -> Result<Option<(IndexHeader, Vec<u8>)>> {
        debug!("blob index simple serialize headers");
        if let Some(record_header) = headers.values().next().and_then(|v| v.first()) {
            debug!("index simple serialize headers first: {:?}", record_header);
            let record_header_size = record_header.serialized_size().try_into()?;
            trace!("record header serialized size: {}", record_header_size);
            let headers = headers.iter().flat_map(|r| r.1).collect::<Vec<_>>(); // produce sorted
            let header = IndexHeader::new(record_header_size, headers.len(), meta.len());
            let hs: usize = header.serialized_size()?.try_into().expect("u64 to usize");
            trace!("index header size: {}b", hs);
            let fsize = header.meta_size;
            let mut buf = Vec::with_capacity(hs + fsize + headers.len() * record_header_size);
            serialize_into(&mut buf, &header)?;
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
            let hash = get_hash(&buf);
            let header =
                IndexHeader::with_hash(record_header_size, headers.len(), meta.len(), hash);
            serialize_into(buf.as_mut_slice(), &header)?;
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
}
