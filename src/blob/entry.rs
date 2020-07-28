use super::prelude::*;

use std::slice::Iter;

/// [`Entry`] is a [`Future`], which contains header and metadata of the record,
/// but does not contain all of the data in memory.
///
/// If you searching for the records with particular meta, you don't need to load
/// full record. When you've found entry with required meta, call [`load`] to get
/// body.
///
/// [`Entry`]: struct.Entry.html
/// [`load`]: struct.Entry.html#method.load
#[derive(Debug)]
pub struct Entry {
    meta: Meta,
    blob_offset: u64,
    full_size: usize,
    data_offset: u64,
    data_size: usize,
    blob_file: File,
}

/// [`Entries`] is an iterator over the entries with the same key.
///
/// It is a [`Stream`],
/// because it requires to load record headers from the disk index. But only if
/// the blob is closed and index dumped
///
/// [`Entries`]: struct.Entries.html
/// [`Stream`]: `futures::stream::Stream`
pub struct Entries<'a> {
    inner: &'a State,
    key: &'a [u8],
    token: Option<()>,
    in_memory_iter: Option<Iter<'a, RecordHeader>>,
    loading_entries: FuturesUnordered<BoxFuture<'a, Result<Entry>>>,
    load_fut: Option<PinBox<dyn Future<Output = AnyResult<Vec<RecordHeader>>> + 'a + Send>>,
    blob_file: File,
    loaded_headers: Option<VecDeque<RecordHeader>>,
}

impl Entry {
    /// Returns record data
    /// # Errors
    /// Returns the error type for I/O operations, see [`std::io::Error`]
    pub async fn load(&self) -> AnyResult<Vec<u8>> {
        let mut buf = vec![0; self.data_size as usize];
        self.blob_file
            .read_at(&mut buf, self.data_offset)
            .await
            .with_context(|| "blob load failed")?; // TODO: verify read size
        Ok(buf)
    }

    pub(crate) fn new(meta: Meta, header: &RecordHeader, blob_file: File) -> Self {
        let data_size = header.data_size().try_into().expect("u64 to usize");
        let data_offset = header.data_offset();
        let blob_offset = header.blob_offset();
        let full_size = header.full_size().try_into().expect("u64 to usize");
        Self {
            meta,
            data_offset,
            data_size,
            blob_offset,
            full_size,
            blob_file,
        }
    }

    pub(crate) fn meta(&self) -> Meta {
        self.meta.clone()
    }

    pub(crate) const fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    pub(crate) const fn full_size(&self) -> usize {
        self.full_size
    }
}

impl<'a> Stream for Entries<'a> {
    type Item = Result<Entry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        debug!("entries poll next");
        if let Some(headers) = &mut self.loaded_headers {
            debug!("headers loaded");
            let next = if let Some(header) = headers.pop_front() {
                debug!("{} headers loaded, create entries from them", headers.len());
                let entry = Self::create_entry(self.blob_file.clone(), &header);
                pin_mut!(entry);
                let entry = ready!(entry.poll(cx));
                Some(entry)
            } else {
                debug!("no headers left, finish entries future");
                None
            };
            Poll::Ready(next)
        } else {
            cx.waker().wake_by_ref();
            self.get_headers_from_index(cx)
        }
    }
}

impl<'a> Entries<'a> {
    pub(crate) fn new(inner: &'a State, key: &'a [u8], blob_file: File) -> Self {
        Self {
            inner,
            key,
            token: Some(()),
            in_memory_iter: None,
            loading_entries: FuturesUnordered::new(),
            load_fut: None,
            blob_file,
            loaded_headers: None,
        }
    }

    fn get_headers_from_index(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        debug!("get headers from index");
        match self.inner {
            State::InMemory(headers) => self.get_next_poll(cx, headers),
            State::OnDisk(file) => self.load_headers_from_file(cx, file),
        }
    }

    fn load_headers_from_file(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        file: &'a File,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        debug!("load headers from file");
        if let Some(fut) = &mut self.load_fut {
            let headers = ready!(fut.as_mut().poll(cx));
            if let Ok(headers) = headers {
                debug!("{} headers loaded", headers.len());
                self.reset_load_future(headers);
            }
        } else {
            let fut = SimpleIndex::load_records(file);
            self.load_fut = Some(fut.boxed());
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn reset_load_future(mut self: Pin<&mut Self>, headers: Vec<RecordHeader>) {
        self.loaded_headers = Some(
            headers
                .into_iter()
                .filter(|h| {
                    debug!("check {:?}", h.key());
                    h.key() == self.key
                })
                .collect(),
        );
        debug!("loaded headers set");
        self.load_fut = None;
    }

    fn get_next_poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        headers: &'a [RecordHeader],
    ) -> Poll<Option<Result<Entry>>> {
        debug!("get next poll");
        let key = self.key;
        if self.token.take().is_some() {
            debug!("first entries poll, create entries futures");
            for h in headers {
                debug!("check key: {:?} == {:?}", h.key(), key);
                if h.key() == key {
                    debug!("key matched");
                    let entry = Self::create_entry(self.blob_file.clone(), h);
                    self.loading_entries.push(entry.boxed());
                }
            }
            cx.waker().wake_by_ref();
        }

        let fut = Pin::new(&mut self.loading_entries);
        Stream::poll_next(fut, cx)
    }

    async fn create_entry(file: File, header: &RecordHeader) -> Result<Entry> {
        debug!("create entry");
        let meta = Meta::load(&file, header.meta_location())
            .await
            .map_err(Error::new)?;
        debug!("meta loaded");
        let entry = Entry::new(meta, header, file);
        debug!("entry created");
        Ok(entry)
    }
}

impl Debug for Entries<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Entries")
            .field("inner", &self.inner)
            .field("key", &self.key)
            .field("in_memory_iter", &self.in_memory_iter)
            .field(
                "load_fut",
                &self.load_fut.as_ref().map(|_| format_args!("_")),
            )
            .field("blob_file", &self.blob_file)
            .field("loaded_headers", &self.loaded_headers)
            .finish()
    }
}
