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
    header: RecordHeader,
    meta: Option<Meta>,
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
    loading_entries: Vec<Entry>,
    load_fut: Option<PinBox<dyn Future<Output = Result<InMemoryIndex>> + 'a + Send>>,
    blob_file: File,
    loaded_headers: Option<Vec<RecordHeader>>,
    entries_fut: Option<BoxFuture<'a, Result<Entry>>>,
}

impl Entry {
    /// Consumes Entry and returns whole loaded record.
    /// # Errors
    /// Returns the error type for I/O operations, see [`std::io::Error`]
    pub async fn load(self) -> Result<Record> {
        let meta_size = self.header.meta_size().try_into()?;
        let data_size: usize = self.header.data_size().try_into()?;
        let mut buf = vec![0; data_size + meta_size];
        self.blob_file
            .read_at(&mut buf, self.header.meta_offset())
            .await
            .with_context(|| "blob load failed")?; // TODO: verify read size
        let data_buf = buf.split_off(meta_size);
        let meta = Meta::from_raw(&buf)?;
        let record = Record::new(self.header.clone(), meta, data_buf);
        record.validate()
    }

    /// Returns only data.
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn load_data(&self) -> Result<Vec<u8>> {
        let data_offset = self.header.data_offset();
        let mut buf = vec![0; self.header.data_size().try_into()?];
        self.blob_file.read_at(&mut buf, data_offset).await?;
        Ok(buf)
    }

    /// Loads meta data from fisk, and returns reference to it.
    /// # Errors
    /// Fails after any disk IO errors.
    pub async fn load_meta(&mut self) -> Result<Option<&Meta>> {
        let meta_offset = self.header.meta_offset();
        let mut buf = vec![0; self.header.meta_size().try_into()?];
        self.blob_file.read_at(&mut buf, meta_offset).await?;
        self.meta = Some(Meta::from_raw(&buf)?);
        Ok(self.meta.as_ref())
    }

    pub(crate) fn new(header: RecordHeader, blob_file: File) -> Self {
        Self {
            meta: None,
            header,
            blob_file,
        }
    }
}

impl<'a> Stream for Entries<'a> {
    type Item = Result<Entry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        trace!("entries poll next");
        if let Some(fut) = self.entries_fut.as_mut() {
            trace!("poll entries future");
            match fut.as_mut().poll(cx) {
                Poll::Ready(res) => {
                    self.entries_fut = None;
                    Poll::Ready(Some(res))
                }
                Poll::Pending => Poll::Pending,
            }
        } else if let Some(headers) = &mut self.loaded_headers {
            trace!("headers loaded");
            if let Some(header) = headers.pop() {
                trace!("{} headers loaded, create entries from them", headers.len());
                let entry = Entry::new(header, self.blob_file.clone());
                Poll::Ready(Some(Ok(entry)))
            } else {
                trace!("no headers left, finish entries future");
                Poll::Ready(None)
            }
        } else {
            cx.waker().wake_by_ref();
            self.get_headers_from_index(cx)
        }
    }
}

impl<'a> Entries<'a> {
    fn get_headers_from_index(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        trace!("get headers from index");
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
        trace!("load headers from file");
        if let Some(fut) = &mut self.load_fut {
            let headers = ready!(fut.as_mut().poll(cx));
            if let Ok(headers) = headers {
                trace!("{} headers loaded", headers.len());
                self.reset_load_future(headers);
            }
        } else {
            let fut = SimpleIndex::load_records(file);
            self.load_fut = Some(fut.boxed());
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn reset_load_future(mut self: Pin<&mut Self>, mut headers: InMemoryIndex) {
        self.loaded_headers = headers.remove(self.key);
        self.load_fut = None;
    }

    fn get_next_poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        all_headers: &'a BTreeMap<Vec<u8>, Vec<RecordHeader>>,
    ) -> Poll<Option<Result<Entry>>> {
        trace!("get next poll");
        let key = self.key;
        if self.token.take().is_some() {
            trace!("first entries poll, create entries futures");
            if let Some(headers) = all_headers.get(key) {
                for h in headers {
                    if h.key() == key {
                        trace!("key matched");
                        let entry = Entry::new(h.clone(), self.blob_file.clone());
                        self.loading_entries.push(entry);
                    }
                }
            }
            cx.waker().wake_by_ref();
        }

        Poll::Ready(Ok(self.loading_entries.pop()).transpose())
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
