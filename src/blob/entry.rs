use super::prelude::*;
use crate::prelude::*;

use std::slice::Iter;

/// [`Entry`] is a [`Future`], which contains header and metadata of the record,
/// but does not contain all of the data in memory. To resolve [`Entry`] to data,
/// you should poll it.
/// [`Entry`]: struct.Entry.html
pub struct Entry {
    meta: Meta,
    blob_offset: u64,
    full_size: usize,
    data_offset: u64,
    data_size: usize,
    inner: PinBox<dyn Future<Output = IOResult<Vec<u8>>> + Send>,
}

/// `Entries`
pub struct Entries<'a> {
    inner: &'a State,
    key: &'a [u8],
    in_memory_iter: Option<Iter<'a, RecordHeader>>,
    load_fut: Option<PinBox<dyn Future<Output = Result<Vec<RecordHeader>>> + 'a + Send>>,
    blob_file: File,
    loaded_entries: Option<Vec<RecordHeader>>,
}

impl Entry {
    pub(crate) fn new(meta: Meta, header: &RecordHeader, blob_file: File) -> Self {
        let data_size = header.data_size().try_into().unwrap();
        let data_offset = header.data_offset();
        let blob_offset = header.blob_offset();
        let full_size = header.full_size().try_into().unwrap();
        let inner = async move { blob_file.read_at(data_size, data_offset).await }.boxed();
        Self {
            meta,
            data_offset,
            data_size,
            blob_offset,
            full_size,
            inner,
        }
    }

    pub(crate) fn meta(&self) -> Meta {
        self.meta.clone()
    }

    pub(crate) fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    pub(crate) fn full_size(&self) -> usize {
        self.full_size
    }
}

impl Future for Entry {
    type Output = IOResult<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl Debug for Entry {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Entry")
            .field("meta", &self.meta)
            .field("data_offset", &self.data_offset)
            .field("data_size", &self.data_size)
            .field("inner", &format_args!("_"))
            .finish()
    }
}
impl<'a> Stream for Entries<'a> {
    type Item = Entry;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(entries) = &mut self.loaded_entries {
            if let Some(rec) = entries.pop() {
                let entry = Self::create_entry(&self.blob_file, &rec);
                pin_mut!(entry);
                let entry = ready!(entry.poll(cx)).unwrap();
                Poll::Ready(Some(entry))
            } else {
                Poll::Ready(None)
            }
        } else {
            match self.inner {
                State::InMemory(headers) => Self::get_next_poll(self, cx, headers),
                State::OnDisk(file) => {
                    debug!("state on disk");
                    if let Some(fut) = &mut self.load_fut {
                        debug!("get stored future");
                        let entries = ready!(fut.as_mut().poll(cx)).unwrap();
                        debug!("future polled");
                        self.loaded_entries = Some(
                            entries
                                .into_iter()
                                .filter(|h| h.key() == self.key)
                                .collect(),
                        );
                        debug!(
                            "loaded entries total {:?}",
                            self.loaded_entries.as_ref().map(|entries| entries.len())
                        );
                        self.load_fut = None;
                        Poll::Pending
                    } else {
                        debug!("create new future");
                        let fut = SimpleIndex::load(file);
                        debug!("created");
                        self.load_fut = Some(fut.boxed());
                        debug!("future stored");
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
        }
    }
}

impl<'a> Entries<'a> {
    pub(crate) fn new(inner: &'a State, key: &'a [u8], blob_file: File) -> Self {
        Self {
            inner,
            key,
            in_memory_iter: None,
            load_fut: None,
            blob_file,
            loaded_entries: None,
        }
    }

    fn get_next_poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        headers: &'a [RecordHeader],
    ) -> Poll<Option<Entry>> {
        let key = self.key;
        let file = self.blob_file.clone();
        if let Some(it) = &mut self.in_memory_iter {
            trace!("find in iterator");
            Self::try_find_record_header(it, key, file, cx)
        } else {
            trace!("iterator not set");
            let rec_iter = headers.iter();
            self.in_memory_iter = Some(rec_iter);
            cx.waker().wake_by_ref();
            trace!("wake scheduled");
            Poll::Pending
        }
    }

    fn try_find_record_header(
        it: &mut Iter<RecordHeader>,
        key: &[u8],
        file: File,
        cx: &mut Context,
    ) -> Poll<Option<Entry>> {
        for h in it {
            if h.key() == key {
                let entry = Self::create_entry(&file, h);
                pin_mut!(entry);
                let entry = ready!(Future::poll(entry, cx));
                return Poll::Ready(entry.ok());
            }
        }
        Poll::Ready(None)
    }

    async fn create_entry(file: &File, header: &RecordHeader) -> Result<Entry> {
        let meta = Meta::load(file, header.meta_location().map_err(Error::new)?)
            .await
            .map_err(Error::new)?;
        let entry = Entry::new(meta, &header, file.clone());
        debug!("entry: {:?}", entry);
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
            .field("loaded_entries", &self.loaded_entries)
            .finish()
    }
}
