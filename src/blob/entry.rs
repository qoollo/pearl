use super::prelude::*;
use crate::prelude::*;

use std::slice::Iter;

/// `Entry` similar to `Record`, but does not contain all of the data in memory.
#[derive(Debug)]
pub struct Entry {
    meta: Meta,
    data_offset: Option<u64>,
    data_size: Option<usize>,
}

#[derive(Debug)]
pub struct Entries<'a> {
    inner: &'a State,
    key: &'a [u8],
    in_memory_iter: Option<Iter<'a, RecordHeader>>,
    blob_file: &'a File,
}

impl Entry {
    pub(crate) fn new(meta: Meta) -> Self {
        Self {
            meta,
            data_offset: None,
            data_size: None,
        }
    }

    pub(crate) fn meta(&self) -> Meta {
        self.meta.clone()
    }

    pub(crate) fn offset(&self) -> u64 {
        self.data_offset.expect("get data offset")
    }

    pub(crate) fn size(&self) -> usize {
        self.data_size.expect("get data size")
    }
}

impl<'a> Stream for Entries<'a> {
    type Item = Entry;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.inner {
            State::InMemory(headers) => Self::get_next_poll(self, cx, headers),
            State::OnDisk(file) => unimplemented!(),
        }
    }
}

impl<'a> Entries<'a> {
    pub(crate) fn new(inner: &'a State, key: &'a [u8], blob_file: &'a File) -> Self {
        Self {
            inner,
            key,
            in_memory_iter: None,
            blob_file,
        }
    }

    fn get_next_poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        headers: &'a [RecordHeader],
    ) -> Poll<Option<Entry>> {
        let key = self.key;
        let file = self.blob_file;
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
        file: &File,
        cx: &mut Context,
    ) -> Poll<Option<Entry>> {
        for h in it {
            if h.key() == key {
                let entry = Self::create_entry(file, h);
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
        let mut entry = Entry::new(meta);
        entry.data_offset = Some(header.blob_offset());
        entry.data_size = Some(header.full_size()?.try_into()?);
        Ok(entry)
    }
}
