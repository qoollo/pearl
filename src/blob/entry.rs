use std::slice::Iter;

use crate::prelude::*;

use super::{core::Result, file::File, simple_index::State};

/// `Entry` similar to `Record`, but does not contain all of the data in memory.
#[derive(Debug)]
pub struct Entry {
    meta: Option<Meta>,
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
    pub(crate) fn new() -> Self {
        Self {
            meta: None,
            data_offset: None,
            data_size: None,
        }
    }

    pub(crate) fn set_meta(&mut self, meta: Meta) {
        self.meta = Some(meta);
    }

    pub(crate) fn meta(&self) -> Option<Meta> {
        info!("meta: {:?}", self.meta);
        self.meta.clone()
    }

    pub(crate) fn offset(&self) -> u64 {
        self.data_offset.expect("data offset was not set")
    }

    pub(crate) fn size(&self) -> usize {
        self.data_size.unwrap()
    }

    /// Converts `Entry` into `Record` by loading skipped parts from disk.
    pub(crate) async fn load_data(self) -> Result<Vec<u8>> {
        unimplemented!()
    }
}

impl<'a> Stream for Entries<'a> {
    type Item = Entry;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        info!("match inner");
        match self.inner {
            State::InMemory(headers) => {
                info!("index in memory");
                let key = self.key;
                if let Some(it) = &mut self.in_memory_iter {
                    info!("headers iterator set");
                    info!("iter: {:?}", it);
                    for h in it {
                        info!("header: {:?}", h);
                        if h.key() == key {
                            info!("key matched");
                            let meta = Meta::load(self.blob_file, h.meta_location()).boxed();
                            pin_mut!(meta);
                            let resolved_meta = ready!(Future::poll(meta, cx));
                            let mut entry = Entry::new();
                            entry.set_meta(resolved_meta);
                            entry.data_offset = Some(h.blob_offset());
                            entry.data_size = Some(h.full_size().unwrap().try_into().unwrap());
                            return Poll::Ready(Some(entry));
                        }
                    }
                    Poll::Ready(None)
                } else {
                    info!("headers iterator not set");
                    let mut rec_iter = headers.iter();
                    info!("created new headers iterator");
                    let h = rec_iter.find(|h| h.key() == key).unwrap();
                    let meta = Meta::load(self.blob_file, h.meta_location()).boxed();
                    pin_mut!(meta);
                    let resolved_meta = ready!(Future::poll(meta, cx));
                    let mut entry = Entry::new();
                    entry.set_meta(resolved_meta);
                    entry.data_offset = Some(h.blob_offset());
                    entry.data_size = Some(h.full_size().unwrap().try_into().unwrap());
                    self.in_memory_iter = Some(rec_iter);
                    return Poll::Ready(Some(entry));
                }
            }
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
}
