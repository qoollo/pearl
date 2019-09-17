use crate::prelude::*;

use super::core::Error;
use crate::record::Header as RecordHeader;

type Result<T> = std::result::Result<T, Error>;

pub(crate) trait Index {
    fn get(&self, key: &[u8]) -> Get;
    fn next_item(&self, key: &[u8], file_offset: Option<usize>, vec_index: Option<usize>) -> Next;
    fn push(&mut self, h: RecordHeader) -> Push;
    fn contains_key(&self, key: &[u8]) -> ContainsKey;
    fn count(&self) -> Count;
    fn dump(&mut self) -> Dump;
    fn load(&mut self) -> Load;
}

pub(crate) trait IndexExt: Index {
    fn get_all<'a>(&'a self, key: &'a [u8]) -> GetAll<'a>
    where
        Self: Sized,
    {
        GetAll {
            inner: self.get(key).inner,
            key,
            index: self,
            file_offset: None,
            vec_index: None,
        }
    }
}

impl<T> IndexExt for T where T: Index {}

type Inner<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

pub(crate) struct Get {
    pub(crate) inner: Inner<RecordHeader>,
}

pub(crate) struct Next {
    pub(crate) inner: Inner<RecordHeader>,
    pub(crate) file_offset: Option<Arc<AtomicIsize>>,
    pub(crate) vec_index: Option<usize>,
}

pub(crate) struct GetAll<'a> {
    inner: Inner<RecordHeader>,
    pub key: &'a [u8],
    index: &'a dyn Index,
    file_offset: Option<usize>,
    vec_index: Option<usize>,
}

pub(crate) struct Push(pub(crate) Inner<()>);

pub(crate) struct ContainsKey(pub(crate) Inner<bool>);

pub(crate) struct Count(pub(crate) Inner<usize>);

pub(crate) struct Dump(pub(crate) Inner<()>);

pub(crate) struct Load<'a>(pub(crate) Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>);

impl<'a> Stream for GetAll<'a> {
    type Item = RecordHeader;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        info!("poll_next");
        let record = ready!(Future::poll(self.inner.as_mut(), cx));
        info!("record ready");
        let next = self
            .index
            .next_item(&self.key, self.file_offset, self.vec_index);
        self.file_offset = next
            .file_offset
            .map(|fo| fo.load(Ordering::Relaxed).try_into().unwrap());
        self.vec_index = next.vec_index;
        info!("create next future");
        self.inner = next.inner;
        info!("set next future");
        Poll::Ready(record.ok())
    }
}

impl Future for Count {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.0.as_mut(), cx)
    }
}

impl Future for ContainsKey {
    type Output = Result<bool>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.0.as_mut(), cx)
    }
}

impl Future for Get {
    type Output = Result<RecordHeader>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.inner.as_mut(), cx)
    }
}

impl Future for Dump {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.0.as_mut(), cx)
    }
}

impl Future for Push {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.0.as_mut(), cx)
    }
}

impl<'a> Future for Load<'a> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.0.as_mut(), cx)
    }
}
