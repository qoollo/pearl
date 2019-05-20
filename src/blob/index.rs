use futures::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::core::{Error, FileName, SimpleIndex};
use crate::record::Header as RecordHeader;

type Result<T> = std::result::Result<T, Error>;

pub(crate) trait Index {
    fn get(&self, key: &[u8]) -> Get;
    fn push(&mut self, h: RecordHeader) -> Push;
    fn contains_key(&self, key: &[u8]) -> ContainsKey;
    fn count(&self) -> Count;
    fn flush(&mut self) -> Flush;
}

pub(crate) struct Get(pub(crate) Pin<Box<dyn Future<Output = Result<()>> + Send>>);

pub(crate) struct Push(pub(crate) Pin<Box<dyn Future<Output = Result<()>> + Send>>);

pub(crate) struct ContainsKey(pub(crate) Pin<Box<dyn Future<Output = Result<()>> + Send>>);

pub(crate) struct Count(pub(crate) Pin<Box<dyn Future<Output = Result<usize>> + Send>>);

pub(crate) struct Flush(pub(crate) Pin<Box<dyn Future<Output = Result<()>> + Send>>);

impl Future for Count {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self.0.as_mut(), cx)
    }
}
