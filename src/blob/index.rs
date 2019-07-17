use futures::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::core::Error;
use crate::record::Header as RecordHeader;

type Result<T> = std::result::Result<T, Error>;

pub(crate) trait Index {
    fn get(&self, key: &[u8]) -> Get;
    fn push(&mut self, h: RecordHeader) -> Push;
    fn contains_key(&self, key: &[u8]) -> ContainsKey;
    fn count(&self) -> Count;
    fn dump(&mut self) -> Dump;
    fn load(&mut self) -> Load;
}

type Inner<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

pub(crate) struct Get(pub(crate) Inner<RecordHeader>);

pub(crate) struct Push(pub(crate) Inner<()>);

pub(crate) struct ContainsKey(pub(crate) Inner<bool>);

pub(crate) struct Count(pub(crate) Inner<usize>);

pub(crate) struct Dump(pub(crate) Inner<()>);

pub(crate) struct Load<'a>(pub(crate) Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>);

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
        Future::poll(self.0.as_mut(), cx)
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
