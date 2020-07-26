use crate::prelude::*;

/// bloom filter for faster check record contains in blob
pub mod bloom;
mod simple;

pub(crate) use super::prelude::*;
pub(crate) use bloom::{Bloom, Config};
pub(crate) use simple::{Simple, State};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use ahash::AHasher;
    pub(crate) use bitvec::prelude::*;
    pub(crate) use std::hash::Hasher;
}

pub(crate) trait Index: Send + Sync {
    fn get(&self, key: &[u8]) -> Get;
    fn push(&mut self, h: RecordHeader) -> Push;
    fn contains_key(&self, key: &[u8]) -> PinBox<dyn Future<Output = Result<bool>> + Send>;
    fn count(&self) -> Count;
    fn dump(&mut self) -> Dump;
    fn load(&mut self) -> Load;
}

pub(crate) struct Get {
    pub(crate) inner: PinBox<dyn Future<Output = Result<RecordHeader>> + Send>,
}

pub(crate) struct Push(pub(crate) PinBox<dyn Future<Output = Result<()>> + Send>);

pub(crate) struct Count(pub(crate) PinBox<dyn Future<Output = Result<usize>> + Send>);

pub(crate) struct Dump(pub(crate) PinBox<dyn Future<Output = Result<usize>> + Send>);

pub(crate) struct Load<'a>(pub(crate) PinBox<dyn Future<Output = Result<()>> + Send + 'a>);

impl Future for Count {
    type Output = Result<usize>;

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
    type Output = Result<usize>;

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
