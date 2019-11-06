use crate::prelude::*;

pub(crate) trait Index: Send + Sync {
    fn get(&self, key: &[u8]) -> Get;
    fn push(&mut self, h: RecordHeader) -> Push;
    fn contains_key(&self, key: &[u8]) -> ContainsKey;
    fn count(&self) -> Count;
    fn dump(&mut self) -> Dump;
    fn load(&mut self) -> Load;
}

pub(crate) struct Get {
    pub(crate) inner: PinBox<dyn Future<Output = Result<RecordHeader>> + Send>,
}

pub(crate) struct Push(pub(crate) PinBox<dyn Future<Output = Result<()>> + Send>);

pub(crate) struct ContainsKey(pub(crate) PinBox<dyn Future<Output = Result<bool>> + Send>);

pub(crate) struct Count(pub(crate) PinBox<dyn Future<Output = Result<usize>> + Send>);

pub(crate) struct Dump(pub(crate) PinBox<dyn Future<Output = Result<()>> + Send>);

pub(crate) struct Load<'a>(pub(crate) PinBox<dyn Future<Output = Result<()>> + Send + 'a>);

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
