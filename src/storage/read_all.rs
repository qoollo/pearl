use super::prelude::*;

/// @TODO
#[derive(Debug)]
pub struct ReadAll<'a, K> {
    key: &'a [u8],
    inner: &'a Storage<K>,
    state: RefCell<State<'a>>,
    ready_entries: Vec<Entry>,
}

enum State<'a> {
    Initial,
    LockStorage,
    ActiveBlob(PinBox<dyn Future<Output = MutexGuard<'a, Safe>> + 'a + Send>),
    CollectFromActiveBlob(PinBox<(dyn Future<Output = Vec<Entry>> + 'a)>),
    ClosedBlobs(PinBox<dyn Future<Output = MutexGuard<'a, Safe>> + 'a + Send>),
    CollectFromClosedBlobs(PinBox<(dyn Future<Output = Vec<Entry>> + 'a)>),
    Finished,
}

impl<'a> Debug for State<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            State::Initial => f.write_str("Initial"),
            State::LockStorage => f.write_str("LockStorage"),
            State::ActiveBlob(_) => f.write_str("ActiveBlob"),
            State::CollectFromActiveBlob(_) => f.write_str("CollectFromActiveBlob"),
            State::ClosedBlobs(_) => f.write_str("ClosedBlobs"),
            State::CollectFromClosedBlobs(_) => f.write_str("CollectFromClosedBlobs"),
            State::Finished => f.write_str("Finished"),
        }
    }
}

impl<'a, K> Stream for ReadAll<'a, K> {
    type Item = Entry;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(entry) = self.ready_entries.pop() {
            debug!("ready entries count: {}", self.ready_entries.len() + 1);
            cx.waker().wake_by_ref();
            Poll::Ready(Some(entry))
        } else {
            let key = self.key;
            let state = self.state.get_mut();
            match state {
                State::Initial => {
                    debug!("Initial");
                    self.state.replace(State::LockStorage);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::LockStorage => {
                    debug!("LockStorage");
                    let storage_fut = self.inner.inner.safe.lock();
                    self.state.replace(State::ActiveBlob(storage_fut.boxed()));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::ActiveBlob(fut) => {
                    info!("search for key in active blob");
                    let poll_res = fut.as_mut().poll(cx);
                    let safe = ready!(poll_res);
                    debug!("future ready");
                    let key = key.to_vec();
                    let new_fut = async move {
                        debug!("enter async closure");
                        safe.active_blob
                            .as_ref()
                            .unwrap()
                            .read_all(&key)
                            .await
                            .collect::<Vec<_>>()
                            .await
                    };
                    self.state
                        .replace(State::CollectFromActiveBlob(new_fut.boxed()));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::CollectFromActiveBlob(fut) => {
                    debug!("CollectFromActiveBlob");
                    let pin_ref = fut.as_mut();
                    let poll_res = Future::poll(pin_ref, cx);
                    debug!("future polled: {:?}", poll_res);
                    let entries = ready!(poll_res);
                    self.ready_entries.extend(entries);
                    let storage_fut = self.inner.inner.safe.lock();
                    self.state.replace(State::ClosedBlobs(storage_fut.boxed()));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::ClosedBlobs(fut) => {
                    info!("search for key in closed blobs");
                    let pin_ref = fut.as_mut();
                    let poll_res = Future::poll(pin_ref, cx);
                    let safe = ready!(poll_res);
                    debug!("future ready");
                    let new_fut = async move {
                        debug!("enter async closure");
                        let mut entries = Vec::new();
                        for blob in &safe.blobs {
                            entries.extend(blob.read_all(&key).await.collect::<Vec<_>>().await);
                        }
                        entries
                    }
                        .boxed();
                    self.state.replace(State::CollectFromClosedBlobs(new_fut));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::CollectFromClosedBlobs(fut) => {
                    debug!("CollectFromClosedBlobs");
                    let pin_ref = fut.as_mut();
                    let poll_res = Future::poll(pin_ref, cx);
                    let entries = ready!(poll_res);
                    debug!("future ready");
                    self.ready_entries.extend(entries);
                    self.state.replace(State::Finished);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::Finished => Poll::Ready(None),
            }
        }
    }
}

impl<'a, K> ReadAll<'a, K> {
    pub(crate) fn new(inner: &'a Storage<K>, key: &'a [u8]) -> Self {
        Self {
            key,
            inner,
            state: RefCell::new(State::Initial),
            ready_entries: Vec::new(),
        }
    }
}
