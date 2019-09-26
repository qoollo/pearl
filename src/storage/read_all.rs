use crate::prelude::*;

use super::core::Safe;
use futures::lock::{MutexGuard, MutexLockFuture};
use std::cell::Cell;

/// @TODO
pub struct ReadAll<'a, K> {
    key: &'a [u8],
    inner: &'a Storage<K>,
    state: Cell<State<'a>>,
    safe: Option<MutexGuard<'a, Safe>>,
    ready_entries: Vec<Entry>,
}

enum State<'a> {
    Initial,
    LockStorage,
    ActiveBlob(MutexLockFuture<'a, Safe>),
    ClosedBlobs(MutexLockFuture<'a, Safe>),
}

impl<'a, K> Debug for ReadAll<'a, K> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        unimplemented!()
    }
}

impl<'a> Debug for State<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        unimplemented!()
    }
}

impl<'a, K> Stream for ReadAll<'a, K> {
    type Item = Entry;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(entry) = self.ready_entries.pop() {
            cx.waker().wake_by_ref();
            Poll::Ready(Some(entry))
        } else {
            let key = self.key;
            let state = self.state.get_mut();
            match state {
                State::Initial => {
                    info!("Initial");
                    self.state.set(State::LockStorage);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::LockStorage => {
                    info!("LockStorage");
                    let storage_fut = self.inner.inner.safe.lock();
                    self.state.set(State::ActiveBlob(storage_fut));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::ActiveBlob(fut) => {
                    info!("ActiveBlob");
                    let fut = fut.then(async move |safe| {
                        safe.active_blob
                            .as_ref()
                            .unwrap()
                            .read_all(key)
                            .await
                            .collect::<Vec<_>>()
                            .await
                    });
                    pin_mut!(fut);
                    let entries = ready!(Future::poll(fut, cx));
                    self.ready_entries.extend(entries);
                    let storage_fut = self.inner.inner.safe.lock();
                    self.state.set(State::ClosedBlobs(storage_fut));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                State::ClosedBlobs(fut) => {
                    info!("ClosedBlobs");
                    unimplemented!()
                }
                _ => unimplemented!(),
            }
        }
    }
}

impl<'a, K> ReadAll<'a, K> {
    pub(crate) fn new(inner: &'a Storage<K>, key: &'a [u8]) -> Self {
        Self {
            key,
            inner,
            state: Cell::new(State::Initial),
            safe: None,
            ready_entries: Vec::new(),
        }
    }
}
