use super::prelude::*;

/// Stream of entries
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
    CollectFromActiveBlob(PinBox<(dyn Future<Output = Option<Result<Vec<Entry>>>> + 'a)>),
    ClosedBlobs(PinBox<dyn Future<Output = MutexGuard<'a, Safe>> + 'a + Send>),
    CollectFromClosedBlobs(PinBox<(dyn Future<Output = Result<Vec<Entry>>> + 'a)>),
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
    type Item = Result<Entry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(entry) = self.ready_entries.pop() {
            cx.waker().wake_by_ref();
            Poll::Ready(Some(Ok(entry)))
        } else {
            debug!("match stream state");
            self.match_state(cx)
        }
    }
}

impl<'a, K> ReadAll<'a, K> {
    pub(crate) const fn new(inner: &'a Storage<K>, key: &'a [u8]) -> Self {
        Self {
            key,
            inner,
            state: RefCell::new(State::Initial),
            ready_entries: Vec::new(),
        }
    }

    fn match_state(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        let key = self.key;
        let state = self.state.get_mut();
        match state {
            State::Initial => {
                self.state.replace(State::LockStorage);
            }
            State::LockStorage => {
                let storage_fut = self.inner.inner.safe.lock();
                self.state.replace(State::ActiveBlob(storage_fut.boxed()));
            }
            State::ActiveBlob(fut) => {
                info!("search for key in active blob");
                let safe = ready!(fut.as_mut().poll(cx));
                let key = key.to_vec();
                let new_fut = async move {
                    let active_blob = safe.active_blob.as_ref()?;
                    Some(active_blob.read_all(&key).try_collect().await)
                };
                self.state
                    .replace(State::CollectFromActiveBlob(new_fut.boxed()));
            }
            State::CollectFromActiveBlob(fut) => {
                if let Some(Ok(entries)) = ready!(fut.as_mut().poll(cx)) {
                    self.ready_entries.extend(entries);
                };
                let storage_fut = self.inner.inner.safe.lock();
                self.state.replace(State::ClosedBlobs(storage_fut.boxed()));
            }
            State::ClosedBlobs(fut) => {
                info!("search for key in closed blobs");
                let safe = ready!(fut.as_mut().poll(cx));
                let new_fut = async move {
                    let mut entries = Vec::new();
                    for blob in &safe.blobs {
                        let read_all = blob.read_all(key).try_collect::<Vec<_>>().await?;
                        debug!("read all from blob finished");
                        entries.extend(read_all);
                    }
                    debug!("read all from all blobs finished");
                    Ok(entries)
                }
                .boxed();
                self.state.replace(State::CollectFromClosedBlobs(new_fut));
            }
            State::CollectFromClosedBlobs(fut) => {
                debug!("enter collect from closed blobs state");
                let entries = ready!(fut.as_mut().poll(cx));
                match entries {
                    Ok(entries) => {
                        self.ready_entries.extend(entries);
                        self.state.replace(State::Finished);
                        debug!("collect from closed blobs finished");
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            }
            State::Finished => {
                debug!("state finished, return None");
                return Poll::Ready(None);
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
