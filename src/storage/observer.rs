use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use futures::executor::block_on;
use futures::future::{FutureExt, FutureObj};
use futures::stream::{Stream, StreamExt};
use futures::task::SpawnExt;

use crate::blob::Blob;

use super::{
    core::{Inner, Result},
    error::{Error, ErrorKind},
};

pub(crate) struct Observer<S>
where
    S: SpawnExt + Send + 'static,
{
    spawner: S,
    inner: Inner,
    next_update: Instant,
    update_interval: Duration,
}

impl<S> Observer<S>
where
    S: SpawnExt + Send + 'static + Unpin + Sync,
{
    pub(crate) fn new(
        update_interval: Duration,
        inner: Inner,
        next_update: Instant,
        spawner: S,
    ) -> Self {
        Self {
            update_interval,
            inner,
            next_update,
            spawner,
        }
    }

    pub(crate) fn run(mut self) {
        while let Some(f) = block_on(self.next()) {
            if let Err(e) = self.spawner.spawn(f.map(|r| {
                r.expect("active blob update future paniced");
            })) {
                error!("{:?}", e);
                break;
            }
            thread::sleep(self.update_interval);
        }
        trace!("observer stopped");
    }
}

impl<S> Stream for Observer<S>
where
    S: SpawnExt + Send + 'static + Unpin,
{
    type Item = FutureObj<'static, Result<()>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let now = Instant::now();
        if self.next_update < now {
            trace!("Observer update");
            self.as_mut().next_update = now + self.update_interval;
            let inner_cloned = self.inner.clone();
            let res = async {
                if let Some(inner) = active_blob_check(inner_cloned).await? {
                    update_active_blob(inner).await?;
                }
                Ok(())
            };
            let res = Box::new(res);
            let obj = FutureObj::new(res);
            Poll::Ready(Some(obj))
        } else if self.inner.need_exit.load(Ordering::Relaxed) {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            trace!("observer state: false");
            Poll::Ready(None)
        }
    }
}

async fn active_blob_check(inner: Inner) -> Result<Option<Inner>> {
    let (active_size, active_count) = {
        let safe_locked = inner.safe.lock().await;
        let active_blob = safe_locked
            .active_blob
            .as_ref()
            .ok_or(ErrorKind::ActiveBlobNotSet)?;
        (
            active_blob.file_size().map_err(Error::new)?,
            active_blob.records_count().await.map_err(Error::new)? as u64,
        )
    };
    let config_max_size = inner
        .config
        .max_blob_size
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    let config_max_count = inner
        .config
        .max_data_in_blob
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    if active_size > config_max_size || active_count >= config_max_count {
        Ok(Some(inner))
    } else {
        Ok(None)
    }
}

async fn update_active_blob(inner: Inner) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    let new_active = Blob::open_new(next_name).await.map_err(Error::new)?.boxed();

    let mut safe_locked = inner.safe.lock().await;
    let mut old_active = safe_locked
        .active_blob
        .replace(new_active)
        .ok_or(ErrorKind::ActiveBlobNotSet)?;
    old_active.dump().await.map_err(Error::new)?;
    safe_locked.blobs.push(*old_active);
    Ok(())
}
