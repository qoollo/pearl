use crate::prelude::*;

use super::{
    core::{Inner, Result},
    error::{Error, ErrorKind},
};

pub(crate) struct Observer {
    inner: Inner,
    update_interval: Duration,
}

impl Observer {
    pub(crate) fn new(update_interval: Duration, inner: Inner) -> Self {
        Self {
            update_interval,
            inner,
        }
    }

    pub(crate) async fn run(mut self) {
        debug!("update interval: {:?}", self.update_interval);
        let mut interval = Interval::new(Instant::now(), self.update_interval);
        while !self.inner.need_exit.load(Ordering::Relaxed) && interval.next().await.is_some() {
            trace!("check active blob");
            if let Err(e) = self.try_update().await {
                error!("{}", e);
                warn!("active blob will no longer be updated, shutdown the system");
                break;
            }
        }
        info!("observer stopped");
    }

    async fn try_update(&mut self) -> Result<()> {
        let inner_cloned = self.inner.clone();
        if let Some(inner) = active_blob_check(inner_cloned).await? {
            update_active_blob(inner).await?;
        }
        Ok(())
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
