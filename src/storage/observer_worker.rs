use super::prelude::*;
use tokio::{sync::mpsc::Receiver, sync::Semaphore, time::timeout};

pub(crate) struct ObserverWorker {
    inner: Inner,
    receiver: Receiver<Msg>,
    dump_sem: Arc<Semaphore>,
    update_interval: Duration,
}

impl ObserverWorker {
    pub(crate) fn new(receiver: Receiver<Msg>, inner: Inner, dump_sem: Arc<Semaphore>) -> Self {
        let update_interval = Duration::from_millis(inner.config.update_interval_ms());
        Self {
            inner,
            receiver,
            dump_sem,
            update_interval,
        }
    }

    pub(crate) async fn run(mut self) {
        debug!("update interval: {:?}", self.update_interval);
        loop {
            if let Err(e) = self.tick().await {
                warn!("active blob will no longer be updated, shutdown the system");
                warn!("{}", e);
                break;
            }
        }
        info!("observer stopped");
    }

    async fn tick(&mut self) -> Result<()> {
        match timeout(self.update_interval, self.receiver.recv()).await {
            Ok(Some(mut msg)) => match msg.optype() {
                OperationType::ForceUpdateActiveBlob => {
                    update_active_blob(self.inner.clone()).await?;
                    msg.commit();
                    self.inner
                        .try_dump_old_blob_indexes(self.dump_sem.clone())
                        .await
                }
                OperationType::CloseActiveBlob => {
                    self.inner.close_active_blob().await.map(|_| ())?
                }
                OperationType::CreateActiveBlob => {
                    self.inner.create_active_blob().await.map(|_| ())?
                }
                OperationType::RestoreActiveBlob => {
                    self.inner.restore_active_blob().await.map(|_| ())?
                }
            },
            Ok(None) => {
                return Err(anyhow!(
                    "all observer connected to this worker are dropped, so worker is done"
                        .to_string()
                ))
            }
            Err(_) => {}
        }
        trace!("check active blob");
        self.try_update().await
    }

    async fn try_update(&self) -> Result<()> {
        trace!("try update active blob");
        let inner_cloned = self.inner.clone();
        if let Some(mut inner) = active_blob_check(inner_cloned).await? {
            update_active_blob(inner.clone()).await?;
            inner.try_dump_old_blob_indexes(self.dump_sem.clone()).await;
        }
        Ok(())
    }
}

async fn active_blob_check(inner: Inner) -> Result<Option<Inner>> {
    let (active_size, active_count) = {
        trace!("await for lock");
        let safe_locked = inner.safe.read().await;
        trace!("lock acquired");
        if let Some(active_blob) = safe_locked.active_blob.as_ref() {
            (active_blob.file_size(), active_blob.records_count() as u64)
        } else {
            // if active blob doesn't exists, it doesn't need to be updated
            return Ok(None);
        }
    };
    trace!("lock released");
    let config_max_size = inner
        .config
        .max_blob_size()
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    let config_max_count = inner
        .config
        .max_data_in_blob()
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    if active_size as u64 > config_max_size || active_count >= config_max_count {
        Ok(Some(inner))
    } else {
        Ok(None)
    }
}

async fn update_active_blob(inner: Inner) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    trace!("obtaining new active blob");
    let new_active = Blob::open_new(next_name, inner.ioring, inner.config.filter())
        .await?
        .boxed();
    inner
        .safe
        .write()
        .await
        .replace_active_blob(new_active)
        .await?;
    Ok(())
}
