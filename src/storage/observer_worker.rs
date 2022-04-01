use super::prelude::*;
use tokio::{sync::mpsc::Receiver, sync::Semaphore, time::Instant};

pub(crate) struct ObserverWorker<K: Key> {
    inner: Inner<K>,
    receiver: Receiver<Msg>,
    dump_sem: Arc<Semaphore>,
    async_oplock: Arc<Mutex<()>>,
    last_blob_update_time: Option<Instant>,
    debounce_interval: Duration,
}

impl<K: Key + 'static> ObserverWorker<K> {
    pub(crate) fn new(
        receiver: Receiver<Msg>,
        inner: Inner<K>,
        dump_sem: Arc<Semaphore>,
        async_oplock: Arc<Mutex<()>>,
    ) -> Self {
        let debounce_interval = Duration::from_millis(inner.config.debounce_interval_ms());
        Self {
            inner,
            receiver,
            dump_sem,
            async_oplock,
            last_blob_update_time: None,
            debounce_interval,
        }
    }

    pub(crate) async fn run(mut self) {
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
        match self.receiver.recv().await {
            Some(msg) => self.process_msg(msg).await,
            None => {
                return Err(anyhow!(
                    "all observer connected to this worker are dropped, so worker is done"
                        .to_string()
                ))
            }
        }
    }

    async fn process_msg(&mut self, msg: Msg) -> Result<()> {
        if !self.predicate_wrapper(&msg.predicate).await {
            return Ok(());
        }
        let _lock = self.async_oplock.lock().await;
        if !self.predicate_wrapper(&msg.predicate).await {
            return Ok(());
        }
        match msg.optype {
            OperationType::ForceUpdateActiveBlob => {
                if self.last_blob_update_time.is_none()
                    || Instant::now().saturating_duration_since(self.last_blob_update_time.unwrap())
                        > self.debounce_interval
                {
                    update_active_blob(self.inner.clone()).await?;
                    self.last_blob_update_time = Some(Instant::now());
                }
            }
            OperationType::CloseActiveBlob => {
                self.inner.close_active_blob().await?;
            }
            OperationType::CreateActiveBlob => {
                self.inner.create_active_blob().await?;
            }
            OperationType::RestoreActiveBlob => {
                self.inner.restore_active_blob().await?;
            }
            OperationType::TryDumpBlobIndexes => {
                self.inner
                    .try_dump_old_blob_indexes(self.dump_sem.clone())
                    .await;
            }
        }
        Ok(())
    }

    async fn predicate_wrapper(&self, predicate: &Option<ActiveBlobPred>) -> bool {
        if let Some(predicate) = predicate {
            predicate(self.inner.active_blob_stat().await)
        } else {
            true
        }
    }
}

async fn update_active_blob<K: Key + 'static>(inner: Inner<K>) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    trace!("obtaining new active blob");
    let new_active = Blob::open_new(next_name, inner.ioring, inner.config.index())
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
