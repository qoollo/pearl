use super::prelude::*;
use tokio::{sync::mpsc::Receiver, sync::Semaphore};

pub(crate) struct ObserverWorker<K: Key> {
    inner: Inner<K>,
    receiver: Receiver<Msg>,
    dump_sem: Arc<Semaphore>,
    async_oplock: Arc<Mutex<()>>,
}

impl<K: Key + 'static> ObserverWorker<K> {
    pub(crate) fn new(
        receiver: Receiver<Msg>,
        inner: Inner<K>,
        dump_sem: Arc<Semaphore>,
        async_oplock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            inner,
            receiver,
            dump_sem,
            async_oplock,
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
                update_active_blob(self.inner.clone()).await?;
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
            OperationType::TryUpdateActiveBlob => {
                if self.try_update_active_blob().await? {
                    self.inner
                        .try_dump_old_blob_indexes(self.dump_sem.clone())
                        .await;
                }
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

    async fn try_update_active_blob(&self) -> Result<bool> {
        let config_max_size = self
            .inner
            .config
            .max_blob_size()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
        let config_max_count = self
            .inner
            .config
            .max_data_in_blob()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;

        {
            let read = self.inner.safe.read().await;
            let active_blob = read.active_blob.as_ref();
            if let Some(active_blob) = active_blob {
                if active_blob.file_size() < config_max_size
                    && (active_blob.records_count() as u64) < config_max_count
                {
                    return Ok(false);
                }
            }
        }
        update_active_blob(&self.inner).await.map(|_| true)
    }
}

async fn update_active_blob<K: Key + 'static>(inner: Inner<K>) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    trace!("obtaining new active blob");
    let new_active = Blob::open_new(next_name, inner.ioring.clone(), inner.config.index())
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
