use std::time::Duration;

use super::prelude::*;
use futures::TryFutureExt;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    sync::Semaphore,
    time::{sleep, Instant},
};

pub(crate) struct ObserverWorker<K>
where
    for<'a> K: Key<'a>,
{
    inner: Inner<K>,
    receiver: Receiver<Msg>,
    loopback_sender: Sender<Msg>,
    dump_sem: Arc<Semaphore>,
    async_oplock: Arc<Mutex<()>>,
    first_deferred_event_time: Arc<RwLock<Option<Instant>>>,
    last_deferred_event_time: Arc<RwLock<Option<Instant>>>,
}
/*
There is a time of the first event, T0
There is a time of the last occured event, T
There is a maximum time allowed, TDmax
There is a minimum time allowed, TDmin
If there are no events in TDMin since T, fire dump event
If TDmax passed since T0, fire dump event
T0 stays the same until event is fired
T changes with each event*/
impl<K> ObserverWorker<K>
where
    for<'a> K: Key<'a> + 'static,
{
    pub(crate) fn new(
        receiver: Receiver<Msg>,
        loopback_sender: Sender<Msg>,
        inner: Inner<K>,
        dump_sem: Arc<Semaphore>,
        async_oplock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            inner,
            receiver,
            loopback_sender,
            dump_sem,
            async_oplock,
            first_deferred_event_time: Arc::default(),
            last_deferred_event_time: Arc::default(),
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
                update_active_blob(&self.inner).await?;
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
            OperationType::DeferredDumpBlobIndexes => self.deffer_blob_indexes_dump().await?,
        }
        Ok(())
    }

    async fn deffer_blob_indexes_dump(&self) -> Result<()> {
        let sender = self.loopback_sender.clone();
        let first_time = self.first_deferred_event_time.clone();
        let last_time = self.last_deferred_event_time.clone();
        let minD = Duration::from_secs(10);
        let maxD = Duration::from_secs(100);
        let delay = async move {
            sleep(minD).await;
            let last_time = last_time.read().await.unwrap();
            let first_time = first_time.read().await.unwrap();
            if last_time.elapsed() >= minD || first_time.elapsed() >= maxD {
                let _ = sender
                    .send(Msg::new(OperationType::TryDumpBlobIndexes, None))
                    .await;
            }
            return Result::<(), ()>::Ok(());
        };
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

        let mut write = self.inner.safe.write().await;
        let active_blob = write.active_blob.as_ref();
        if let Some(active_blob) = active_blob {
            if active_blob.file_size() >= config_max_size
                || active_blob.records_count() as u64 >= config_max_count
            {
                let new_active = get_new_active_blob(&self.inner).await?;
                write.replace_active_blob(new_active).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

async fn update_active_blob<K>(inner: &Inner<K>) -> Result<()>
where
    for<'a> K: Key<'a> + 'static,
{
    let new_active = get_new_active_blob(inner).await?;
    inner
        .safe
        .write()
        .await
        .replace_active_blob(new_active)
        .await?;
    Ok(())
}

async fn get_new_active_blob<K>(inner: &Inner<K>) -> Result<Box<Blob<K>>>
where
    for<'a> K: Key<'a> + 'static,
{
    let next_name = inner.next_blob_name()?;
    trace!("obtaining new active blob");
    let new_active = Blob::open_new(next_name, inner.ioring.clone(), inner.config.index())
        .await?
        .boxed();
    Ok(new_active)
}
