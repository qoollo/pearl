use std::time::Duration;

use super::prelude::*;
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
    deferred: Arc<Mutex<Option<DeferredEventData>>>,
    deferred_min_duration: Duration,
    deferred_max_duration: Duration,
}

struct DeferredEventData {
    first_time: Instant,
    last_time: Instant,
}

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
        let deferred_min_duration = inner.config.deferred_min_time().clone();
        let deferred_max_duration = inner.config.deferred_max_time().clone();
        Self {
            inner,
            receiver,
            loopback_sender,
            dump_sem,
            async_oplock,
            deferred: Arc::default(),
            deferred_min_duration,
            deferred_max_duration,
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
        let mut deferred = self.deferred.lock().await;
        if let Some(ref mut deferred) = *deferred {
            deferred.update_last_time();
        } else {
            let sender = self.loopback_sender.clone();
            let data = DeferredEventData::new();
            *deferred = Some(data);
            let min = self.deferred_min_duration.clone();
            let max = self.deferred_max_duration.clone();
            let deferred_local = self.deferred.clone();
            tokio::spawn(async move {
                loop {
                    sleep(min).await;
                    let mut deferred = deferred_local.lock().await;
                    if deferred.as_ref().unwrap().last_time.elapsed() >= min
                        || deferred.as_ref().unwrap().first_time.elapsed() >= max
                    {
                        let _ = sender
                            .send(Msg::new(OperationType::TryDumpBlobIndexes, None))
                            .await;
                        *deferred = None;
                        return;
                    }
                }
            });
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
                let active_blob = active_blob.read().await;
                if active_blob.file_size() < config_max_size
                    && (active_blob.records_count() as u64) < config_max_count
                {
                    return Ok(false);
                }
            }
        }

        let mut write = self.inner.safe.write().await;
        let mut replace = false;
        {
            let active_blob = write.active_blob.as_ref();
            if let Some(active_blob) = active_blob {
                let active_blob = active_blob.read().await;
                if active_blob.file_size() >= config_max_size
                    || active_blob.records_count() as u64 >= config_max_count
                {
                    replace = true;
                }
            }
        }
        if replace {
            let new_active = get_new_active_blob(&self.inner).await?;
            write.replace_active_blob(Box::new(ASRwLock::new(*new_active))).await?;
            return Ok(true);
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
        .replace_active_blob(Box::new(ASRwLock::new(*new_active)))
        .await?;
    Ok(())
}

async fn get_new_active_blob<K>(inner: &Inner<K>) -> Result<Box<Blob<K>>>
where
    for<'a> K: Key<'a> + 'static,
{
    let next_name = inner.next_blob_name()?;
    trace!("obtaining new active blob");
    let new_active = Blob::open_new(next_name, inner.ioring.clone(), inner.config.blob())
        .await?
        .boxed();
    Ok(new_active)
}

impl DeferredEventData {
    fn new() -> Self {
        let time = Instant::now();
        Self {
            first_time: time,
            last_time: time,
        }
    }

    fn update_last_time(&mut self) {
        self.last_time = Instant::now();
    }
}
