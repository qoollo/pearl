use super::prelude::*;
use tokio::{
    sync::mpsc::Receiver,
    sync::Semaphore,
    time::{timeout_at, Instant, Duration},
};

pub(crate) struct ObserverWorker<K>
where
    for<'a> K: Key<'a>,
{
    inner: Arc<Inner<K>>,
    receiver: Receiver<Msg>,
    dump_sem: Arc<Semaphore>,
    deferred_index_dump: Option<DeferredEventData>,
    next_deadline: Option<Instant>
}

struct DeferredEventData {
    first_time: Instant,
    last_time: Instant,
}

#[derive(Debug)]
enum TickResult {
    Continue,
    Stop
}

impl<K> ObserverWorker<K>
where
    for<'a> K: Key<'a> + 'static,
{
    pub(crate) fn new(
        receiver: Receiver<Msg>,
        inner: Arc<Inner<K>>,
        dump_sem: Arc<Semaphore>,
    ) -> Self {
        Self {
            inner,
            receiver,
            dump_sem,
            deferred_index_dump: None,
            next_deadline: None
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let tick_result = 
                match self.next_deadline {
                    None => self.tick().await,
                    Some(deadline) => self.tick_with_deadline(deadline).await
                };

            match tick_result {
                Ok(TickResult::Continue) => {},
                Ok(TickResult::Stop) => {
                    debug!("ObserverWorker stopping. No future update is possible");
                    break;
                },
                Err(err) => {
                    error!("ObserverWorker unexpected error: {:?}", err);
                    panic!("ObserverWorker unexpected error: {:?}", err);
                }
            }
        }
        debug!("observer stopped");
    }

    async fn tick(&mut self) -> Result<TickResult> {
        match self.receiver.recv().await {
            Some(msg) => {
                self.process_msg(msg).await?;
                Ok(TickResult::Continue)
            },
            None => Ok(TickResult::Stop)
        }
    }

    async fn tick_with_deadline(&mut self, deadline: Instant) -> Result<TickResult> {
        // Extend deadline a little bit to guaranty, that process_defered will detect exceeding
        let deadline = deadline + Duration::from_millis(10);
        match timeout_at(deadline, self.receiver.recv()).await {
            Ok(Some(msg)) => {
                self.process_msg(msg).await?;
                Ok(TickResult::Continue)
            },
            Ok(None) => {
                Ok(TickResult::Stop)
            },
            Err(_) => {
                // Deadline reached
                self.next_deadline = None; // Reset deadline
                self.process_defered().await?;
                Ok(TickResult::Continue)
            }
        }
    }

    fn update_deadline(&mut self, deadline: Instant) {
        match self.next_deadline {
            None => {
                self.next_deadline = Some(deadline);
            },
            Some(prev_deadline) => {
                if deadline < prev_deadline {
                    self.next_deadline = Some(deadline);
                }
            }
        }
    }

    /// Processes messages
    async fn process_msg(&mut self, msg: Msg) -> Result<()> {
        if !self.predicate_wrapper(&msg.predicate).await {
            return Ok(());
        }
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

    /// Processes defered events
    async fn process_defered(&mut self) -> Result<()> {
        self.process_deffered_blob_index_dump().await?;

        Ok(())
    }

    async fn deffer_blob_indexes_dump(&mut self) -> Result<()> {
        if let Some(deferred) = &mut self.deferred_index_dump {
            deferred.update_last_time();
        } else {
            self.deferred_index_dump = Some(DeferredEventData::new());
        }

        if let Some(deferred) = &self.deferred_index_dump {
            let min = self.inner.config().deferred_min_time();
            let max = self.inner.config().deferred_max_time();
            let next_deadline = deferred.next_deadline(min, max);
            self.update_deadline(next_deadline);
        }

        Ok(())
    }

    async fn process_deffered_blob_index_dump(&mut self) -> Result<()> {
        if let Some(deferred) = &self.deferred_index_dump {
            let min = self.inner.config().deferred_min_time();
            let max = self.inner.config().deferred_max_time();
            if deferred.last_time.elapsed() >= min || deferred.first_time.elapsed() >= max {
                self.deferred_index_dump = None;
                self.process_msg(Msg::new(OperationType::TryDumpBlobIndexes, None)).await?;
            } else {
                let next_deadline = deferred.next_deadline(min, max);
                self.update_deadline(next_deadline);
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
            .config()
            .max_blob_size()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
        let config_max_count = self
            .inner
            .config()
            .max_data_in_blob()
            .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;

        {
            let read = self.inner.safe().read().await;
            if let Some(active_blob) = read.read_active_blob().await {
                if active_blob.file_size() < config_max_size
                    && (active_blob.records_count() as u64) < config_max_count
                {
                    return Ok(false);
                }
            };
        }

        let mut write = self.inner.safe().write().await;
        let mut replace = false;
        {
            if let Some(active_blob) = write.read_active_blob().await {
                if active_blob.file_size() >= config_max_size
                    || active_blob.records_count() as u64 >= config_max_count
                {
                    replace = true;
                }
            }
        }
        if replace {
            let new_active = get_new_active_blob(&self.inner).await?;
            write
                .replace_active_blob(Box::new(ASRwLock::new(*new_active)))
                .await?;
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
        .safe()
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
    let new_active = Blob::open_new(next_name, inner.io_driver().clone(), inner.config().blob())
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

    #[inline]
    fn next_deadline(&self, min: Duration, max: Duration) -> Instant {
        (self.first_time + max).min(self.last_time + min)
    }
}
