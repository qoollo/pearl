use super::prelude::*;
use tokio::sync::{
    mpsc::{channel, Sender},
    Semaphore,
};

#[derive(Debug, Clone)]
pub(crate) enum OperationType {
    CreateActiveBlob = 0,
    CloseActiveBlob = 1,
    RestoreActiveBlob = 2,
    ForceUpdateActiveBlob = 3,
    TryDumpBlobIndexes = 4,
    TryUpdateActiveBlob = 5,
    DeferredDumpBlobIndexes = 6,
}

#[derive(Debug)]
pub struct ActiveBlobStat {
    pub records_count: usize,
    pub index_memory: usize,
    pub file_size: usize,
}

impl ActiveBlobStat {
    pub fn new(records_count: usize, index_memory: usize, file_size: usize) -> Self {
        Self {
            records_count,
            index_memory,
            file_size,
        }
    }
}

pub type ActiveBlobPred = fn(Option<ActiveBlobStat>) -> bool;

#[derive(Debug)]
pub(crate) struct Msg {
    pub(crate) optype: OperationType,
    pub(crate) predicate: Option<ActiveBlobPred>,
}

impl Msg {
    pub(crate) fn new(optype: OperationType, predicate: Option<ActiveBlobPred>) -> Self {
        Self { optype, predicate }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Observer<K>
where
    for<'a> K: Key<'a>,
{
    inner: Option<Arc<Inner<K>>>,
    pub sender: Option<Sender<Msg>>,
    dump_sem: Arc<Semaphore>,
}

impl<K> Observer<K>
where
    for<'a> K: Key<'a> + 'static,
{
    pub(crate) fn new(inner: Arc<Inner<K>>, dump_sem: Arc<Semaphore>) -> Self {
        Self {
            inner: Some(inner),
            sender: None,
            dump_sem,
        }
    }

    pub(crate) fn run(&mut self) {
        if let Some(inner) = self.inner.take() {
            let (sender, receiver) = channel(1024);
            let loop_sender = sender.clone();
            self.sender = Some(sender);
            let worker = ObserverWorker::new(
                receiver,
                loop_sender,
                inner,
                self.dump_sem.clone(),
            );
            tokio::spawn(worker.run());
        }
    }

    pub(crate) async fn force_update_active_blob(&self, predicate: ActiveBlobPred) {
        self.send_msg(Msg::new(
            OperationType::ForceUpdateActiveBlob,
            Some(predicate),
        ))
        .await
    }

    pub(crate) async fn restore_active_blob(&self) {
        self.send_msg(Msg::new(OperationType::RestoreActiveBlob, None))
            .await
    }

    pub(crate) async fn close_active_blob(&self) {
        self.send_msg(Msg::new(OperationType::CloseActiveBlob, None))
            .await
    }

    pub(crate) async fn create_active_blob(&self) {
        self.send_msg(Msg::new(OperationType::CreateActiveBlob, None))
            .await
    }

    pub(crate) async fn try_dump_old_blob_indexes(&self) {
        self.send_msg(Msg::new(OperationType::TryDumpBlobIndexes, None))
            .await
    }

    pub(crate) async fn defer_dump_old_blob_indexes(&self) {
        self.send_msg(Msg::new(OperationType::DeferredDumpBlobIndexes, None))
            .await
    }

    pub(crate) async fn try_update_active_blob(&self) {
        self.send_msg(Msg::new(OperationType::TryUpdateActiveBlob, None))
            .await
    }

    async fn send_msg(&self, msg: Msg) {
        if let Some(sender) = &self.sender {
            let optype = msg.optype.clone();
            if let Err(e) = sender.send(msg).await {
                error!(
                    "Can't send message to worker:\nOperation: {:?}\nReason: {:?}",
                    optype, e
                );
            }
        } else {
            error!("storage observer task was not launched");
        }
    }
}
