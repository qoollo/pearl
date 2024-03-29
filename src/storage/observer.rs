use super::prelude::*;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::{channel, Sender};

/// Max size of the channel between `Observer` and `ObserverWorker`
const OBSERVER_CHANNEL_SIZE_LIMIT: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) enum OperationType {
    CreateActiveBlob = 0,
    CloseActiveBlob = 1,
    RestoreActiveBlob = 2,
    ForceUpdateActiveBlob = 3,
    TryDumpBlobIndexes = 4,
    TryUpdateActiveBlob = 5,
    DeferredDumpBlobIndexes = 6,
    TryFsyncData = 7,
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

#[derive(Debug)]
pub(crate) struct Observer<K>
where
    for<'a> K: Key<'a>,
{
    state: ObserverState<K>
}

#[derive(Debug)]
enum ObserverState<K>
where
    for<'a> K: Key<'a>,
{
    Created(Arc<Inner<K>>),
    Running(Sender<Msg>, JoinHandle<()>),
    Stopped
}

impl<K> Observer<K>
where
    for<'a> K: Key<'a> + 'static,
{
    pub(crate) fn new(inner: Arc<Inner<K>>) -> Self {
        Self {
            state: ObserverState::Created(inner)
        }
    }

    pub(crate) fn run(&mut self) {
        if !matches!(&self.state, ObserverState::Created(_)) {
            return;
        }

        let ObserverState::Created(inner) = std::mem::replace(&mut self.state, ObserverState::Stopped) else {
            unreachable!("State should be ObserverState::Created. It was checked at the beggining");
        };

        let (sender, receiver) = channel(OBSERVER_CHANNEL_SIZE_LIMIT);  
        let worker = ObserverWorker::new(
            receiver,
            inner
        );
        let handle = tokio::spawn(worker.run());

        self.state = ObserverState::Running(sender, handle);
    }

    pub(crate) async fn shutdown(mut self) {
        if let ObserverState::Running(sender, handle) = self.state {
            std::mem::drop(sender); // Drop sender. That trigger ObserverWorker stopping
            // Wait for completion
            if let Err(err) = handle.await {
                error!("Unexpected JoinError in Observer: {:?}", err);
            }
        }

        self.state = ObserverState::Stopped;
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

    pub(crate) async fn try_fsync_data(&self) {
        self.send_msg(Msg::new(OperationType::TryFsyncData, None))
            .await
    }

    async fn send_msg(&self, msg: Msg) {
        if let ObserverState::Running(sender, _) = &self.state {
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
