use super::prelude::*;
use tokio::sync::{
    mpsc::{channel, Sender}
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
    state: ObserverState<K>
}

#[derive(Debug, Clone)]
enum ObserverState<K>
where
    for<'a> K: Key<'a>,
{
    Created(Arc<Inner<K>>),
    Running(Sender<Msg>),
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

        let (sender, receiver) = channel(1024);  
        let loop_sender = sender.clone();
        let dump_sem = inner.get_dump_sem();
        let worker = ObserverWorker::new(
            receiver,
            loop_sender,
            inner,
            dump_sem,
        );
        tokio::spawn(worker.run());

        self.state = ObserverState::Running(sender);
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
        if let ObserverState::Running(sender) = &self.state {
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
