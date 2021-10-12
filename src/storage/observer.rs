use super::prelude::*;
use tokio::{
    sync::{mpsc::{channel, Sender}, Semaphore},
};

#[derive(Debug, Clone)]
pub(crate) enum OperationType {
    CreateActiveBlob = 0,
    CloseActiveBlob = 1,
    RestoreActiveBlob = 2,
    ForceUpdateActiveBlob = 3,
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
pub(crate) struct Observer {
    inner: Option<Inner>,
    pub sender: Option<Sender<Msg>>,
    dump_sem: Arc<Semaphore>,
    async_oplock: Arc<Mutex<()>>,
}

impl Observer {
    pub(crate) fn new(inner: Inner, dump_sem: Arc<Semaphore>) -> Self {
        Self {
            inner: Some(inner),
            sender: None,
            dump_sem,
            async_oplock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.async_oplock.try_lock().is_none()
    }

    pub(crate) fn run(&mut self) {
        if let Some(inner) = self.inner.take() {
            let (sender, receiver) = channel(1024);
            self.sender = Some(sender);
            let worker = ObserverWorker::new(
                receiver,
                inner,
                self.dump_sem.clone(),
                self.async_oplock.clone(),
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

    pub(crate) fn get_dump_sem(&self) -> Arc<Semaphore> {
        self.dump_sem.clone()
    }
}
