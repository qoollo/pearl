use super::prelude::*;
use tokio::{
    sync::mpsc::{channel, Sender},
    sync::Semaphore,
};

pub(crate) enum OperationType {
    CreateActiveBlob = 0,
    CloseActiveBlob = 1,
    RestoreActiveBlob = 2,
    ForceUpdateActiveBlob = 3,
}

pub struct ActiveBlobStat {
    pub records_count: usize,
    pub index_memory: Option<usize>,
    pub file_size: Option<usize>,
}

pub struct ActiveBlobStatReq {
    pub records_count: bool,
    pub index_memory: bool,
    pub file_size: bool,
}

// NOTE: this must be FnOnce function, because it mustn't be executed more than once
// This will be an error in case:
// 1. user of Msg[1] struct commits explicitly
// 2. after that commit next observer operation changes `is_pending` on true and creates new Msg
// 3. Then Msg[1] is dropped (and commits again during that drop)
// * `is_pending` has wrong value (false, but operation may be in pending state) after that drop
type ActiveBlobPred = Box<dyn Fn(Option<ActiveBlobStat>) -> bool + Send + Sync>;

// Option<CommitFn> is used, because Drop::drop(&mut self) use mutable ref and commit_fn can't be moved
// from structure
pub(crate) struct Msg {
    optype: OperationType,
    predicate: ActiveBlobPred,
}

impl Msg {
    pub(crate) fn new(optype: OperationType, predicate: ActiveBlobPred) -> Self {
        Self { optype, predicate }
    }

    pub(crate) fn predicate(&self, stat: Option<ActiveBlobStat>) -> bool {
        (self.predicate)(stat)
    }

    pub(crate) fn optype(&self) -> &OperationType {
        &self.optype
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
        self.async_oplock.try_lock().is_some()
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

    pub(crate) async fn force_update_active_blob(&self) -> bool {
        if let Some(msg) = self.build_msg(OperationType::ForceUpdateActiveBlob) {
            self.send_msg(msg).await;
            true
        } else {
            false
        }
    }

    pub(crate) async fn restore_active_blob(&self) -> bool {
        if let Some(msg) = self.build_msg(OperationType::RestoreActiveBlob) {
            self.send_msg(msg).await;
            true
        } else {
            false
        }
    }

    pub(crate) async fn close_active_blob(&self) -> bool {
        if let Some(msg) = self.build_msg(OperationType::CloseActiveBlob) {
            self.send_msg(msg).await;
            true
        } else {
            false
        }
    }

    pub(crate) async fn create_active_blob(&self) -> bool {
        if let Some(msg) = self.build_msg(OperationType::CreateActiveBlob) {
            self.send_msg(msg).await;
            true
        } else {
            false
        }
    }

    fn build_msg(&self, optype: OperationType) -> Option<Msg> {
        let res = self.is_pending.compare_exchange(false, true, ORD, ORD);
        if res.is_ok() {
            let is_pending = self.is_pending.clone();
            let commit_fn = Box::new(move || is_pending.store(false, ORD));
            Some(Msg::new(optype, commit_fn))
        } else {
            None
        }
    }

    async fn send_msg(&self, msg: Msg) {
        if let Some(sender) = &self.sender {
            if let Err(e) = sender.send(msg).await {
                error!(
                    "observer cannot force update active blob: task failed: {}",
                    e
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
