use super::prelude::*;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::{
    sync::Semaphore,
    sync::{
        mpsc::{channel, Sender},
        Mutex,
    },
};

pub trait CondChecker: Send + Sync {
    fn check(&self) -> Pin<Box<dyn Send + Future<Output = bool>>>;
}

pub(crate) enum OperationType {
    CreateActiveBlob = 0,
    CloseActiveBlob = 1,
    RestoreActiveBlob = 2,
    ForceUpdateActiveBlob = 3,
}

pub(crate) struct Msg {
    optype: OperationType,
    checker: Box<dyn CondChecker>,
    oplocker: Arc<Mutex<()>>,
}

impl Msg {
    pub(crate) fn new(
        optype: OperationType,
        checker: Box<dyn CondChecker>,
        oplocker: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            optype,
            checker,
            oplocker,
        }
    }

    pub(crate) fn optype(&self) -> &OperationType {
        &self.optype
    }

    pub(crate) fn locker(&self) -> Arc<Mutex<()>> {
        self.oplocker.clone()
    }

    pub(crate) fn checker(&self) -> &Box<dyn CondChecker> {
        &self.checker
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Observer {
    inner: Option<Inner>,
    pub sender: Option<Sender<Msg>>,
    dump_sem: Arc<Semaphore>,
    oplocker: Arc<Mutex<()>>,
}

impl Observer {
    pub(crate) fn new(inner: Inner, dump_sem: Arc<Semaphore>) -> Self {
        Self {
            inner: Some(inner),
            sender: None,
            dump_sem,
            oplocker: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.oplocker.try_lock().is_ok()
    }

    pub(crate) fn run(&mut self) {
        if let Some(inner) = self.inner.take() {
            let (sender, receiver) = channel(1024);
            self.sender = Some(sender);
            let worker = ObserverWorker::new(receiver, inner, self.dump_sem.clone());
            tokio::spawn(worker.run());
        }
    }

    pub(crate) async fn force_update_active_blob(
        &self,
        cond_checker: Box<dyn CondChecker>,
    ) -> bool {
        let msg = self.build_msg(OperationType::ForceUpdateActiveBlob, cond_checker);
        self.send_msg(msg).await;
        true
    }

    pub(crate) async fn restore_active_blob(&self, cond_checker: Box<dyn CondChecker>) -> bool {
        let msg = self.build_msg(OperationType::RestoreActiveBlob, cond_checker);
        self.send_msg(msg).await;
        true
    }

    pub(crate) async fn close_active_blob(&self, cond_checker: Box<dyn CondChecker>) -> bool {
        let msg = self.build_msg(OperationType::CloseActiveBlob, cond_checker);
        self.send_msg(msg).await;
        true
    }

    pub(crate) async fn create_active_blob(&self, cond_checker: Box<dyn CondChecker>) -> bool {
        let msg = self.build_msg(OperationType::CreateActiveBlob, cond_checker);
        self.send_msg(msg).await;
        true
    }

    fn build_msg(&self, optype: OperationType, cond_checker: Box<dyn CondChecker>) -> Msg {
        Msg::new(optype, cond_checker, self.oplocker.clone())
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
