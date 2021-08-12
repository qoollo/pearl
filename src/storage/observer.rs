use super::prelude::*;
use tokio::{
    sync::mpsc::{channel, Sender},
    sync::Semaphore,
};

pub(crate) enum Msg {
    CreateActiveBlob,
    CloseActiveBlob,
    RestoreActiveBlob,
    ForceUpdateActiveBlob,
}

#[derive(Debug, Clone)]
pub(crate) struct Observer {
    inner: Option<Inner>,
    pub sender: Option<Sender<Msg>>,
    dump_sem: Arc<Semaphore>,
}

impl Observer {
    pub(crate) const fn new(inner: Inner, dump_sem: Arc<Semaphore>) -> Self {
        Self {
            inner: Some(inner),
            sender: None,
            dump_sem,
        }
    }

    pub(crate) fn run(&mut self) {
        if let Some(inner) = self.inner.take() {
            let (sender, receiver) = channel(1024);
            self.sender = Some(sender);
            let worker = ObserverWorker::new(receiver, inner, self.dump_sem.clone());
            tokio::spawn(worker.run());
        }
    }

    pub(crate) async fn force_update_active_blob(&self) {
        self.send_msg(Msg::ForceUpdateActiveBlob).await
    }

    pub(crate) async fn restore_active_blob(&self) {
        self.send_msg(Msg::RestoreActiveBlob).await
    }

    pub(crate) async fn close_active_blob(&self) {
        self.send_msg(Msg::CloseActiveBlob).await
    }

    pub(crate) async fn create_active_blob(&self) {
        self.send_msg(Msg::CreateActiveBlob).await
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
