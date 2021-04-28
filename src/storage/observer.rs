use super::prelude::*;
use tokio::{
    sync::mpsc::{channel, Sender},
    sync::Semaphore,
};

pub(crate) enum Msg {
    CloseActiveBlob,
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

    pub(crate) async fn close_active_blob(&self) {
        if let Some(sender) = &self.sender {
            if let Err(e) = sender.send(Msg::CloseActiveBlob).await {
                error!("observer cannot close active blob: task failed: {}", e);
            }
        } else {
            error!("storage observer task was not launched");
        }
    }

    pub(crate) fn get_dump_sem(&self) -> Arc<Semaphore> {
        self.dump_sem.clone()
    }
}
