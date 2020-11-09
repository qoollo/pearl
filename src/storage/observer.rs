use super::prelude::*;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};

pub(crate) enum Msg {
    CloseActiveBlob,
    Shutdown,
}

#[derive(Debug, Clone)]
pub(crate) struct Observer {
    inner: Inner,
    update_interval: Duration,
    pub sender: Option<Sender<Msg>>,
}

impl Observer {
    pub(crate) const fn new(update_interval: Duration, inner: Inner) -> Self {
        Self {
            update_interval,
            inner,
            sender: None,
        }
    }

    pub(crate) fn run(&mut self) {
        let (sender, receiver) = channel(1024);
        self.sender = Some(sender);
        tokio::spawn(Self::worker(self.clone(), receiver));
    }

    async fn worker(self, mut receiver: Receiver<Msg>) {
        debug!("update interval: {:?}", self.update_interval);
        loop {
            if let Err(e) = self.tick(&mut receiver).await {
                warn!("active blob will no longer be updated, shutdown the system");
                warn!("{}", e);
                break;
            }
        }
        info!("observer stopped");
    }

    async fn tick(&self, receiver: &mut Receiver<Msg>) -> Result<()> {
        match timeout(self.update_interval, receiver.recv()).await {
            Ok(Some(Msg::CloseActiveBlob)) => update_active_blob(self.inner.clone()).await?,
            Ok(Some(Msg::Shutdown)) | Ok(None) => return Err(anyhow!("internal".to_string())),
            Err(_) => {}
        }
        trace!("check active blob");
        self.try_update().await
    }

    async fn try_update(&self) -> Result<()> {
        trace!("try update active blob");
        let inner_cloned = self.inner.clone();
        if let Some(inner) = active_blob_check(inner_cloned).await? {
            update_active_blob(inner).await?;
        }
        Ok(())
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

    pub(crate) fn shutdown(&self) {
        if let Some(sender) = &self.sender {
            if let Err(e) = sender.try_send(Msg::Shutdown) {
                error!("{}", e);
            }
        }
    }
}

async fn active_blob_check(inner: Inner) -> Result<Option<Inner>> {
    let (active_size, active_count) = {
        trace!("await for lock");
        let safe_locked = inner.safe.lock().await;
        trace!("lock acquired");
        let active_blob = safe_locked
            .active_blob
            .as_ref()
            .ok_or_else(Error::active_blob_not_set)?;
        let size = active_blob.file_size();
        let count = active_blob.records_count() as u64;
        (size, count)
    };
    trace!("lock released");
    let config_max_size = inner
        .config
        .max_blob_size()
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    let config_max_count = inner
        .config
        .max_data_in_blob()
        .ok_or_else(|| Error::from(ErrorKind::Uninitialized))?;
    if active_size as u64 > config_max_size || active_count >= config_max_count {
        Ok(Some(inner))
    } else {
        Ok(None)
    }
}

#[cfg(feature = "aio")]
async fn update_active_blob(inner: Inner) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    let new_active = Blob::open_new(next_name, inner.ioring.clone(), inner.config.filter())
        .await?
        .boxed();
    inner
        .safe
        .lock()
        .await
        .replace_active_blob(new_active)
        .await
}

#[cfg(not(feature = "aio"))]
async fn update_active_blob(inner: Inner) -> Result<()> {
    let next_name = inner.next_blob_name()?;
    // Opening a new blob may take a while
    let new_active = Blob::open_new(next_name, inner.config.filter())
        .await?
        .boxed();
    inner
        .safe
        .lock()
        .await
        .replace_active_blob(new_active)
        .await
}
