use super::prelude::*;
use pearl::rio;

pub struct Writer<K: Key> {
    storage: Storage<K>,
}

impl<K: Key> Writer<K> {
    pub fn new(
        tmp_dir: &Path,
        max_blob_size: u64,
        max_data_in_blob: u64,
        allow_duplicates: bool,
    ) -> Self {
        let mut builder = Builder::new()
            .blob_file_name_prefix("benchmark")
            .max_blob_size(max_blob_size)
            .max_data_in_blob(max_data_in_blob)
            .work_dir(tmp_dir.join("pearl_benchmark"));
        if allow_duplicates {
            info!("duplicates allowed");
            builder = builder.allow_duplicates();
        }

        let rio = rio::new().unwrap();
        let storage = builder.enable_aio(rio).build().unwrap();
        Self { storage }
    }

    pub async fn init(&mut self) {
        self.storage.init().await.unwrap()
    }

    pub async fn write(&self, key: impl AsRef<K>, data: Vec<u8>, mut tx: Sender<Report>) {
        let kbuf: &[u8] = key.as_ref().as_ref();
        let mut report = Report::new(kbuf.len(), data.len());
        let now = Instant::now();
        self.storage.write(key, data).await.unwrap();
        debug!("write finished");
        report.set_latency(now);
        tx.try_send(report).unwrap();
        debug!("report sent");
    }

    pub async fn close(&self) {
        self.storage.clone().close().await.unwrap();
    }
}
