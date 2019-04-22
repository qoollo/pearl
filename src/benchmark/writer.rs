use crate::statistics::Report;
use std::path::PathBuf;
use std::sync::Arc;

use futures::executor::ThreadPool;
use pearl::*;

pub struct Writer {
    storage: Storage,
}

impl Writer {
    pub fn new(tmp_dir: PathBuf) -> Self {
        let storage = Builder::new()
            .blob_file_name_prefix("benchmark")
            .max_blob_size(100_000_000)
            .max_data_in_blob(10000)
            .work_dir(tmp_dir.join("pearl_benchmark"))
            .build()
            .unwrap();
        Self { storage }
    }

    pub async fn init(&mut self, spawner: ThreadPool) {
        await!(self.storage.init(spawner)).unwrap();
    }

    pub async fn write(self: Arc<Self>, record: Record) -> Report {
        let report = Report::new(record.key_len(), record.data_len());
        await!(self.storage.clone().write(record)).unwrap();
        report
    }

    pub fn close(&self) {
        self.storage.close().unwrap();
    }
}
