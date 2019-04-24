use crate::statistics::Report;
use std::path::PathBuf;
use std::sync::Arc;

use futures::executor::ThreadPool;
use pearl::*;

pub struct Writer {
    storage: Storage,
}

impl Writer {
    pub fn new(tmp_dir: PathBuf, max_blob_size: u64, max_data_in_blob: u64) -> Self {
        let storage = Builder::new()
            .blob_file_name_prefix("benchmark")
            .max_blob_size(max_blob_size)
            .max_data_in_blob(max_data_in_blob)
            .work_dir(tmp_dir.join("pearl_benchmark"))
            .build()
            .unwrap();
        Self { storage }
    }

    pub async fn init(&mut self, spawner: ThreadPool) {
        await!(self.storage.init(spawner)).unwrap();
    }

    pub async fn write(self: Arc<Self>, key: Vec<u8>, data: Vec<u8>) -> Report {
        let record = Report::new(key.len(), data.len());
        await!(self.storage.clone().write(key, data)).unwrap();
        record
    }

    pub fn close(&self) {
        self.storage.close().unwrap();
    }
}
