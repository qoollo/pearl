use std::path::PathBuf;
use crate::statistics::Report;

use futures::executor::ThreadPool;
use pearl::*;
pub struct Writer {
    speed: usize,
    storage: Storage,
}

impl Writer {
    pub fn new(speed: usize, tmp_dir: PathBuf) -> Self {
        let storage = Builder::new()
            .blob_file_name_prefix("benchmark")
            .max_blob_size(100_000_000)
            .max_data_in_blob(10000)
            .work_dir(tmp_dir.join("pearl_benchmark"))
            .build()
            .unwrap();
        Self { speed, storage }
    }

    pub async fn init(&mut self, spawner: ThreadPool) {
        await!(self.storage.init(spawner)).unwrap();
    }

    pub async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Report {
        let report = Report::new(key.len(), value.len());
        let mut record = Record::new();
        record.set_body(key, value);
        print!("await storage record write ");
        await!(self.storage.clone().write(record)).unwrap();
        report
    }

    pub fn close(&mut self) {
        self.storage.close().unwrap();
    }
}
