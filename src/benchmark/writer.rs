use super::prelude::*;
use pearl::rio;

pub struct Writer<K> {
    storage: Storage<K>,
}

impl<K> Writer<K> {
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
        let storage = builder.build(rio).unwrap();
        Self { storage }
    }

    pub async fn init(&mut self) {
        self.storage.init().await.unwrap()
    }

    pub async fn write(self: Arc<Self>, key: K, data: Vec<u8>) -> Report
    where
        K: Key,
    {
        let record = Report::new(key.as_ref().len(), data.len());
        self.storage.clone().write(key, data).await.unwrap();
        record
    }

    pub async fn close(&self) {
        self.storage.close().await.unwrap();
    }
}
