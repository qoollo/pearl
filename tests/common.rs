use futures::executor::ThreadPool;
use std::env;

use pearl::{Builder, Storage};

pub fn default_test_storage() -> Result<Storage, String> {
    let path = env::temp_dir().join("pearl_test/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    storage.init().unwrap();
    Ok(storage)
}

pub fn write(storage: &Storage, base_number: usize, executor: ThreadPool) -> Result<(), String> {
    unimplemented!()
}

pub fn check(storage: &Storage, keys: Vec<usize>) -> Result<(), String> {
    unimplemented!()
}
