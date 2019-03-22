#![feature(futures_api, await_macro)]

use futures::{executor::ThreadPool, future::FutureExt};
use pearl::{Builder, Record};
use std::{env, fs};

#[test]
fn test_storage_init_new() {
    let path = env::temp_dir().join("pearl_new/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build::<u32>().unwrap();
    assert!(storage.init().map_err(|e| eprintln!("{:?}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 1);
    let blob_file_path = path.join("test.0.blob");
    assert!(blob_file_path.exists());
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_storage_init_from_existing() {
    let path = env::temp_dir().join("pearl_ex/");
    {
        let builder = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(1_000_000usize)
            .max_data_in_blob(1_000usize);
        let mut temp_storage = builder.build::<u32>().unwrap();
        temp_storage.init().unwrap();
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.join("test.1.blob"))
            .unwrap();
    }
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());

    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build::<u32>().unwrap();

    assert!(storage.init().map_err(|e| eprintln!("{:?}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    assert_eq!(
        path.join("test.1.blob"),
        storage.active_blob_path().unwrap()
    );
    fs::remove_file(path.join("test.0.blob")).unwrap();
    fs::remove_file(path.join("test.1.blob")).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_storage_read_write() {
    let path = env::temp_dir().join("pearl_rw/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build().unwrap();
    let key = "test".to_owned();
    storage.init().unwrap();
    let mut record = Record::new();
    record.set_data(b"test data string".to_vec());
    record.set_key(key.clone());
    let mut pool = ThreadPool::new().unwrap();
    let w = storage.write(record);
    let r = storage.read(key.clone());
    let rec = pool.run(w.then(|_| r)).unwrap();
    assert_eq!(rec.key(), &key);
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_storage_close() {
    let path = env::temp_dir().join("pearl_close/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build::<i32>().unwrap();
    assert!(storage.init().map_err(|e| eprintln!("{:?}", e)).is_ok());
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}
