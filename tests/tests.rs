#![feature(futures_api, await_macro, repeat_generic_slice, duration_float)]

use futures::{
    executor::ThreadPool,
    future::FutureExt,
    stream::{futures_ordered, StreamExt, TryStreamExt},
};
use pearl::{Builder, Record};
use rand::seq::SliceRandom;
use std::{env, fs};

#[test]
fn test_storage_init_new() {
    let path = env::temp_dir().join("pearl_new/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
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
            .max_blob_size(1_000_000)
            .max_data_in_blob(1_000);
        let mut temp_storage = builder.build().unwrap();
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
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();

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
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    storage.init().unwrap();
    let key = "test-test".to_owned();
    let data = b"test data string".to_vec();
    let mut record = Record::new(&key, &data);
    record.set_body(key.clone(), data.clone());
    let mut pool = ThreadPool::new().unwrap();
    let w = storage.write(&mut record).unwrap();
    let r = storage.read(&key).unwrap();
    let rec = pool.run(w.then(|_| r)).unwrap();
    assert_eq!(rec.data().len(), data.len());
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
    assert_eq!(rec.key(), key.as_bytes());
}

#[test]
fn test_storage_multiple_read_write() {
    let path = env::temp_dir().join("pearl_mrw/");
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000)
        .build()
        .unwrap();
    storage.init().unwrap();
    let mut keys = Vec::new();
    let mut records: Vec<_> = (0..1000)
        .map(|i| {
            let data = b"viva".repeat(i + 1);
            let key = format!("key{}", i);
            let mut rec = Record::new(&key, &data);
            keys.push(key.clone());
            rec.set_body(key, data);
            rec
        })
        .collect();
    records.shuffle(&mut rand::thread_rng());
    let write_futures: Vec<_> = records
        .iter_mut()
        .map(|mut record| storage.write(&mut record).unwrap())
        .collect();
    let write_stream = futures_ordered(write_futures);
    let mut pool = ThreadPool::new().unwrap();
    let now = std::time::Instant::now();
    pool.run(write_stream.map_err(|e| dbg!(e)).collect::<Vec<_>>());
    let elapsed = now.elapsed().as_secs_f64();
    let blob_file_path = path.join("test.0.blob");
    let written = fs::metadata(&blob_file_path).unwrap().len();
    println!("write {}B/s", written as f64 / elapsed);
    let read_futures: Vec<_> = keys.iter().map(|key| storage.read(key).unwrap()).collect();
    let read_stream = futures_ordered(read_futures);
    let now = std::time::Instant::now();
    let mut records_from_file = pool.run(
        read_stream
            .map_err(|e| dbg!(e))
            .map(Result::unwrap)
            .collect::<Vec<_>>(),
    );
    let elapsed = now.elapsed().as_secs_f64();
    records.sort_by_key(|record| record.key().to_owned());
    records_from_file.sort_by_key(|record| record.key().to_owned());
    let written = fs::metadata(&blob_file_path).unwrap().len();
    println!("read {}B/s", written as f64 / elapsed);
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
    assert_eq!(records.len(), records_from_file.len());
    assert_eq!(records, records_from_file);
}

#[test]
fn test_storage_close() {
    let path = env::temp_dir().join("pearl_close/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    assert!(storage.init().map_err(|e| eprintln!("{:?}", e)).is_ok());
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}
