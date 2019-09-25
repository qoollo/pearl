#![feature(repeat_generic_slice, async_closure)]

#[macro_use]
extern crate log;

use std::time::{Duration, Instant};
use std::{env, fs, path::PathBuf};

use futures::future::FutureExt;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt, TryStreamExt};
use pearl::{Builder, Meta, Storage};
use rand::seq::SliceRandom;
use tokio::timer::delay;

mod common;

use common::KeyTest;

#[tokio::test]
async fn test_storage_init_new() {
    let dir = common::init("new");
    let storage = common::default_test_storage_in(&dir).await.unwrap();
    assert_eq!(storage.blobs_count(), 1);
    let path = env::temp_dir().join(&dir);
    let blob_file_path = path.join("test.0.blob");
    assert!(blob_file_path.exists());
    common::clean(storage, dir).await.unwrap();
}

#[tokio::test]
async fn test_storage_init_from_existing() {
    let dir = common::init("existing");
    let path = env::temp_dir().join(&dir);
    let task = async {
        let builder = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(900_000)
            .max_data_in_blob(1_000);
        let mut temp_storage: Storage<KeyTest> = builder.build()?;
        temp_storage.init().await?;
        let mut records = common::generate_records(15, 100_000);
        while !records.is_empty() {
            let key = KeyTest::new(records.len() as u32);
            let value = records.pop().unwrap();
            let delay = delay(Instant::now() + Duration::from_millis(100));
            delay.then(|_| temp_storage.write(key, value)).await?;
        }
        temp_storage.close().await
    };
    task.await.unwrap();
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());

    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();

    assert!(storage.init().await.map_err(|e| error!("{:?}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    common::clean(storage, dir).await.unwrap();
}

#[tokio::test]
async fn test_storage_read_write() {
    let dir = common::init("read_write");
    let storage = common::default_test_storage_in(&dir).await.unwrap();
    let key = 1234;
    let data = b"test data string";
    write_one(&storage, 1234, data, None).await.unwrap();
    let new_data = storage.read(KeyTest::new(key)).await.unwrap();
    assert_eq!(new_data, data);
    common::clean(storage, dir).await.unwrap();
}

#[tokio::test]
async fn test_storage_multiple_read_write() {
    let dir = common::init("multiple");
    let storage = common::default_test_storage_in(&dir).await.unwrap();
    let keys = (0..100).collect::<Vec<u32>>();
    let data = b"test data string";

    let write_stream: FuturesUnordered<_> = keys
        .iter()
        .map(|key| write_one(&storage, *key, data, None))
        .collect();
    write_stream.collect::<Vec<_>>().await;
    let read_stream: FuturesUnordered<_> = keys
        .iter()
        .map(|key| storage.read(KeyTest::new(*key)))
        .collect();
    let data_from_file = read_stream
        .map_err(|e| dbg!(e))
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    common::clean(storage, dir).await.unwrap();
    assert_eq!(keys.len(), data_from_file.len());
}

#[tokio::test]
async fn test_multithread_read_write() -> Result<(), String> {
    let dir = common::init("multithread");
    let storage = common::default_test_storage_in(&dir).await?;
    let indexes = common::create_indexes(10, 10);
    let (snd, rcv) = tokio::sync::mpsc::channel(1024);
    let data = b"test data string";
    let clonned_storage = storage.clone();
    indexes.iter().cloned().for_each(move |mut range| {
        let st = clonned_storage.clone();
        let mut snd_cloned = snd.clone();
        std::thread::Builder::new()
            .name(format!("thread#{}", range[0]))
            .spawn(move || {
                let s = st.clone();
                let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
                range.shuffle(&mut rand::thread_rng());
                let task = async {
                    let start = range[0];
                    for i in range {
                        write_one(&s, i as u32, data, None).await.unwrap();
                    }
                    snd_cloned.send(start).await.unwrap();
                };
                rt.block_on(task);
            })
            .unwrap();
    });
    let handles = rcv.collect::<Vec<_>>().await;
    assert_eq!(handles.len(), 10);
    let keys = indexes
        .iter()
        .flatten()
        .map(|i| *i as u32)
        .collect::<Vec<_>>();
    common::check_all_written(&storage, keys)?;
    common::clean(storage, dir).await.unwrap();
    Ok(())
}

#[tokio::test]
async fn test_storage_multithread_blob_overflow() -> Result<(), String> {
    let dir = common::init("overflow");
    let storage = common::create_test_storage(&dir, 10_000).await.unwrap();
    let mut range: Vec<u32> = (0..100).collect();
    range.shuffle(&mut rand::thread_rng());
    let data = b"test data string".repeat(16);
    for i in range {
        delay(Instant::now() + Duration::from_millis(100)).await;
        write_one(&storage, i, &data, None).await.unwrap();
    }
    let path = env::temp_dir().join(&dir);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    common::clean(storage, dir).await
}

#[tokio::test]
async fn test_storage_close() {
    let dir = common::init("pearl_close");
    let builder = Builder::new()
        .work_dir(&dir)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage: Storage<KeyTest> = builder.build().unwrap();
    assert!(storage.init().await.map_err(|e| error!("{:?}", e)).is_ok());
    let path: PathBuf = dir.into();
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[tokio::test]
async fn test_on_disk_index() -> Result<(), String> {
    let dir = common::init("index");
    let data_size = 500;
    let max_blob_size = 1500;
    let num_records_to_write = 5u32;
    let read_key = 3u32;

    let path = env::temp_dir().join(&dir);
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .max_data_in_blob(1_000)
        .build()
        .unwrap();
    let slice = [17, 40, 29, 7, 75];
    let data: Vec<u8> = slice.repeat(data_size / slice.len());
    storage.init().await.unwrap();
    for i in 0..num_records_to_write {
        delay(Instant::now() + Duration::from_millis(100))
            .then(|_| write_one(&storage, i, &data, None))
            .await
            .unwrap();
    }
    while storage.blobs_count() < 2 {
        delay(Instant::now() + Duration::from_millis(200)).await;
    }
    assert!(path.join("test.1.blob").exists());
    let new_data = storage.read(KeyTest::new(read_key)).await.unwrap();
    assert_eq!(new_data, data);
    common::clean(storage, dir).await
}

#[tokio::test]
async fn test_work_dir_lock() {
    let dir = common::init("work_dir_lock");
    let storage_one = common::create_test_storage(&dir, 1_000_000);
    let res_one = storage_one.await;
    assert!(res_one.is_ok());
    let storage = res_one.unwrap();
    let storage_two = common::create_test_storage(&dir, 1_000_000);
    let res_two = storage_two.await;
    dbg!(&res_two);
    assert!(res_two.is_err());
    common::clean(storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}

#[tokio::test]
async fn test_index_from_blob() {
    let dir = common::init("index_from_blob");
    let storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    let data_chunks = common::generate_records(10, 10_000);
    for (i, data) in data_chunks.iter().enumerate() {
        write_one(&storage, i as u32, &data, None).await.unwrap();
    }
    storage.close().await.unwrap();
    let dir_path: PathBuf = env::temp_dir().join(&dir);
    let index_file_path = dir_path.join("test.0.index");
    fs::remove_file(&index_file_path).unwrap();
    let new_storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    assert!(index_file_path.exists());
    common::clean(new_storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}

#[tokio::test]
async fn test_write_with() {
    let dir = common::init("write_with");
    let storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    let key = 1234;
    let data = b"data_with_empty_meta";
    write_one(&storage, key, data, None).await.unwrap();
    let data = b"data_with_meta";
    write_one(&storage, key, data, Some("1.0")).await.unwrap();
    common::clean(storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}

#[tokio::test]
async fn test_write_with_with_on_disk_index() {
    let dir = common::init("write_with_with_on_disk_index");
    let storage = common::create_test_storage(&dir, 10_000).await.unwrap();

    let key = 1234;
    let data = b"data_with_empty_meta";
    write_one(&storage, key, data, None).await.unwrap();

    let records = common::generate_records(20, 1000);
    for (i, ref data) in records.into_iter().enumerate() {
        delay(Instant::now() + Duration::from_millis(32)).await;
        write_one(&storage, i as u32, data, Some("1.0"))
            .await
            .unwrap();
    }
    assert!(storage.blobs_count() > 1);

    let data = b"data_with_meta";
    write_one(&storage, key, data, Some("1.0")).await.unwrap();

    assert!(write_one(&storage, key, data, Some("1.0")).await.is_err());

    common::clean(storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}

#[tokio::test]
async fn test_write_1_000_records_with_same_key() {
    let now = Instant::now();
    let dir = common::init("write_1_000_000_records_with_same_key");
    let storage = common::create_test_storage(&dir, 10_000).await.unwrap();
    let key = KeyTest::new(1234);
    let value = b"data_with_empty_meta".to_vec();
    for i in 0..1_000 {
        let mut meta = Meta::new();
        meta.insert("version".to_owned(), i.to_string());
        delay(Instant::now() + Duration::from_micros(8)).await;
        storage.write_with(&key, value.clone(), meta).await.unwrap();
        if i % 8 == 0 {}
    }
    common::clean(storage, dir)
        .await
        .expect("work dir clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_read_with() {
    let now = Instant::now();
    let dir = common::init("read_with");
    let storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    debug!("storage created");
    let key = 2345;
    trace!("key: {}", key);
    let data = b"some_random_data";
    trace!("data: {:?}", data);
    write_one(&storage, key, data, Some("1.0")).await.unwrap();
    debug!("first data written");
    let data = b"some data with different version";
    trace!("data: {:?}", data);
    write_one(&storage, key, data, Some("2.0")).await.unwrap();
    debug!("second data written");
    let key = KeyTest::new(key);
    let data_read_with = storage.read_with(&key, &meta_with("2.0")).await.unwrap();
    debug!("read with finished");
    let data_read = storage.read(&key).await.unwrap();
    debug!("read finished");
    assert_ne!(data_read_with, data_read);
    assert_eq!(data_read_with, data);
    common::clean(storage, dir)
        .await
        .expect("work dir clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

async fn write_one(
    storage: &Storage<KeyTest>,
    key: u32,
    data: &[u8],
    version: Option<&str>,
) -> Result<(), String> {
    let data = data.to_vec();
    trace!("data: {:?}", data);
    let key = KeyTest::new(key);
    trace!("key: {:?}", key);
    if let Some(v) = version {
        debug!("write with");
        storage.write_with(key, data, meta_with(v)).await
    } else {
        debug!("write");
        storage.write(key, data).await
    }
    .map_err(|e| e.to_string())
}

fn meta_with(version: &str) -> Meta {
    let mut meta = Meta::new();
    meta.insert("version".to_owned(), version);
    meta
}
