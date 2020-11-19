#[macro_use]
extern crate log;

use anyhow::Result as AnyResult;
use futures::{
    future::FutureExt,
    stream::{futures_unordered::FuturesUnordered, StreamExt, TryStreamExt},
    TryFutureExt,
};
use pearl::{Builder, Meta, Storage};
use rand::{seq::SliceRandom, Rng};
use std::{
    fs,
    time::{Duration, Instant},
};
use tokio::time::sleep;

mod common;

use common::KeyTest;

#[tokio::test]
async fn test_storage_init_new() {
    let path = common::init("new");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    // check if active blob created
    assert_eq!(storage.blobs_count(), 1);
    // check if active blob file exists
    assert!(path.join("test.0.blob").exists());
    common::clean(storage, path).await.unwrap();
}

#[tokio::test]
async fn test_storage_init_from_existing() {
    let now = Instant::now();
    let path = common::init("existing");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let records = common::generate_records(15, 1_000);
    for (key, data) in &records {
        sleep(Duration::from_millis(100)).await;
        write_one(&storage, *key, data, None).await.unwrap();
    }
    storage.close().await.unwrap();
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    assert!(!path.join("pearl.lock").exists());

    let storage = common::default_test_storage_in(&path).await.unwrap();
    assert_eq!(storage.blobs_count(), 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    common::clean(storage, path).await.unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_storage_read_write() {
    let now = Instant::now();
    let path = common::init("read_write");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let key = 1234;
    let data = b"test data string";
    write_one(&storage, 1234, data, None).await.unwrap();
    let new_data = storage.read(KeyTest::new(key)).await.unwrap();
    assert_eq!(new_data, data);
    common::clean(storage, path).await.unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_storage_multiple_read_write() {
    let now = Instant::now();
    let path = common::init("multiple");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let keys = (0..100).collect::<Vec<u32>>();
    let data = b"test data string";

    keys.iter()
        .map(|key| write_one(&storage, *key, data, None))
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;
    let data_from_file = keys
        .iter()
        .map(|key| storage.read(KeyTest::new(*key)))
        .collect::<FuturesUnordered<_>>()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(keys.len(), data_from_file.len());
    common::clean(storage, path).await.unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_multithread_read_write() -> Result<(), String> {
    let now = Instant::now();
    let path = common::init("multithread");
    let storage = common::default_test_storage_in(&path).await?;
    let threads = 20;
    let writes = 25;
    let indexes = common::create_indexes(threads, writes);
    let data = vec![184u8; 3000];
    let clonned_storage = storage.clone();
    let handles: FuturesUnordered<_> = indexes
        .iter()
        .cloned()
        .map(|mut range| {
            let st = clonned_storage.clone();
            let data = data.clone();
            let task = async move {
                let s = st.clone();
                let clonned_data = data.clone();
                range.shuffle(&mut rand::thread_rng());
                for i in range {
                    write_one(&s, i as u32, &clonned_data, None).await.unwrap();
                    sleep(Duration::from_millis(2)).await;
                }
            };
            tokio::spawn(task)
        })
        .collect();
    let handles = handles.try_collect::<Vec<_>>().await.unwrap();
    let index = path.join("test.0.index");
    sleep(Duration::from_millis(32)).await;
    assert!(index.exists());
    assert_eq!(handles.len(), threads);
    let keys = indexes
        .iter()
        .flatten()
        .map(|i| *i as u32)
        .collect::<Vec<_>>();
    debug!("make sure that all keys was written");
    common::check_all_written(&storage, keys).await?;
    common::clean(storage, path).await.unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
    Ok(())
}

#[tokio::test]
async fn test_storage_multithread_blob_overflow() -> AnyResult<()> {
    let now = Instant::now();
    let path = common::init("overflow");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let mut range: Vec<u32> = (0..90).collect();
    range.shuffle(&mut rand::thread_rng());
    let data = "test data string".repeat(16).as_bytes().to_vec();
    for i in range {
        sleep(Duration::from_millis(10)).await;
        write_one(&storage, i, &data, None).await.unwrap();
    }
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    common::clean(storage, &path).await?;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
    Ok(())
}

#[tokio::test]
async fn test_storage_close() {
    let now = Instant::now();
    let path = common::init("pearl_close");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    storage.close().await.unwrap();
    let blob_file_path = path.join("test.0.blob");
    let lock_file_path = path.join("pearl.lock");
    assert!(blob_file_path.exists());
    assert!(!lock_file_path.exists());
    fs::remove_dir_all(path).unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_on_disk_index() -> AnyResult<()> {
    let now = Instant::now();
    let path = common::init("index");
    let data_size = 500;
    let max_blob_size = 1500;
    let num_records_to_write = 5u32;
    let read_key = 3u32;

    let ioring = rio::new().expect("create uring");
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .max_data_in_blob(1_000)
        .enable_aio(ioring)
        .build()
        .unwrap();
    let slice = [17, 40, 29, 7, 75];
    let mut data = Vec::new();
    for _ in 0..(data_size / slice.len()) {
        data.extend(&slice);
    }
    storage.init().await.unwrap();
    info!("write (0..{})", num_records_to_write);
    for i in 0..num_records_to_write {
        sleep(Duration::from_millis(100)).await;
        write_one(&storage, i, &data, None).await.unwrap();
    }
    while storage.blobs_count() < 2 {
        sleep(Duration::from_millis(200)).await;
    }
    assert!(path.join("test.1.blob").exists());
    info!("read {}", read_key);
    let new_data = storage.read(KeyTest::new(read_key)).await.unwrap();
    assert_eq!(new_data, data);
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
    common::clean(storage, path).await
}

#[tokio::test]
async fn test_work_dir_lock() {
    let now = Instant::now();
    let path = common::init("work_dir_lock");
    let storage_one = common::create_test_storage(&path, 1_000_000);
    let res_one = storage_one.await;
    assert!(res_one.is_ok());
    let storage = res_one.unwrap();
    let storage_two = common::create_test_storage(&path, 1_000_000);
    let res_two = storage_two.await;
    dbg!(&res_two);
    assert!(res_two.is_err());
    common::clean(storage, path)
        .map(|res| res.expect("clean failed"))
        .await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_index_from_blob() {
    let now = Instant::now();
    let path = common::init("index_from_blob");
    let storage = common::create_test_storage(&path, 70_000).await.unwrap();
    let records = common::generate_records(10, 10_000);
    for (i, data) in &records {
        write_one(&storage, *i, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    storage.close().await.unwrap();
    let index_file_path = path.join("test.0.index");
    fs::remove_file(&index_file_path).unwrap();
    let new_storage = common::create_test_storage(&path, 1_000_000).await.unwrap();
    assert!(index_file_path.exists());
    common::clean(new_storage, path)
        .map(|res| res.expect("clean failed"))
        .await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_index_from_empty_blob() {
    let now = Instant::now();
    let path = common::init("index_from_empty_blob");
    let storage = common::default_test_storage_in(path.clone()).await.unwrap();
    storage.close().await.unwrap();
    let index_file_path = path.join("test.0.index");
    assert!(!index_file_path.exists());
    let blob_file_path = path.join("test.0.blob");
    assert!(blob_file_path.exists());
    let new_storage = common::create_test_storage(&path, 1_000_000).await.unwrap();
    new_storage
        .write(KeyTest::new(1), vec![1; 8])
        .await
        .unwrap();
    new_storage.close().await.unwrap();
    assert!(index_file_path.exists());
    fs::remove_dir_all(path).unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_write_with() {
    let now = Instant::now();
    let path = common::init("write_with");
    let storage = common::create_test_storage(&path, 1_000_000).await.unwrap();
    let key = 1234;
    let data = b"data_with_empty_meta";
    write_one(&storage, key, data, None).await.unwrap();
    let data = b"data_with_meta";
    write_one(&storage, key, data, Some("1.0")).await.unwrap();
    common::clean(storage, path)
        .map(|res| res.expect("clean failed"))
        .await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_write_with_with_on_disk_index() {
    let now = Instant::now();
    let path = common::init("write_with_with_on_disk_index");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();

    let key = 1234;
    let data = b"data_with_empty_meta";
    write_one(&storage, key, data, None).await.unwrap();

    let records = common::generate_records(20, 1000);
    for (i, data) in &records {
        sleep(Duration::from_millis(32)).await;
        write_one(&storage, *i, data, Some("1.0")).await.unwrap();
    }
    assert!(storage.blobs_count() > 1);

    // let data = b"data_with_meta";
    // write_one(&storage, key, data, Some("1.0")).await.unwrap();

    // assert!(write_one(&storage, key, data, Some("1.0")).await.is_err());

    common::clean(storage, path)
        .map(|res| res.expect("clean failed"))
        .await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_write_512_records_with_same_key() {
    let now = Instant::now();
    let path = common::init("write_1_000_000_records_with_same_key");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let key = KeyTest::new(1234);
    let value = b"data_with_empty_meta".to_vec();
    for i in 0..512 {
        let mut meta = Meta::new();
        meta.insert("version".to_owned(), i.to_string());
        sleep(Duration::from_micros(1)).await;
        storage.write_with(&key, value.clone(), meta).await.unwrap();
    }
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_read_with() {
    let now = Instant::now();
    let path = common::init("read_with");
    let storage = common::create_test_storage(&path, 1_000_000).await.unwrap();
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
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_read_all_load_all() {
    let now = Instant::now();
    let path = common::init("read_all");
    let storage = common::create_test_storage(&path, 100_000).await.unwrap();
    let key = 3456;
    let records_write = common::generate_records(100, 9_000);
    let mut versions = Vec::new();
    for (count, (i, data)) in records_write.iter().enumerate() {
        let v = i.to_string();
        versions.push(v.as_bytes().to_vec());
        write_one(&storage, key, data, Some(&v)).await.unwrap();
        sleep(Duration::from_millis(1)).await;
        assert_eq!(storage.records_count().await, count + 1);
    }
    assert_eq!(storage.records_count().await, records_write.len());
    sleep(Duration::from_millis(100)).await;
    let records_read = storage
        .read_all(&KeyTest::new(key))
        .and_then(|entry| {
            entry
                .into_iter()
                .map(|e| e.load())
                .collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>()
        })
        .await
        .unwrap();
    let mut versions2 = records_read
        .iter()
        .map(|r| r.meta().get("version").unwrap().to_vec())
        .collect::<Vec<_>>();
    versions.sort();
    versions2.sort();
    for v1 in &versions {
        if !versions2.contains(v1) {
            debug!("MISSED: {:?}", v1);
        }
    }
    assert_eq!(versions2.len(), versions.len());
    assert_eq!(versions2, versions);
    let mut records = records_write
        .into_iter()
        .map(|(_, data)| data)
        .collect::<Vec<_>>();
    let mut records_read = records_read
        .into_iter()
        .map(|r| r.into_data())
        .collect::<Vec<_>>();
    records.sort();
    records_read.sort();
    assert_eq!(records.len(), records_read.len());
    assert_eq!(records, records_read);
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_read_all_find_one_key() {
    let now = Instant::now();
    let path = common::init("read_all_1000_find_one_key");
    let storage = common::create_test_storage(&path, 1_000_000).await.unwrap();
    let count = 1000;
    let size = 3_000;
    info!("generate {} records with size {}", count, size);
    let records_write = common::generate_records(count, size);
    for (i, data) in &records_write {
        write_one(&storage, *i, data, None).await.unwrap();
    }
    let key = records_write.last().unwrap().0;
    debug!("read all with key: {:?}", &key);
    let records_read = storage
        .read_all(&KeyTest::new(key))
        .and_then(|entry| {
            entry
                .into_iter()
                .map(|e| e.load())
                .collect::<FuturesUnordered<_>>()
                .map(|e| e.map(|r| r.into_data()))
                .try_collect::<Vec<_>>()
        })
        .await
        .unwrap();
    debug!("storage read all finished");
    assert_eq!(
        records_write
            .iter()
            .find_map(|(i, data)| if *i == key { Some(data) } else { None })
            .unwrap(),
        &records_read[0]
    );
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_check_bloom_filter_single() {
    let now = Instant::now();
    let path = common::init("contains_bloom_filter_single");
    let storage = common::create_test_storage(&path, 10).await.unwrap();
    let data = b"some_random_data";
    let repeat = 8192;
    for i in 0..repeat {
        let pos_key = KeyTest::new(i + repeat);
        let neg_key = KeyTest::new(i + 2 * repeat);
        trace!("key: {}, pos: {:?}, negative: {:?}", i, pos_key, neg_key);
        let key = KeyTest::new(i);
        storage.write(&key, data.to_vec()).await.unwrap();
        assert_eq!(storage.check_bloom(key).await, Some(true));
        let data = b"other_random_data";
        storage.write(&pos_key, data.to_vec()).await.unwrap();
        assert_eq!(storage.check_bloom(pos_key).await, Some(true));
        assert_eq!(storage.check_bloom(neg_key).await, Some(false));
    }
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}
#[tokio::test]
async fn test_check_bloom_filter_multiple() {
    let now = Instant::now();
    let path = common::init("check_bloom_filter_multiple");
    let storage = common::create_test_storage(&path, 20000).await.unwrap();
    let data =
        b"lfolakfsjher_rladncreladlladkfsje_pkdieldpgkeolladkfsjeslladkfsj_slladkfsjorladgedom_dladlladkfsjlad";
    for i in 1..800 {
        let key = KeyTest::new(i);
        storage.write(&key, data.to_vec()).await.unwrap();
        sleep(Duration::from_millis(6)).await;
        trace!("blobs count: {}", storage.blobs_count());
    }
    for i in 1..800 {
        assert_eq!(storage.check_bloom(KeyTest::new(i)).await, Some(true));
    }
    for i in 800..1600 {
        assert_eq!(storage.check_bloom(KeyTest::new(i)).await, Some(false));
    }
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_check_bloom_filter_init_from_existing() {
    let now = Instant::now();
    let path = common::init("check_bloom_filter_init_from_existing");
    debug!("open new storage");
    let base = 20_000;
    {
        let storage = common::create_test_storage(&path, 100_000).await.unwrap();
        debug!("write some data");
        let data =
        b"lfolakfsjher_rladncreladlladkfsje_pkdieldpgkeolladkfsjeslladkfsj_slladkfsjorladgedom_dladlladkfsjlad";
        for i in 1..base {
            let key = KeyTest::new(i);
            trace!("write key: {}", i);
            storage.write(&key, data.to_vec()).await.unwrap();
            trace!("blobs count: {}", storage.blobs_count());
        }
        debug!("close storage");
        storage.close().await.unwrap();
    }

    debug!("storage closed, await a little");
    sleep(Duration::from_millis(1000)).await;
    debug!("reopen storage");
    let storage = common::create_test_storage(&path, 100).await.unwrap();
    debug!("check check_bloom");
    for i in 1..base {
        assert_eq!(storage.check_bloom(KeyTest::new(i)).await, Some(true));
    }
    info!("check certainly missed keys");
    let mut false_positive_counter = 0usize;
    for i in base..base * 2 {
        trace!("check key: {}", i);
        if storage.check_bloom(KeyTest::new(i)).await == Some(true) {
            false_positive_counter += 1;
        }
    }
    let fpr = false_positive_counter as f64 / base as f64;
    info!("false positive rate: {:.6} < 0.001", fpr);
    assert!(fpr < 0.001);
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_check_bloom_filter_generated() {
    let now = Instant::now();
    let path = common::init("check_bloom_filter_generated");
    debug!("open new storage");
    let base = 20_000;
    {
        let storage = common::create_test_storage(&path, 100_000).await.unwrap();
        debug!("write some data");
        let data =
        b"lfolakfsjher_rladncreladlladkfsje_pkdieldpgkeolladkfsjeslladkfsj_slladkfsjorladgedom_dladlladkfsjlad";
        for i in 1..base {
            let key = KeyTest::new(i);
            trace!("write key: {}", i);
            storage.write(&key, data.to_vec()).await.unwrap();
            trace!("blobs count: {}", storage.blobs_count());
        }
        debug!("close storage");
        storage.close().await.unwrap();
        let index_file_path = path.join("test.0.index");
        fs::remove_file(index_file_path).unwrap();
        info!("index file removed");
    }

    debug!("storage closed, await a little");
    sleep(Duration::from_millis(1000)).await;
    debug!("reopen storage");
    let storage = common::create_test_storage(&path, 100).await.unwrap();
    debug!("check check_bloom");
    for i in 1..base {
        trace!("check key: {}", i);
        assert_eq!(storage.check_bloom(KeyTest::new(i)).await, Some(true));
    }
    info!("check certainly missed keys");
    let mut false_positive_counter = 0usize;
    for i in base..base * 2 {
        trace!("check key: {}", i);
        if storage.check_bloom(KeyTest::new(i)).await == Some(true) {
            false_positive_counter += 1;
        }
    }
    let fpr = false_positive_counter as f64 / base as f64;
    info!("false positive rate: {:.6} < 0.001", fpr);
    assert!(fpr < 0.001);
    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

async fn write_one(
    storage: &Storage<KeyTest>,
    key: u32,
    data: &[u8],
    version: Option<&str>,
) -> AnyResult<()> {
    let data = data.to_vec();
    let key = KeyTest::new(key);
    debug!("tests write one key: {:?}", key);
    if let Some(v) = version {
        debug!("tests write one write with");
        storage.write_with(key, data, meta_with(v)).await
    } else {
        debug!("tests write one write");
        storage.write(key, data).await
    }
}

fn meta_with(version: &str) -> Meta {
    let mut meta = Meta::new();
    meta.insert("version".to_owned(), version);
    meta
}

#[tokio::test]
async fn test_records_count() {
    let now = Instant::now();
    let path = common::init("records_count");
    let storage = common::create_test_storage(&path, 20000).await.unwrap();

    let count = 30;
    let records = common::generate_records(count, 1_000);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(storage.records_count().await, count);
    assert!(storage.records_count_in_active_blob().await < Some(count));

    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_records_count_in_active() {
    let now = Instant::now();
    let path = common::init("records_count_in_active");
    let storage = common::create_test_storage(&path, 20000).await.unwrap();

    let count = 10;
    let records = common::generate_records(count, 1_000);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(storage.records_count_in_active_blob().await, Some(count));

    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_records_count_detailed() {
    let now = Instant::now();
    let path = common::init("records_count_detailed");
    let storage = common::create_test_storage(&path, 20000).await.unwrap();

    let count = 30;
    let records = common::generate_records(count, 1000);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(64)).await;
    }
    sleep(Duration::from_millis(1000)).await;
    let details = storage.records_count_detailed().await;
    assert!(details[0].1 > 18);
    assert_eq!(details.iter().fold(0, |acc, d| acc + d.1), count);

    common::clean(storage, path).await.expect("clean failed");
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_manual_close_active_blob() {
    let path = common::init("manual_close_active_blob");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let records = common::generate_records(5, 1000);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(storage.blobs_count(), 1);
    assert!(path.join("test.0.blob").exists());
    sleep(Duration::from_millis(1000)).await;
    storage.close_active_blob().await;
    assert!(path.join("test.0.blob").exists());
    assert!(!path.join("test.1.blob").exists());
    common::clean(storage, path).await.unwrap();
}

#[tokio::test]
async fn test_blobs_count_random_names() {
    let path = common::init("blobs_count_random_names");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let records = common::generate_records(5, 1000);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    storage.close().await.unwrap();
    assert_eq!(storage.blobs_count(), 1);
    assert!(path.join("test.0.blob").exists());
    let mut rng = rand::thread_rng();
    let mut numbers = [0usize; 16];
    rng.fill(&mut numbers);
    let names: Vec<_> = numbers
        .iter()
        .filter_map(|i| {
            if *i != 0 {
                Some(format!("test.{}.blob", i))
            } else {
                None
            }
        })
        .collect();
    debug!("test blob names: {:?}", names);
    let source = path.join("test.0.blob");
    for name in &names {
        let dst = path.join(name);
        debug!("{:?} -> {:?}", source, dst);
        tokio::fs::copy(&source, dst).await.unwrap();
    }
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    assert_eq!(names.len() + 1, storage.blobs_count());
    common::clean(storage, path).await.unwrap();
}
