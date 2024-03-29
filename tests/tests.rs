#[macro_use]
extern crate log;

use anyhow::Result;
use bytes::Bytes;
use futures::{
    stream::{futures_unordered::FuturesUnordered, FuturesOrdered, StreamExt, TryStreamExt},
    TryFutureExt,
};
use pearl::{BloomProvider, Builder, Meta, ReadResult, Storage, Key, BlobRecordTimestamp};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use std::{
    fs,
    time::{Duration, Instant},
    sync::Arc,
    collections::HashMap
};
use tokio::time::sleep;


mod common;

use common::{KeyTest, MAX_DEFER_TIME, MIN_DEFER_TIME};

#[tokio::test]
async fn test_storage_init_new() {
    let path = common::init("new");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    // check if active blob created
    assert_eq!(storage.blobs_count().await, 1);
    // check if active blob file exists
    assert!(path.join("test.0.blob").exists());
    common::clean(storage, path).await;
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
    assert_eq!(storage.blobs_count().await, 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    common::clean(storage, path).await;
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
    assert_eq!(new_data, ReadResult::Found(Bytes::copy_from_slice(data)));
    common::clean(storage, path).await;
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
    common::clean(storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_multithread_read_write() -> Result<(), String> {
    let now = Instant::now();
    let path = common::init("multithread");
    let storage = Arc::new(common::default_test_storage_in(&path).await?);
    let threads = 16;
    let writes = 25;
    let indexes = common::create_indexes(threads, writes);
    let data = vec![184u8; 3000];
    let handles: FuturesUnordered<_> = indexes
        .iter()
        .cloned()
        .map(|mut range| {
            let st = storage.clone();
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
    sleep(Duration::from_millis(64)).await;
    assert_eq!(handles.len(), threads);
    let keys = indexes
        .iter()
        .flatten()
        .map(|i| *i as u32)
        .collect::<Vec<_>>();
    debug!("make sure that all keys was written");
    common::check_all_written(&storage, keys).await?;
    let storage = Arc::try_unwrap(storage).expect("this should be the last alive storage");
    common::close_storage(storage, &[&index]).await.unwrap();
    assert!(index.exists());
    fs::remove_dir_all(path).unwrap();
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_multithread_read_write_exist_delete() -> Result<(), String> 
{
    fn map_value_into_read_result(val: Option<&usize>) -> ReadResult<usize> {
        match val {
            Some(v) if *v == usize::MAX => ReadResult::Deleted(BlobRecordTimestamp::new(0)),
            Some(v) => ReadResult::Found(*v),
            None => ReadResult::NotFound
        }
    }
    fn reset_delete_timestamp<T>(val: ReadResult<T>) -> ReadResult<T> {
        if val.is_deleted() {
            return ReadResult::Deleted(BlobRecordTimestamp::new(0));
        } 
        val
    }

    let now = Instant::now();
    let path = common::init("multithread_read_write_exist_delete");
    let storage = Arc::new(
        common::create_custom_test_storage(&path, |builder| {
            builder
                .max_blob_size(3_000_000)
                .set_deferred_index_dump_times(Duration::from_millis(750), Duration::from_millis(1500))
        }).await?);
    const THREADS: usize = 2;
    const KEYS_PER_THREAD: usize = 5000;
    const OPERATIONS_PER_THREAD: usize = 25000;

    let data = vec![184u8; 2000];
    let handles: FuturesUnordered<_> = (0..THREADS)
        .into_iter()
        .map(|thread_id| {
            let st = storage.clone();
            let data = data.clone();
            let task = async move {
                let mut rng = ::rand::rngs::StdRng::from_entropy();
                let mut map = HashMap::new();
                let min_key: u32 = (thread_id * KEYS_PER_THREAD) as u32;
                let max_key: u32 = ((thread_id + 1) * KEYS_PER_THREAD) as u32;

                for _ in 0..OPERATIONS_PER_THREAD {
                    let key: u32 = rng.gen_range(min_key..max_key);
                    match rng.gen::<usize>() % 10 {
                        0 => {
                            st.delete(KeyTest::new(key), BlobRecordTimestamp::now(), false).await.expect("delete success");
                            map.insert(key, usize::MAX);
                        },
                        1 | 2 | 3 => {
                            let data_len = rng.gen::<usize>() % data.len();
                            write_one(&st, key, &data[0..data_len], None).await.expect("write success");
                            map.insert(key, data_len);
                        },
                        4 | 5 | 6 => {
                            let exist_res = st.contains(KeyTest::new(key)).await.expect("exist success");
                            let map_res = map_value_into_read_result(map.get(&key));
                            assert_eq!(map_res.map(|_| ()), reset_delete_timestamp(exist_res.map(|_| ())));
                        },
                        _ => {
                            let read_res = st.read(KeyTest::new(key)).await.expect("read success");
                            let map_res = map_value_into_read_result(map.get(&key));
                            assert_eq!(map_res, reset_delete_timestamp(read_res.map(|v| v.len())));
                        }
                    }
                }
                
                // Check all keys
                for key in min_key..max_key {
                    let read_res = st.read(KeyTest::new(key)).await.expect("read success");
                    let map_res = map_value_into_read_result(map.get(&key));
                    assert_eq!(map_res, reset_delete_timestamp(read_res.map(|v| v.len())));

                    let exist_res = st.contains(KeyTest::new(key)).await.expect("exist success");
                    let map_res = map_value_into_read_result(map.get(&key));
                    assert_eq!(map_res.map(|_| ()), reset_delete_timestamp(exist_res.map(|_| ())));
                }
            };
            tokio::spawn(task)
        })
        .collect();
    let handles = handles.try_collect::<Vec<_>>().await.expect("Threads success");
    assert_eq!(THREADS, handles.len());
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
    let storage = Arc::try_unwrap(storage).expect("this should be the last alive storage");
    common::clean(storage, &path).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_multithread_blob_overflow() {
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
    common::clean(storage, &path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
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
async fn test_on_disk_index() {
    let now = Instant::now();
    let path = common::init("index");
    let data_size = 500;
    let max_blob_size = 1500;
    let num_records_to_write = 5u32;
    let read_key = 3u32;
    let iodriver = pearl::IoDriver::new();
    let mut storage = Builder::new()
        .work_dir(&path)
        .set_io_driver(iodriver)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .max_data_in_blob(1_000)
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
    while storage.blobs_count().await < 2 {
        sleep(Duration::from_millis(200)).await;
    }
    assert!(path.join("test.1.blob").exists());
    info!("read {}", read_key);
    let new_data = storage.read(KeyTest::new(read_key)).await.unwrap();
    assert!(matches!(new_data, ReadResult::Found(_)));
    assert_eq!(new_data.unwrap(), data);
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
    common::clean(storage, path).await;
}

#[test]
fn test_work_dir_lock() {
    use rusty_fork::{fork, rusty_fork_id, rusty_fork_test_name};
    use std::fs::{create_dir_all, read, write};
    use std::panic;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::runtime::Builder;

    const MAX_CHILD_WAIT_COUNT: usize = 30;
    const MAX_PARENT_WAIT_SECONDS: u64 = 120;

    let path = Arc::new(common::init("test_work_dir_lock"));
    let parent_init_file = Path::new(path.as_ref()).join("main_init_ready.special");
    let parent_init_file_c = parent_init_file.clone();
    let child_init_file = Path::new(path.as_ref()).join("child_init_ready.special");
    let child_init_file_c = child_init_file.clone();
    let child_path = path.clone();
    create_dir_all(path.as_ref()).expect("failed to create path to init");
    // We need separate processes because locks do not work within the same process
    fork(
        rusty_fork_test_name!(test_work_dir_lock),
        rusty_fork_id!(),
        |_| {},
        move |c, _| {
            let runtime = Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("failed to create runtime in parent");
            let now = Instant::now();

            let storage_one = common::create_test_storage(path.as_ref(), 1_000_000);
            let res_one = runtime.block_on(storage_one);
            write(parent_init_file, vec![]).expect("failed to create init file");
            assert!(res_one.is_ok());

            let storage = res_one.unwrap();
            let exit = c
                .wait_timeout(Duration::from_secs(MAX_PARENT_WAIT_SECONDS))
                .expect("failed to wait for child");
            if let Some(exit) = exit {
                assert!(exit.success());
            } else {
                c.kill().expect("failed to kill child process");
                assert!(false, "child didn't exit on time")
            }

            assert!(
                read(&child_init_file_c).is_ok(),
                "child didn't spawn, please check test name passed to fork"
            );

            runtime.block_on(
                common::clean(storage, path.as_ref())
            );

            warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
        },
        move || {
            write(child_init_file, vec![]).expect("failed to create init file");
            let mut attempt = 0;
            while !read(&parent_init_file_c).is_ok() {
                assert!(attempt < MAX_CHILD_WAIT_COUNT);
                attempt = attempt + 1;

                std::thread::sleep(Duration::from_millis(200));
            }

            if let Err(_) = panic::catch_unwind(|| {
                let runtime = Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .expect("failed to create runtime in child");
                let storage_two = common::create_test_storage(child_path.as_ref(), 1_000_000);
                let _ = runtime.block_on(storage_two);
            }) {
                return;
            } else {
                unreachable!("Second storage process must panic")
            }
        },
    )
    .expect("failed fork");
}

#[tokio::test]
async fn test_corrupted_index_regeneration() {
    let now = Instant::now();
    let path = common::init("corrupted_index");
    let storage = common::create_test_storage(&path, 70_000).await.unwrap();
    let records = common::generate_records(100, 10_000);
    for (i, data) in &records {
        write_one(&storage, *i, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    storage.close().await.unwrap();
    let index_file_path = path.join("test.0.index");
    assert!(index_file_path.exists());

    common::corrupt_file(index_file_path, common::CorruptionType::ZeroedAtBegin(1024))
        .expect("index corruption failed");

    let new_storage = common::create_test_storage(&path, 1_000_000)
        .await
        .expect("storage should be loaded successfully");

    common::clean(new_storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_corrupted_blob_index_regeneration() {
    use std::mem::size_of;
    use common::CorruptionType;

    let now = Instant::now();
    let path = common::init("corrupted_blob_index");
    let storage = common::create_test_storage(&path, 70_000).await.unwrap();
    let records = common::generate_records(100, 10_000);
    for (i, data) in &records {
        write_one(&storage, *i, data, None).await.unwrap();
    }
    storage.close().await.unwrap();

    let blob_header_size = 2 * size_of::<u64>() + size_of::<u32>();
    let magic_len_size = size_of::<u64>() + size_of::<usize>();
    let corruption = CorruptionType::ZeroedAt((blob_header_size + magic_len_size) as u64, 1);

    let blob_file_path = path.join("test.0.blob");
    let index_file_path = path.join("test.0.index");
    assert!(blob_file_path.exists());
    assert!(index_file_path.exists());

    common::corrupt_file(blob_file_path, corruption).expect("blob corruption failed");
    std::fs::remove_file(index_file_path.clone()).expect("index removal");
    let new_storage = common::create_test_storage(&path, 1_000_000)
        .await
        .expect("storage should be loaded successfully");

    assert_eq!(new_storage.corrupted_blobs_count(), 1);
    let index_file_path = path.join("test.0.index");
    assert!(!index_file_path.exists());

    common::clean(new_storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_index_from_blob() {
    let now = Instant::now();
    let path = common::init("index_from_blob");
    let storage = common::create_test_storage(&path, 70_000).await.unwrap();
    let records = common::generate_records(20, 10_000);
    for (i, data) in &records {
        write_one(&storage, *i, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    storage.close().await.unwrap();
    let index_file_path = path.join("test.0.index");
    fs::remove_file(&index_file_path).unwrap();
    let new_storage = common::create_test_storage(&path, 1_000_000).await.unwrap();
    common::close_storage(new_storage, &[&index_file_path])
        .await
        .unwrap();
    assert!(index_file_path.exists());
    fs::remove_dir_all(path).unwrap();
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
        .write(KeyTest::new(1), vec![1; 8].into(), BlobRecordTimestamp::now())
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
    common::clean(storage, path).await;
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
    assert!(storage.blobs_count().await > 1);

    // let data = b"data_with_meta";
    // write_one(&storage, key, data, Some("1.0")).await.unwrap();

    // assert!(write_one(&storage, key, data, Some("1.0")).await.is_err());

    common::clean(storage, path).await;
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
        storage
            .write_with(&key, value.clone().into(), BlobRecordTimestamp::now(), meta)
            .await
            .unwrap();
    }
    common::clean(storage, path).await;
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
    let data1 = b"some_random_data";
    trace!("data1: {:?}", data1);
    write_one(&storage, key, data1, Some("1.0")).await.unwrap();
    debug!("data1 written");
    let data2 = b"some data with different version";
    trace!("data2: {:?}", data2);
    write_one(&storage, key, data2, Some("2.0")).await.unwrap();
    debug!("data2 written");
    let key = KeyTest::new(key);
    let data_read_with = storage.read_with(&key, &meta_with("1.0")).await.unwrap();
    debug!("read with finished");
    let data_read = storage.read(&key).await.unwrap();
    debug!("read finished");
    // data_read - last record, data_read_with - first record with "1.0" meta
    assert!(matches!(data_read_with, ReadResult::Found(_)));
    assert!(matches!(data_read, ReadResult::Found(_)));
    let data_read_with = data_read_with;
    assert_ne!(data_read_with, data_read);
    assert_eq!(
        data_read_with,
        ReadResult::Found(Bytes::copy_from_slice(data1))
    );
    common::clean(storage, path).await;
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
    common::clean(storage, path).await;
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
    common::clean(storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_check_bloom_filter_single() {
    let now = Instant::now();
    let path = common::init("contains_bloom_filter_single");
    let storage = common::create_test_storage(&path, 10).await.unwrap();
    let data = b"some_random_data";
    let repeat = 4096;
    for i in 0..repeat {
        let pos_key = KeyTest::new(i + repeat);
        let neg_key = KeyTest::new(i + 2 * repeat);
        trace!("key: {}, pos: {:?}, negative: {:?}", i, pos_key, neg_key);
        let key = KeyTest::new(i);
        storage.write(&key, data.to_vec().into(), BlobRecordTimestamp::now()).await.unwrap();
        assert_eq!(storage.check_filters(key).await, Some(true));
        let data = b"other_random_data";
        storage.write(&pos_key, data.to_vec().into(), BlobRecordTimestamp::now()).await.unwrap();
        assert_eq!(storage.check_filters(pos_key).await, Some(true));
        assert_eq!(storage.check_filters(neg_key).await, Some(false));
    }
    common::clean(storage, path).await;
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
        storage.write(&key, data.to_vec().into(), BlobRecordTimestamp::now()).await.unwrap();
        sleep(Duration::from_millis(6)).await;
        trace!("blobs count: {}", storage.blobs_count().await);
    }
    for i in 1..800 {
        assert_eq!(storage.check_filters(KeyTest::new(i)).await, Some(true));
    }
    for i in 800..1600 {
        assert_eq!(storage.check_filters(KeyTest::new(i)).await, Some(false));
    }
    common::clean(storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_check_bloom_filter_multiple_offloaded() {
    let now = Instant::now();
    let path = common::init("check_bloom_filter_multiple_offloaded");
    let mut storage = common::create_test_storage(&path, 20000).await.unwrap();
    let data =
        b"lfolakfsjher_rladncreladlladkfsje_pkdieldpgkeolladkfsjeslladkfsj_slladkfsjorladgedom_dladlladkfsjlad";
    for i in 1..800 {
        let key = KeyTest::new(i);
        storage.write(&key, data.to_vec().into(), BlobRecordTimestamp::now()).await.unwrap();
        sleep(Duration::from_millis(6)).await;
        trace!("blobs count: {}", storage.blobs_count().await);
    }
    storage.offload_buffer(usize::MAX, 100).await;
    for i in 1..800 {
        assert_eq!(storage.check_filters(KeyTest::new(i)).await, Some(true));
    }
    for i in 800..1600 {
        assert_eq!(storage.check_filters(KeyTest::new(i)).await, Some(false));
    }
    common::clean(storage, path).await;
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
            storage.write(&key, data.to_vec().into(), BlobRecordTimestamp::now()).await.unwrap();
            trace!("blobs count: {}", storage.blobs_count().await);
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
        assert_eq!(storage.check_filters(KeyTest::new(i)).await, Some(true));
    }
    info!("check certainly missed keys");
    let mut false_positive_counter = 0usize;
    for i in base..base * 2 {
        trace!("check key: {}", i);
        if storage.check_filters(KeyTest::new(i)).await == Some(true) {
            false_positive_counter += 1;
        }
    }
    let fpr = false_positive_counter as f64 / base as f64;
    info!("false positive rate: {:.6} < 0.001", fpr);
    assert!(fpr < 0.001);
    common::clean(storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_check_bloom_filter_generated() {
    let now = Instant::now();
    let path = common::init("check_bloom_filter_generated");
    debug!("open new storage");
    let base = 20_000;
    {
        let storage = common::create_test_storage(&path, 100_000_000)
            .await
            .unwrap();
        debug!("write some data");
        let data =
            b"lfolakfsjher_rladncreladlladkfsje_pkdieldpgkeolladkfsjeslladkfsj_slladkfsjorladgedom_dladlladkfsjlad";
        for i in 1..base {
            let key = KeyTest::new(i);
            trace!("write key: {}", i);
            storage.write(&key, data.to_vec().into(), BlobRecordTimestamp::now()).await.unwrap();
            trace!("blobs count: {}", storage.blobs_count().await);
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
        assert_eq!(storage.check_filters(KeyTest::new(i)).await, Some(true));
    }
    info!("check certainly missed keys");
    let mut false_positive_counter = 0usize;
    for i in base..base * 2 {
        trace!("check key: {}", i);
        if storage.check_filters(KeyTest::new(i)).await == Some(true) {
            false_positive_counter += 1;
        }
    }
    let fpr = false_positive_counter as f64 / base as f64;
    info!("false positive rate: {:.6} < 0.001", fpr);
    assert!(fpr < 0.001);
    common::clean(storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

async fn write_one(
    storage: &Storage<KeyTest>,
    key: u32,
    data: &[u8],
    version: Option<&str>,
) -> Result<()> {
    let data = Bytes::copy_from_slice(data);
    let key = KeyTest::new(key);
    debug!("tests write one key: {:?}", key);
    if let Some(v) = version {
        debug!("tests write one write with");
        storage.write_with(key, data, BlobRecordTimestamp::now(), meta_with(v)).await
    } else {
        debug!("tests write one write");
        storage.write(key, data, BlobRecordTimestamp::now()).await
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

    common::clean(storage, path).await;
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

    common::clean(storage, path).await;
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

    common::clean(storage, path).await;
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
    assert_eq!(storage.blobs_count().await, 1);
    assert!(path.join("test.0.blob").exists());
    sleep(Duration::from_millis(1000)).await;
    storage.try_close_active_blob().await.unwrap();
    assert!(path.join("test.0.blob").exists());
    assert!(!path.join("test.1.blob").exists());
    common::clean(storage, path).await;
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
    sleep(Duration::from_millis(100)).await;
    assert_eq!(storage.blobs_count().await, 1);
    storage.close().await.unwrap();
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
    assert_eq!(names.len() + 1, storage.blobs_count().await);
    common::clean(storage, path).await;
}

#[tokio::test]
async fn test_memory_index() {
    let path = common::init("memory_index");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let records: Vec<_> = (0..10u8)
        .map(|i| ((i % 4) as u32, vec![i, i + 1, i + 2]))
        .collect();

    for (key, data) in records.iter().take(7) {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    //Record header struct:
    //magic_byte: u64, (8)
    //key: Vec<u8>, (24 (stack) + 4 (heap)) *
    //meta_size: u64, (8)
    //data_size: u64, (8)
    //flags: u8, (1)
    //blob_offset: u64, (8)
    //created: u64, (8)
    //data_checksum: u32, (4)
    //header_checksum: u32, (4)
    //
    // actual packed size: 8 + 24 + 8 + 8 + 1 + 8 + 8 + 4 + 4 = 73 (stack) + 4 (heap; size of u32)
    // actual size: 80 (stack; unpacked) + 4 (heap)
    //
    // *: 24 (stack) + 4 (heap; size of u32) is actual size of key: size_of::<Vec<u8>>() + size_of::<u8>() * key.len(),
    // 12 - seralized size; key.len() in this case == 4 (u32)
    const RECORD_HEADER_SIZE: usize = 84;
    // Key and data size - size of entries in binarymap not including size of entry internal value (RecordHeader)
    // RecordHeader is private, so instead of measurement size_of::Vec<RecordHeader>() there is
    // measurement size_of::<Vec<u8>>(), which has the same size on stack
    const KEY_AND_DATA_SIZE: usize = KeyTest::MEM_SIZE + std::mem::size_of::<Vec<u8>>();
    // I checked vector source code and there is a rule for vector buffer memory allocation (min
    // memory capacity):
    // ```
    // Tiny Vecs are dumb. Skip to:
    // - 8 if the element size is 1, because any heap allocators is likely
    //   to round up a request of less than 8 bytes to at least 8 bytes.
    // - 4 if elements are moderate-sized (<= 1 KiB). (OUR CASE)
    // - 1 otherwise, to avoid wasting too much space for very short Vecs.
    // ```
    // IN OUR CASE: vec![h] gives capacity 1, but on second push capacity grows to 4 (according to
    // rule higher). Then there is the amortized growth.
    assert_eq!(
        storage.index_memory().await,
        KEY_AND_DATA_SIZE * 4 + // 4 keys
        RECORD_HEADER_SIZE * 13 - // 13 allocated
        6 * KeyTest::LEN as usize + // 6 without key on heap
        7  // btree overhead
    );
    assert!(path.join("test.0.blob").exists());
    storage.try_close_active_blob().await.unwrap();
    // Doesn't work without this: indices are written in old btree (which I want to dump in memory)
    sleep(Duration::from_millis(100)).await;
    let file_index_size = storage.index_memory().await;
    for (key, data) in records.iter().skip(7) {
        println!("{} {:?}", key, data);
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        storage.index_memory().await,
        KEY_AND_DATA_SIZE * 3 + // 3 keys
        RECORD_HEADER_SIZE * 3 +  // 3 records in active blob
        file_index_size + // file structures
        5 // btree overhead
    ); // 3 keys, 3 records in active blob (3 allocated)
    assert!(path.join("test.1.blob").exists());
    common::clean(storage, path).await;
}

#[tokio::test]
async fn test_mark_as_deleted_single() {
    let now = Instant::now();
    let path = common::init("mark_as_deleted_single");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let count = 30;
    let records = common::generate_records(count, 1000);
    let delete_key = KeyTest::new(records[0].0);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(64)).await;
    }
    storage.delete(&delete_key, BlobRecordTimestamp::now(), false).await.unwrap();
    assert!(matches!(
        storage.contains(delete_key).await.unwrap(),
        ReadResult::Deleted(_)
    ));
    common::clean(storage, path).await;
    warn!("elapsed: {:.3}", now.elapsed().as_secs_f64());
}

#[tokio::test]
async fn test_mark_as_deleted_deferred_dump() {
    let path = common::init("mark_as_deleted_deferred_dump");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let count = 30;
    let records = common::generate_records(count, 1000);
    let delete_key = KeyTest::new(records[0].0);
    for (key, data) in &records {
        write_one(&storage, *key, data, None).await.unwrap();
        sleep(Duration::from_millis(64)).await;
    }
    let _ = storage.close().await;

    assert!(path.join("test.1.blob").exists());

    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let update_time = std::fs::metadata(&path.join("test.0.index")).expect("metadata");
    storage.delete(&delete_key, BlobRecordTimestamp::now(), false).await.unwrap();

    sleep(MIN_DEFER_TIME / 2).await;
    let new_update_time = std::fs::metadata(&path.join("test.0.index")).expect("metadata");
    assert_eq!(
        update_time.modified().unwrap(),
        new_update_time.modified().unwrap()
    );
    sleep(MAX_DEFER_TIME).await;
    let new_update_time = std::fs::metadata(&path.join("test.0.index")).expect("metadata");
    assert_ne!(
        update_time.modified().unwrap(),
        new_update_time.modified().unwrap()
    );
    common::clean(storage, path).await;
}

#[tokio::test]
async fn test_blob_header_validation() {
    use pearl::error::{AsPearlError, ValidationErrorKind};
    use std::io::{Seek, Write};

    let path = common::init("blob_header_validation");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let data = vec![1, 1, 2, 3, 5, 8];
    write_one(&storage, 42, &data, None).await.unwrap();
    storage.close().await.expect("storage close failed");

    let blob_path = std::env::temp_dir().join(&path).join("test.0.blob");
    info!("path: {}", blob_path.display());
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(false)
            .open(&blob_path)
            .expect("failed to open file");
        let buf = bincode::serialize(&0_u32).expect("failed to serialize u32");
        file.seek(std::io::SeekFrom::Start(8)).expect("seek ok");
        file.write(&buf).expect("failed to overwrite blob version");
        file.flush().expect("flush ok");
    }
    let iodriver = pearl::IoDriver::new();
    let builder = Builder::new()
        .work_dir(&path)
        .set_io_driver(iodriver)
        .blob_file_name_prefix("test")
        .max_blob_size(10_000)
        .max_data_in_blob(100_000)
        .set_filter_config(Default::default())
        .allow_duplicates();
    let mut storage: Storage<KeyTest> = builder.build().unwrap();
    let err = storage
        .init()
        .await
        .expect_err("storage initialized with invalid blob header");
    info!("{:#}", err);
    let pearl_err = err
        .as_pearl_error()
        .expect("result doesn't contains pearl error");
    let is_correct = matches!(
        pearl_err.kind(),
        pearl::ErrorKind::Validation {
            kind: ValidationErrorKind::BlobVersion,
            cause: _
        }
    );
    assert!(is_correct);
    common::clean(storage, path).await;
}


#[tokio::test]
async fn test_empty_blob_detected_as_corrupted() {
    use std::io::Write;

    let path = common::init("empty_blob_detected_as_corrupted");
    let storage = common::create_test_storage(&path, 10_000).await.unwrap();
    let data = vec![1, 1, 2, 3, 5, 8];
    write_one(&storage, 42, &data, None).await.unwrap();
    storage.close().await.expect("storage close failed");

    let blob_path = std::env::temp_dir().join(&path).join("test.0.blob");
    info!("path: {}", blob_path.display());
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(false)
            .open(&blob_path)
            .expect("failed to open file");
        file.set_len(0).expect("set_len success");
        file.flush().expect("flush ok");
    }
    let iodriver = pearl::IoDriver::new();
    let builder = Builder::new()
        .work_dir(&path)
        .set_io_driver(iodriver.clone())
        .blob_file_name_prefix("test")
        .max_blob_size(10_000)
        .max_data_in_blob(100_000)
        .set_filter_config(Default::default())
        .corrupted_dir_name("corrupted")
        .allow_duplicates();
    let mut storage: Storage<KeyTest> = builder.build().unwrap();
    storage
        .init()
        .await
        .expect("storage initialization error");

    write_one(&storage, 43, &data, None).await.unwrap();

    assert_eq!(1, storage.corrupted_blobs_count());
    assert!(path.join("corrupted").exists());
    assert_eq!(2, storage.next_blob_id());

    storage.close().await.expect("storage close failed");

    // One more attempt with lazy storage initialization and blob with length = 1

    let blob_path = std::env::temp_dir().join(&path).join("test.1.blob");
    info!("path: {}", blob_path.display());
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(false)
            .open(&blob_path)
            .expect("failed to open file");
        file.set_len(1).expect("set_len success");
        file.flush().expect("flush ok");
    }

    let builder = Builder::new()
        .work_dir(&path)
        .set_io_driver(iodriver)
        .blob_file_name_prefix("test")
        .max_blob_size(10_000)
        .max_data_in_blob(100_000)
        .set_filter_config(Default::default())
        .corrupted_dir_name("corrupted")
        .allow_duplicates();
    let mut storage: Storage<KeyTest> = builder.build().unwrap();
    storage
        .init_lazy()
        .await
        .expect("storage initialization error");


    assert_eq!(2, storage.corrupted_blobs_count());
    assert!(path.join("corrupted").exists());
    assert_eq!(2, storage.next_blob_id());

    write_one(&storage, 44, &data, None).await.unwrap();

    assert_eq!(3, storage.next_blob_id());

    common::clean(storage, path).await;
}

#[tokio::test]
async fn test_in_memory_and_disk_records_retrieval() -> Result<()> {
    let path = common::init("in_memory_and_disk_records_retrieval");
    let max_blob_size = 1_000_000;
    let records_amount = 30u32;
    let on_disk_key = 0;
    let half_disk_half_memory_key = records_amount / 2 / 2 + 1;
    let in_memory_key = records_amount / 2 - 1;
    let keys = [on_disk_key, half_disk_half_memory_key, in_memory_key];
    let records = common::build_rep_data(500, &mut [17, 40, 29, 7, 75], records_amount as usize);

    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .allow_duplicates()
        // records_amount / 2 + 2 - on disk, records_amount / 2 - 2 - in memory (17, 13)
        .max_data_in_blob(records_amount as u64 / 2 + 2)
        .build()?;

    let res = storage.init().await;
    assert!(res.is_ok());

    info!("write (0..{})", records_amount);
    for i in 0..records_amount {
        sleep(Duration::from_millis(100)).await;
        let res = write_one(&storage, i / 2, &records[i as usize], None).await;
        assert!(res.is_ok());
    }

    while storage.blobs_count().await < 2 {
        sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(
        (storage.blobs_count().await, storage.records_count().await),
        (2, 30)
    );

    for key in keys {
        let record = storage.read(KeyTest::new(key)).await?;
        assert!(matches!(record, ReadResult::Found(_)));
        assert_eq!(record.unwrap(), records[key as usize * 2 + 1]);
    }

    for key in keys {
        let read_records = storage
            .read_all(&KeyTest::new(key))
            .and_then(|entries| {
                entries
                    .into_iter()
                    .map(|e| e.load())
                    .collect::<FuturesOrdered<_>>()
                    .map(|e| e.map(|r| r.into_data().into()))
                    .try_collect::<Vec<_>>()
            })
            .await;
        assert!(common::cmp_records_collections(
            read_records?,
            &records[(key as usize * 2)..(key as usize * 2 + 2)]
        ));
    }

    common::clean(storage, path).await;
    Ok(())
}

#[tokio::test]
async fn test_read_all_with_deletion_marker_delete_middle() -> Result<()> {
    let path = common::init("delete_middle");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let key: KeyTest = vec![0].into();
    let data1: Bytes = "1. test data string".repeat(16).as_bytes().to_vec().into();
    let data2: Bytes = "2. test data string".repeat(16).as_bytes().to_vec().into();
    storage.write(&key, data1.clone(), BlobRecordTimestamp::now()).await?;
    storage.delete(&key, BlobRecordTimestamp::now(), true).await?;
    storage.write(&key, data2.clone(), BlobRecordTimestamp::now()).await?;

    let read = storage.read_all_with_deletion_marker(&key).await?;

    assert_eq!(2, read.len());
    assert!(!read[0].is_deleted());
    assert_eq!(data2, Bytes::from(read[0].load_data().await.unwrap()));
    assert!(read[1].is_deleted());

    std::mem::drop(read); // Entry holds file
    common::clean(storage, path).await;
    Ok(())
}

#[tokio::test]
async fn test_read_all_with_deletion_marker_delete_middle_different_blobs() -> Result<()> {
    let path = common::init("delete_middle_blobs");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let key: KeyTest = vec![0].into();
    let data1: Bytes = "1. test data string".repeat(16).as_bytes().to_vec().into();
    let data2: Bytes = "2. test data string".repeat(16).as_bytes().to_vec().into();
    storage.write(&key, data1.clone(), BlobRecordTimestamp::now()).await?;
    storage.try_close_active_blob().await?;
    storage.delete(&key, BlobRecordTimestamp::now(), false).await?;
    storage.try_close_active_blob().await?;
    storage.write(&key, data2.clone(), BlobRecordTimestamp::now()).await?;
    storage.try_close_active_blob().await?;

    let read = storage.read_all_with_deletion_marker(&key).await?;

    assert_eq!(2, read.len());
    assert!(!read[0].is_deleted());
    assert_eq!(data2, Bytes::from(read[0].load_data().await.unwrap()));
    assert!(read[1].is_deleted());

    std::mem::drop(read); // Entry holds file
    common::clean(storage, path).await;
    Ok(())
}


#[tokio::test]
async fn test_read_ordered_by_timestamp() -> Result<()> {
    let path = common::init("read_ordered_by_timestamp");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let key: KeyTest = vec![0].into();
    let data1: Bytes = "1. test data string".repeat(16).as_bytes().to_vec().into();
    let data2: Bytes = "2. test data string".repeat(16).as_bytes().to_vec().into();
    let data3: Bytes = "3. test data string".repeat(16).as_bytes().to_vec().into();
    storage.write(&key, data1.clone(), BlobRecordTimestamp::new(10)).await?;
    storage.write(&key, data2.clone(), BlobRecordTimestamp::new(10)).await?;
    storage.write(&key, data3.clone(), BlobRecordTimestamp::new(5)).await?;
    storage.delete(&key, BlobRecordTimestamp::new(0), false).await?;

    let read = storage.read_all_with_deletion_marker(&key).await?;

    assert_eq!(4, read.len());
    assert_eq!(BlobRecordTimestamp::new(10), read[0].timestamp());
    assert_eq!(data2, Bytes::from(read[0].load_data().await.unwrap()));
    assert_eq!(BlobRecordTimestamp::new(10), read[1].timestamp());
    assert_eq!(data1, Bytes::from(read[1].load_data().await.unwrap()));
    assert_eq!(BlobRecordTimestamp::new(5), read[2].timestamp());
    assert_eq!(data3, Bytes::from(read[2].load_data().await.unwrap()));
    assert_eq!(BlobRecordTimestamp::new(0), read[3].timestamp());
    assert!(read[3].is_deleted());

    std::mem::drop(read); // Entry holds file

    let data = storage.read(&key).await?;
    assert!(data.is_found());
    assert_eq!(data2, Bytes::from(data.into_option().unwrap()));

    let contains = storage.contains(&key).await?;
    assert!(contains.is_found());
    assert_eq!(BlobRecordTimestamp::new(10), contains.into_option().unwrap());

    common::clean(storage, path).await;
    Ok(())
}

#[tokio::test]
async fn test_read_ordered_by_timestamp_in_different_blobs() -> Result<()> {
    let path = common::init("read_ordered_by_timestamp_in_different_blobs");
    let storage = common::default_test_storage_in(&path).await.unwrap();
    let key: KeyTest = vec![0].into();
    let data1: Bytes = "1. test data string".repeat(16).as_bytes().to_vec().into();
    let data2: Bytes = "2. test data string".repeat(16).as_bytes().to_vec().into();
    let data3: Bytes = "3. test data string".repeat(16).as_bytes().to_vec().into();
    storage.write(&key, data1.clone(), BlobRecordTimestamp::new(10)).await?;
    storage.try_close_active_blob().await?;
    storage.write(&key, data2.clone(), BlobRecordTimestamp::new(10)).await?;
    storage.try_close_active_blob().await?;
    storage.write(&key, data3.clone(), BlobRecordTimestamp::new(5)).await?;
    storage.try_close_active_blob().await?;
    storage.delete(&key, BlobRecordTimestamp::new(0), false).await?;

    let read = storage.read_all_with_deletion_marker(&key).await?;

    assert_eq!(4, read.len());
    assert_eq!(BlobRecordTimestamp::new(10), read[0].timestamp());
    assert_eq!(data2, Bytes::from(read[0].load_data().await.unwrap()));
    assert_eq!(BlobRecordTimestamp::new(10), read[1].timestamp());
    assert_eq!(data1, Bytes::from(read[1].load_data().await.unwrap()));
    assert_eq!(BlobRecordTimestamp::new(5), read[2].timestamp());
    assert_eq!(data3, Bytes::from(read[2].load_data().await.unwrap()));
    assert_eq!(BlobRecordTimestamp::new(0), read[3].timestamp());
    assert!(read[3].is_deleted());

    std::mem::drop(read); // Entry holds file

    let data = storage.read(&key).await?;
    assert!(data.is_found());
    assert_eq!(data2, Bytes::from(data.into_option().unwrap()));

    let contains = storage.contains(&key).await?;
    assert!(contains.is_found());
    assert_eq!(BlobRecordTimestamp::new(10), contains.into_option().unwrap());

    common::clean(storage, path).await;
    Ok(())
}