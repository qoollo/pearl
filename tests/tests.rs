#![feature(async_await, repeat_generic_slice, duration_float)]

#[macro_use]
extern crate log;

use std::{env, fs, path::PathBuf, time::Duration};

use futures::{
    executor::{block_on, ThreadPool},
    future::{FutureExt, TryFutureExt},
    stream::{futures_unordered::FuturesUnordered, StreamExt, TryStreamExt},
};
use futures_timer::Delay;
use pearl::{Builder, Storage};
use rand::seq::SliceRandom;

mod common;

use common::KeyTest;

#[test]
fn test_storage_init_new() {
    let dir = common::init("new");
    let mut pool = ThreadPool::new().unwrap();
    println!("storage init");
    let storage = pool
        .run(common::default_test_storage_in(pool.clone(), &dir))
        .unwrap();
    println!("blobs count");
    assert_eq!(storage.blobs_count(), 1);
    let path = env::temp_dir().join(&dir);
    let blob_file_path = path.join("test.0.blob");
    println!("check path exists");
    assert!(blob_file_path.exists());
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_storage_init_from_existing() {
    let dir = common::init("existing");
    let mut pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join(&dir);
    let cloned_pool = pool.clone();
    let task = async {
        let builder = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(1_000_000)
            .max_data_in_blob(1_000);
        let mut temp_storage: Storage<KeyTest> = builder.build()?;
        temp_storage.init(cloned_pool.clone()).await?;
        let mut records = common::generate_records(15, 100_000);
        while !records.is_empty() {
            let key = KeyTest(records.len().to_be_bytes().to_vec());
            let value = records.pop().unwrap();
            let delay: Delay = futures_timer::Delay::new(Duration::from_millis(16));
            delay
                .then(|_| temp_storage.clone().write(key, value))
                .await?;
        }
        temp_storage.close().await
    };
    pool.run(task).unwrap();
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());

    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();

    assert!(pool
        .run(storage.init(pool.clone()))
        .map_err(|e| error!("{:?}", e))
        .is_ok());
    assert_eq!(storage.blobs_count(), 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_storage_read_write() {
    let dir = common::init("read_write");
    let mut pool = ThreadPool::new().unwrap();
    println!("create default test storage");
    let storage = pool
        .run(common::default_test_storage_in(pool.clone(), &dir))
        .unwrap();

    println!("create key/data");
    let key = "testtest".to_owned();
    let data = b"test data string".to_vec();
    println!("init thread pool");
    let mut pool = ThreadPool::new().unwrap();
    println!("block on write");
    pool.run(
        storage
            .clone()
            .write(KeyTest(key.as_bytes().to_vec()), data.clone()),
    )
    .unwrap();
    println!("block on read");
    let r = storage.read(KeyTest(key.as_bytes().to_vec()));
    println!("run write->read futures");
    let new_data = pool.run(r).unwrap();
    println!("check record data len");
    assert_eq!(new_data.len(), data.len());
    println!("clean test dir");
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_storage_multiple_read_write() {
    let dir = common::init("multiple");
    let mut pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join(&dir);
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000)
        .build()
        .unwrap();
    pool.run(storage.init(pool.clone())).unwrap();

    let mut keys = Vec::new();
    let mut records: Vec<_> = (0..100)
        .map(|i| {
            let data = b"viva";
            let key = format!("key{:5}", i);
            keys.push(key.clone());
            (key, data)
        })
        .collect();
    records.shuffle(&mut rand::thread_rng());
    let write_stream: FuturesUnordered<_> = records
        .clone()
        .into_iter()
        .map(|(key, data)| {
            storage
                .clone()
                .write(KeyTest(key.as_bytes().to_vec()), data.to_vec())
        })
        .collect();
    let mut pool = ThreadPool::new().unwrap();
    let now = std::time::Instant::now();
    pool.run(write_stream.map_err(|e| dbg!(e)).collect::<Vec<_>>());
    let elapsed = now.elapsed().as_secs_f64();
    let blob_file_path = path.join("test.0.blob");
    let written = fs::metadata(&blob_file_path).unwrap().len();
    println!("write {:.0}B/s", written as f64 / elapsed);
    let read_stream: FuturesUnordered<_> = keys
        .iter()
        .map(|key| storage.read(KeyTest(key.as_bytes().to_vec())))
        .collect();
    let now = std::time::Instant::now();
    let data_from_file = pool.run(
        read_stream
            .map_err(|e| dbg!(e))
            .map(Result::unwrap)
            .collect::<Vec<_>>(),
    );
    let elapsed = now.elapsed().as_secs_f64();
    records.sort_by_key(|(key, _)| key.clone());
    let written = fs::metadata(&blob_file_path).unwrap().len();
    println!("read {}B/s", written as f64 / elapsed);
    pool.run(common::clean(storage, dir)).unwrap();
    assert_eq!(records.len(), data_from_file.len());
}

#[test]
fn test_multithread_read_write() -> Result<(), String> {
    use std::thread;

    let dir = common::init("multithread");
    info!("create thread pool");
    let mut pool = ThreadPool::builder()
        .name_prefix("test-pool-")
        .create()
        .map_err(|e| format!("{:?}", e))?;
    info!("block on create default test storage");
    let storage = pool.run(common::default_test_storage_in(pool.clone(), &dir))?;
    info!("collect indexes");
    let indexes = common::create_indexes(10, 10);
    info!("spawn std threads");
    let handles = indexes
        .iter()
        .cloned()
        .map(|mut range| {
            let mut cloned_pool = pool.clone();
            let s = storage.clone();
            thread::Builder::new()
                .name(format!("thread#{}", range[0]))
                .spawn(move || {
                    range.shuffle(&mut rand::thread_rng());
                    let write_futures: FuturesUnordered<_> = range
                        .iter()
                        .map(|i| common::write(s.clone(), *i as u64))
                        .collect();
                    cloned_pool.run(write_futures.collect::<Vec<_>>());
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    info!("threads count: {}", handles.len());
    let errs_cnt = handles
        .into_iter()
        .map(std::thread::JoinHandle::join)
        .filter(Result::is_err)
        .count();
    info!("errors count: {}", errs_cnt);
    info!("generate flat indexes");
    let keys = indexes.iter().flatten().cloned().collect::<Vec<_>>();
    info!("check result");
    common::check_all_written(&storage, keys)?;
    pool.run(common::clean(storage, dir)).unwrap();
    info!("done");
    Ok(())
}

#[test]
fn test_storage_multithread_blob_overflow() -> Result<(), String> {
    use futures::executor::ThreadPool;
    use futures_timer::Delay;
    use std::time::{Duration, Instant};

    let dir = common::init("overflow");
    let mut pool = ThreadPool::new().map_err(|e| format!("{}", e))?;
    let storage = block_on(common::create_test_storage(pool.clone(), &dir, 10_000)).unwrap();

    let cloned_storage = storage.clone();
    let fut = async {
        let mut range: Vec<u64> = (0..100).map(|i| i).collect();
        range.shuffle(&mut rand::thread_rng());
        let mut next_write = Instant::now();
        let data = "omn".repeat(150).as_bytes().to_vec();
        let delay_futures: Vec<_> = range
            .iter()
            .map(|i| {
                Delay::new(Duration::from_millis(i * 100))
                    .map_ok(move |_| println!("{}", i))
                    .map_err(|e| {
                        println!("{}", e);
                    })
            })
            .collect();
        let write_futures: FuturesUnordered<_> = range
            .iter()
            .zip(delay_futures)
            .map(move |(i, df)| {
                next_write += Duration::from_millis(500);
                let write_fut = cloned_storage
                    .clone()
                    .write(KeyTest(i.to_be_bytes().to_vec()), data.clone())
                    .map_err(|e| {
                        println!("{:?}", e);
                    });
                df.and_then(move |_| write_fut)
            })
            .collect();
        if write_futures
            .collect::<Vec<_>>()
            .await
            .iter()
            .any(Result::is_err)
        {
            Err(())
        } else {
            Ok(())
        }
    };
    futures::executor::block_on(fut.map(|res| res.unwrap()));
    let path = env::temp_dir().join(&dir);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    let dir_owned = dir.to_owned();
    let task = async move { common::clean(storage, dir_owned).await };
    pool.run(task).unwrap();
    Ok(())
}

#[test]
fn test_storage_close() {
    let dir = common::init("pearl_close");
    let mut pool = ThreadPool::new().unwrap();
    let builder = Builder::new()
        .work_dir(&dir)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage: Storage<KeyTest> = builder.build().unwrap();
    assert!(pool
        .run(storage.init(pool.clone()))
        .map_err(|e| eprintln!("{:?}", e))
        .is_ok());
    let path: PathBuf = dir.into();
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_on_disk_index() {
    let dir = common::init("index");
    warn!("logger initialized");
    let data_size = 500;
    let max_blob_size = 1500;
    let num_records_to_write = 5usize;
    let read_key = 3usize;

    let mut pool = ThreadPool::new().unwrap();
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
    let cloned_pool = pool.clone();
    warn!("create write task");
    let prepare_task = async {
        warn!("init storage");
        storage.init(cloned_pool).await.unwrap();
        warn!("create write unordered futures");
        let write_results: FuturesUnordered<_> = (0..num_records_to_write)
            .map(|key| {
                storage
                    .clone()
                    .write(KeyTest(key.to_be_bytes().to_vec()), data.clone())
            })
            .collect();
        warn!("await for unordered futures");
        write_results
            .for_each(|res| {
                res.unwrap();
                futures::future::ready(())
            })
            .await;
        let mut count = 0;
        while count < 2 {
            count = storage.blobs_count();
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    };
    warn!("run write task");
    pool.run(prepare_task);
    warn!("prepare read task");
    let read_task = async {
        assert!(path.join("test.1.blob").exists());
        warn!("read key: {}", read_key);
        storage
            .read(KeyTest(read_key.to_be_bytes().to_vec()))
            .await
            .unwrap()
    };

    warn!("run read task");
    let new_data = pool.run(read_task);

    assert_eq!(new_data, data);
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_work_dir_lock() {
    let dir = common::init("work_dir_lock");
    let mut pool = ThreadPool::new().unwrap();
    pool.run(test_work_dir_lock_async(pool.clone(), dir));
}

async fn test_work_dir_lock_async(pool: ThreadPool, dir: String) {
    let storage_one = common::create_test_storage(pool.clone(), &dir, 1_000_000);
    let res_one = storage_one.await;
    assert!(res_one.is_ok());
    let storage = res_one.unwrap();
    let storage_two = common::create_test_storage(pool.clone(), &dir, 1_000_000);
    let res_two = storage_two.await;
    dbg!(&res_two);
    assert!(res_two.is_err());
    common::clean(storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}

#[test]
fn test_index_from_blob() {
    let dir = common::init("index_from_blob");
    let mut pool = ThreadPool::new().unwrap();
    pool.run(test_index_from_blob_async(pool.clone(), dir));
}

async fn test_index_from_blob_async(pool: ThreadPool, dir: String) {
    warn!("create storage");
    let storage = common::create_test_storage(pool.clone(), &dir, 1_000_000)
        .await
        .unwrap();
    warn!("generate records");
    let records = common::generate_records(10, 10_000);
    warn!("create unordered write futures");
    let futures: FuturesUnordered<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, data)| {
            let key = common::KeyTest(i.to_be_bytes().to_vec());
            storage.clone().write(key, data)
        })
        .collect();
    warn!("collect futures");
    futures.map(Result::unwrap).collect::<Vec<_>>().await;
    warn!("close storage");
    storage.close().await.unwrap();
    let dir_path: PathBuf = env::temp_dir().join(&dir);
    info!("ls work dir");
    fs::read_dir(&dir_path)
        .unwrap()
        .map(|r| r.unwrap())
        .for_each(|f| info!("{:?}", f));
    let index_file_path = dir_path.join("test.0.index");
    warn!("delete index file: {}", index_file_path.display());
    fs::remove_file(&index_file_path).unwrap();
    info!("ls work dir, index file deleted");
    fs::read_dir(&dir_path)
        .unwrap()
        .map(|r| r.unwrap())
        .for_each(|f| info!("{:?}", f));
    warn!("create new test storage");
    let new_storage = common::create_test_storage(pool.clone(), &dir, 1_000_000)
        .await
        .unwrap();
    fs::read_dir(&dir_path)
        .unwrap()
        .map(|r| r.unwrap())
        .for_each(|f| info!("{:?}", f));
    assert!(index_file_path.exists());
    common::clean(new_storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}
