#![feature(async_await, await_macro, repeat_generic_slice, duration_float)]

#[macro_use]
extern crate log;

use futures::{
    executor::{block_on, ThreadPool},
    future::{FutureExt, TryFutureExt},
    stream::{futures_unordered::FuturesUnordered, StreamExt, TryStreamExt},
    task::SpawnExt,
};
use pearl::{Builder, Storage};
use rand::seq::SliceRandom;
use std::{env, fs};

mod common;

use common::KeyTest;

#[test]
fn test_storage_init_new() {
    common::init_logger();
    let dir = "pearl_new/";
    let mut pool = ThreadPool::new().unwrap();
    println!("storage init");
    let storage = pool
        .run(common::default_test_storage_in(pool.clone(), dir))
        .unwrap();
    println!("blobs count");
    assert_eq!(storage.blobs_count(), 1);
    let path = env::temp_dir().join(dir);
    let blob_file_path = path.join("test.0.blob");
    println!("check path exists");
    assert!(blob_file_path.exists());
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_storage_init_from_existing() {
    common::init_logger();
    let dir = "pearl_existing/";
    let mut pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join(dir);
    let mut cloned_pool = pool.clone();
    {
        let builder = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(1_000_000)
            .max_data_in_blob(1_000);
        let mut temp_storage: Storage<KeyTest> = builder.build().unwrap();
        cloned_pool
            .run(temp_storage.init(cloned_pool.clone()))
            .unwrap();
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.join("test.1.blob"))
            .unwrap();
        cloned_pool.run(temp_storage.close()).unwrap();
    }
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
        .map_err(|e| eprintln!("{:?}", e))
        .is_ok());
    assert_eq!(storage.blobs_count(), 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_storage_read_write() {
    common::init_logger();
    let dir = "pearl_read_wirte/";
    let mut pool = ThreadPool::new().unwrap();
    println!("create default test storage");
    let storage = pool
        .run(common::default_test_storage_in(pool.clone(), dir))
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
    common::init_logger();
    let dir = "pearl_multiple/";
    let mut pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join(dir);
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

    common::init_logger();
    let dir = "pearl_multithread/";
    println!("create thread pool");
    let mut pool = ThreadPool::builder()
        .name_prefix("test-pool-")
        .stack_size(4)
        .create()
        .map_err(|e| format!("{:?}", e))?;
    println!("block on create default test storage");
    let storage = pool.run(common::default_test_storage_in(pool.clone(), dir))?;
    println!("collect indexes");
    let indexes = common::create_indexes(10, 10);
    println!("spawn std threads");
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
    println!("threads count: {}", handles.len());
    let errs_cnt = handles
        .into_iter()
        .map(std::thread::JoinHandle::join)
        .filter(Result::is_err)
        .count();
    println!("errors count: {}", errs_cnt);
    println!("generate flat indexes");
    let keys = indexes.iter().flatten().cloned().collect::<Vec<_>>();
    println!("check result");
    common::check_all_written(&storage, keys)?;
    pool.run(common::clean(storage, dir)).unwrap();
    println!("done");
    Ok(())
}

#[test]
fn test_storage_multithread_blob_overflow() -> Result<(), String> {
    use futures::executor::ThreadPool;
    use futures_timer::Delay;
    use std::time::{Duration, Instant};

    common::init_logger();

    let dir = "pearl_overflow/";
    let mut pool = ThreadPool::new().map_err(|e| format!("{}", e))?;
    let storage = block_on(common::create_test_storage(pool.clone(), dir, 10_000)).unwrap();

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
    let path = env::temp_dir().join(dir);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    let dir_owned = dir.to_owned();
    let task = async move { common::clean(storage, &dir_owned).await };
    pool.run(task).unwrap();
    Ok(())
}

#[test]
fn test_storage_close() {
    common::init_logger();
    let mut pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join("pearl_close/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage: Storage<KeyTest> = builder.build().unwrap();
    assert!(pool
        .run(storage.init(pool.clone()))
        .map_err(|e| eprintln!("{:?}", e))
        .is_ok());
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_on_disk_index() {
    common::init_logger();
    warn!("logger initialized");
    let data_size = 500;
    let max_blob_size = 1500;
    let num_records_to_write = 5usize;
    let read_key = 3usize;

    let mut pool = ThreadPool::new().unwrap();
    warn!("pool created");
    let dir = "pearl_index";
    let path = env::temp_dir().join(dir);
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .max_data_in_blob(1_000)
        .build()
        .unwrap();
    warn!("storage built");
    let slice = [17, 40, 29, 7, 75];
    let data: Vec<u8> = slice.repeat(data_size / slice.len());
    let cloned_pool = pool.clone();
    let prepare_task = async {
        storage.init(cloned_pool).await.unwrap();
        let write_results: FuturesUnordered<_> = (0..num_records_to_write)
            .map(|key| {
                storage
                    .clone()
                    .write(KeyTest(key.to_be_bytes().to_vec()), data.clone())
            })
            .collect();
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
    pool.run(prepare_task);
    let read_task = async {
        assert!(path.join("test.1.blob").exists());
        storage
            .read(KeyTest(read_key.to_be_bytes().to_vec()))
            .await
            .unwrap()
    };

    let new_data = pool.run(read_task);

    assert_eq!(new_data, data);
    pool.run(common::clean(storage, dir)).unwrap();
}

#[test]
fn test_work_dir_lock() {
    common::init_logger();
    let mut pool = ThreadPool::new().unwrap();
    pool.run(test_work_dir_lock_async(pool.clone()));
}

async fn test_work_dir_lock_async(mut pool: ThreadPool) {
    let dir = "pearl_work_dir";
    let storage_one = common::create_test_storage(pool.clone(), dir, 1_000_000);
    let res_one = storage_one.await;
    assert!(res_one.is_ok());
    let storage = res_one.unwrap();
    let storage_two = common::create_test_storage(pool.clone(), dir, 1_000_000);
    let res_two = storage_two.await;
    dbg!(&res_two);
    assert!(res_two.is_err());
    pool.spawn(common::clean(storage, dir).map(|res| res.expect("work dir clean failed")))
        .unwrap();
}
