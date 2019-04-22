#![feature(
    futures_api,
    async_await,
    await_macro,
    repeat_generic_slice,
    duration_float
)]

use futures::{
    executor::{block_on, ThreadPool},
    future::FutureExt,
    stream::{futures_unordered::FuturesUnordered, StreamExt, TryStreamExt},
};
use pearl::{Builder, Record};
use rand::seq::SliceRandom;
use std::{env, fs};

mod common;

#[test]
fn test_storage_init_new() {
    let dir = "pearl_new/";
    let spawner = ThreadPool::new().unwrap();
    println!("storage init");
    let storage = block_on(common::default_test_storage_in(spawner, dir)).unwrap();
    println!("blobs count");
    assert_eq!(block_on(storage.blobs_count()), 1);
    let path = env::temp_dir().join(dir);
    let blob_file_path = path.join("test.0.blob");
    println!("check path exists");
    assert!(blob_file_path.exists());
    common::clean(dir);
}

#[test]
fn test_storage_init_from_existing() {
    let dir = "pearl_existing/";
    let pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join(dir);
    {
        let builder = Builder::new()
            .work_dir(&path)
            .blob_file_name_prefix("test")
            .max_blob_size(1_000_000)
            .max_data_in_blob(1_000);
        let mut temp_storage = builder.build().unwrap();
        block_on(temp_storage.init(pool.clone())).unwrap();
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

    assert!(block_on(storage.init(pool))
        .map_err(|e| eprintln!("{:?}", e))
        .is_ok());
    assert_eq!(block_on(storage.blobs_count()), 2);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    assert_eq!(
        path.join("test.1.blob"),
        block_on(storage.active_blob_path()).unwrap(),
    );
    common::clean(dir);
}

#[test]
fn test_storage_read_write() {
    let dir = "pearl_read_wirte/";
    let pool = ThreadPool::new().unwrap();
    println!("create default test storage");
    let storage = block_on(common::default_test_storage_in(pool, dir)).unwrap();

    println!("create key/data");
    let key = "test-test".to_owned();
    let data = b"test data string".to_vec();
    println!("new record");
    let mut record = Record::new();
    println!("set record body");
    record.set_body(key.clone(), data.clone());
    println!("init thread pool");
    let mut pool = ThreadPool::new().unwrap();
    println!("block on write");
    block_on(storage.clone().write(record)).unwrap();
    println!("block on read");
    let r = storage.read(key.as_bytes().to_vec());
    println!("run write->read futures");
    let rec = pool.run(r).unwrap();
    println!("check record data len");
    assert_eq!(rec.data().len(), data.len());
    println!("clean test dir");
    common::clean(dir);
    println!("check record key");
    assert_eq!(rec.key(), key.as_bytes());
}

#[test]
fn test_storage_multiple_read_write() {
    let dir = "pearl_multiple/";
    let pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join(dir);
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000)
        .build()
        .unwrap();
    block_on(storage.init(pool)).unwrap();

    let mut keys = Vec::new();
    let mut records: Vec<_> = (
        0..10
        // 00
    )
        .map(|i| {
            let data = b"viva"
            // .repeat(i + 1)
            ;
            let key = format!("key{}", i);
            let mut rec = Record::new();
            rec.set_body(&key, &data);
            keys.push(key.clone());
            rec
        })
        .collect();
    records.shuffle(&mut rand::thread_rng());
    let write_stream: FuturesUnordered<_> = records
        .clone()
        .into_iter()
        .map(|record| storage.clone().write(record))
        .collect();
    let mut pool = ThreadPool::new().unwrap();
    let now = std::time::Instant::now();
    pool.run(write_stream.map_err(|e| dbg!(e)).collect::<Vec<_>>());
    let elapsed = now.elapsed().as_secs_f64();
    let blob_file_path = path.join("test.0.blob");
    let written = fs::metadata(&blob_file_path).unwrap().len();
    println!("write {}B/s", written as f64 / elapsed);
    let read_stream: FuturesUnordered<_> = keys
        .iter()
        .map(|key| storage.read(key.as_bytes().to_vec()))
        .collect();
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
    common::clean(dir);
    assert_eq!(records.len(), records_from_file.len());
    let kv_local: Vec<_> = records.iter().map(|r| (r.key(), r.data())).collect();
    let kv_from_file: Vec<_> = records_from_file
        .iter()
        .map(|r| (r.key(), r.data()))
        .collect();
    assert_eq!(kv_local, kv_from_file);
}

#[test]
fn test_multithread_read_write() -> Result<(), String> {
    use std::thread;

    let dir = "pearl_multithread/";
    println!("create thread pool");
    let pool = ThreadPool::builder()
        .name_prefix("test-pool-")
        .stack_size(4)
        .create()
        .map_err(|e| format!("{:?}", e))?;
    println!("block on create default test storage");
    let storage = block_on(common::default_test_storage_in(pool.clone(), dir))?;
    println!("collect indexes");
    let indexes = common::create_indexes(10, 10);
    println!("spawn std threads");
    let handles = indexes
        .iter()
        .cloned()
        .map(|mut range| {
            let s = storage.clone();
            thread::Builder::new()
                .name(format!("thread#{}", range[0]))
                .spawn(move || {
                    range.shuffle(&mut rand::thread_rng());
                    let write_futures: FuturesUnordered<_> =
                        range.iter().map(|i| common::write(s.clone(), *i)).collect();
                    block_on(write_futures.collect::<Vec<_>>());
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
    common::clean(dir);
    println!("done");
    Ok(())
}

#[test]
fn test_storage_multithread_blob_overflow() -> Result<(), String> {
    use futures::compat::Compat;
    use futures::compat::{Executor01CompatExt, Future01CompatExt};
    use futures::future::TryFutureExt;
    use std::time::{Duration, Instant};
    use tokio::prelude::Future as OldFuture;
    use tokio::runtime::Builder;
    use tokio::timer::Delay;

    let dir = "pearl_overflow/";
    let pool = Builder::new().core_threads(1).build().unwrap();
    let storage = block_on(common::create_test_storage(
        pool.executor().clone().compat(),
        dir,
        10_000,
    ))
    .unwrap();

    let cloned_storage = storage.clone();
    let fut = Compat::new(
        async {
            let mut range: Vec<u64> = (0..100).map(|i| i).collect();
            range.shuffle(&mut rand::thread_rng());
            let mut next_write = Instant::now();
            let data = "omn".repeat(150);
            let delay_futures: Vec<_> = range
                .iter()
                .map(|i| {
                    Delay::new(Instant::now() + Duration::from_millis(i * 100))
                        .compat()
                        .map_err(|e| {
                            println!("{:?}", e);
                        })
                })
                .collect();
            let write_futures: FuturesUnordered<_> = range
                .iter()
                .zip(delay_futures)
                .map(move |(i, df)| {
                    next_write += Duration::from_millis(500);
                    let key = format!("{}key", i);
                    let mut record = Record::new();
                    record.set_body(key, &data);
                    let write_fut = storage.clone().write(record).map_err(|e| {
                        println!("{:?}", e);
                    });
                    df.and_then(move |_| write_fut)
                })
                .collect();
            if await!(write_futures.collect::<Vec<_>>())
                .iter()
                .any(Result::is_err)
            {
                Err(())
            } else {
                Ok(())
            }
        }
            .boxed(),
    );
    pool.block_on_all(fut.map(move |_| {
        cloned_storage.close().unwrap();
    }))
    .unwrap();
    let path = env::temp_dir().join(dir);
    assert!(path.join("test.0.blob").exists());
    assert!(path.join("test.1.blob").exists());
    common::clean(dir);
    Ok(())
}

#[test]
fn test_storage_close() {
    let pool = ThreadPool::new().unwrap();
    let path = env::temp_dir().join("pearl_close/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    assert!(block_on(storage.init(pool))
        .map_err(|e| eprintln!("{:?}", e))
        .is_ok());
    let blob_file_path = path.join("test.0.blob");
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}
