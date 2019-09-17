#![feature(repeat_generic_slice, async_closure)]

#[macro_use]
extern crate log;

use std::time::{Duration, Instant};
use std::{env, fs, path::PathBuf};

use futures::future::{FutureExt, TryFutureExt};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt, TryStreamExt};
use pearl::{Builder, Meta, Storage};
use rand::seq::SliceRandom;
use tokio::timer::delay;

mod common;

use common::KeyTest;

#[tokio::test]
async fn test_storage_init_new() {
    let dir = common::init("new");
    trace!("storage init");
    let storage = common::default_test_storage_in(&dir).await.unwrap();
    trace!("blobs count");
    assert_eq!(storage.blobs_count(), 1);
    let path = env::temp_dir().join(&dir);
    let blob_file_path = path.join("test.0.blob");
    trace!("check path exists");
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
            let key = KeyTest(records.len().to_be_bytes().to_vec());
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
    trace!("create default test storage");
    let storage = common::default_test_storage_in(&dir).await.unwrap();

    trace!("create key/data");
    let key = "testtest".to_owned();
    let data = b"test data string".to_vec();
    trace!("block on write");
    storage
        .clone()
        .write(KeyTest(key.as_bytes().to_vec()), data.clone())
        .await
        .unwrap();
    trace!("block on read");
    let new_data = storage
        .read(KeyTest(key.as_bytes().to_vec()))
        .await
        .unwrap();
    trace!("check record data len");
    assert_eq!(new_data.len(), data.len());
    trace!("clean test dir");
    common::clean(storage, dir).await.unwrap();
}

async fn init_storage(path: &PathBuf) -> Storage<KeyTest> {
    let mut storage = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000)
        .build()
        .unwrap();
    storage.init().await.unwrap();
    storage
}

fn generate_keys() -> Vec<String> {
    let mut keys: Vec<_> = (0..100)
        .map(|i| {
            let key = format!("key{:5}", i);
            key
        })
        .collect();
    keys.shuffle(&mut rand::thread_rng());
    keys
}

#[tokio::test]
async fn test_storage_multiple_read_write() {
    let dir = common::init("multiple");
    dbg!(&dir);
    let path = env::temp_dir().join(&dir);
    dbg!(&path);
    let storage = init_storage(&path).await;
    error!("storage created");
    let mut keys = generate_keys();
    error!("generated {} keys", keys.len());

    let write_stream: FuturesUnordered<_> = keys
        .iter()
        .map(|key| {
            dbg!(&key);
            storage.write(KeyTest(key.as_bytes().to_vec()), b"qwer".to_vec())
        })
        .collect();
    let now = std::time::Instant::now();
    write_stream.map_err(|e| dbg!(e)).collect::<Vec<_>>().await;
    let elapsed = now.elapsed().as_secs_f64();
    let blob_file_path = path.join("test.0.blob");
    let written = fs::metadata(&blob_file_path).unwrap().len();
    trace!("write {:.0}B/s", written as f64 / elapsed);
    let read_stream: FuturesUnordered<_> = keys
        .iter()
        .map(|key| storage.read(KeyTest(key.as_bytes().to_vec())))
        .collect();
    let now = std::time::Instant::now();
    let data_from_file = read_stream
        .map_err(|e| dbg!(e))
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    let elapsed = now.elapsed().as_secs_f64();
    keys.sort();
    let written = fs::metadata(&blob_file_path).unwrap().len();
    trace!("read {}B/s", written as f64 / elapsed);
    common::clean(storage, dir).await.unwrap();
    assert_eq!(keys.len(), data_from_file.len());
}

#[tokio::test]
async fn test_multithread_read_write() -> Result<(), String> {
    let dir = common::init("multithread");
    error!("block on create default test storage");
    let storage = common::default_test_storage_in(&dir).await?;
    error!("collect indexes");
    let indexes = common::create_indexes(10, 10);
    error!("create mpsc channel");
    let (snd, rcv) = tokio::sync::mpsc::channel(1024);
    error!("spawn std threads");
    let s = storage.clone();
    indexes.iter().cloned().for_each(move |mut range| {
        let s = s.clone();
        let mut snd_cloned = snd.clone();
        std::thread::Builder::new()
            .name(format!("thread#{}", range[0]))
            .spawn(move || {
                let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
                range.shuffle(&mut rand::thread_rng());
                let write_futures: FuturesUnordered<_> = range
                    .iter()
                    .map(|i| common::write(s.clone(), *i as u64))
                    .collect();
                rt.spawn(async move {
                    let res = write_futures.collect::<Vec<_>>().await;
                    snd_cloned.send(res).await.unwrap();
                });
                rt.run().unwrap();
            })
            .unwrap();
    });
    error!("await for all threads to finish");
    let handles = rcv.collect::<Vec<_>>().await;
    let errs_cnt = handles.iter().flatten().filter(|r| r.is_err()).count();
    error!("errors count: {}", errs_cnt);
    error!("generate flat indexes");
    let keys = indexes.iter().flatten().cloned().collect::<Vec<_>>();
    error!("check result");
    common::check_all_written(&storage, keys)?;
    common::clean(storage, dir).await.unwrap();
    error!("done");
    Ok(())
}

#[tokio::test]
async fn test_storage_multithread_blob_overflow() -> Result<(), String> {
    let dir = common::init("overflow");
    let storage = common::create_test_storage(&dir, 10_000).await.unwrap();
    let mut range: Vec<u64> = (0..100).map(|i| i).collect();
    range.shuffle(&mut rand::thread_rng());
    let mut next_write = Instant::now();
    let data = "omn".repeat(150).as_bytes().to_vec();
    let clonned_storage = storage.clone();
    let delay_futures: Vec<_> = range
        .iter()
        .map(|i| delay(Instant::now() + Duration::from_millis(i * 100)))
        .collect();
    let write_futures: FuturesUnordered<_> = range
        .iter()
        .zip(delay_futures)
        .map(move |(i, df)| {
            next_write += Duration::from_millis(500);
            let storage = clonned_storage.clone();
            let data = data.clone();
            df.then(async move |_| {
                let write_fut = storage.write(KeyTest(i.to_be_bytes().to_vec()), data);
                write_fut
                    .map_err(|e| {
                        trace!("{:?}", e);
                    })
                    .await
            })
        })
        .collect();
    let results: Vec<_> = write_futures.collect().await;
    assert!(results.iter().all(|r| r.is_ok()));
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
    debug!("logger initialized");
    let data_size = 500;
    let max_blob_size = 1500;
    let num_records_to_write = 5usize;
    let read_key = 3usize;

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
    debug!("init storage");
    storage.init().await.unwrap();
    debug!("create write unordered futures");
    let write_results: FuturesUnordered<_> = (0..num_records_to_write)
        .map(|key| storage.write(KeyTest(key.to_be_bytes().to_vec()), data.clone()))
        .collect();
    debug!("await for unordered futures");
    write_results
        .for_each(|res| {
            res.unwrap();
            futures::future::ready(())
        })
        .await;
    debug!("wait while blobs_count < 2");
    let mut count = 0;
    while count < 2 {
        count = storage.blobs_count();
        debug!("blobs count: {}", count);
        delay(Instant::now() + Duration::from_millis(200)).await;
    }
    assert!(path.join("test.1.blob").exists());
    warn!("read key: {}", read_key);
    let new_data = storage
        .read(KeyTest(read_key.to_be_bytes().to_vec()))
        .await
        .unwrap();
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
    warn!("create storage");
    let storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    warn!("generate records");
    let records = common::generate_records(10, 10_000);
    warn!("create unordered write futures");
    let futures: FuturesUnordered<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, data)| {
            let key = common::KeyTest(i.to_be_bytes().to_vec());
            storage.write(key, data)
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
    let new_storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    fs::read_dir(&dir_path)
        .unwrap()
        .map(|r| r.unwrap())
        .for_each(|f| info!("{:?}", f));
    assert!(index_file_path.exists());
    common::clean(new_storage, dir)
        .map(|res| res.expect("work dir clean failed"))
        .await;
}

#[tokio::test]
async fn test_write_with() {
    let dir = common::init("write_with");
    info!("init dir");
    let storage = common::create_test_storage(&dir, 1_000_000).await.unwrap();
    info!("storage created");
    let key = KeyTest(b"write_with".to_vec());
    info!("key created");
    let value = b"data_with_empty_meta".to_vec();
    info!("value created");
    storage
        .write_with(key.clone(), value, Meta::new())
        .await
        .unwrap();
    info!("write with finished");
    let value = b"data_with_meta".to_vec();
    info!("data with meta created");
    let mut meta = Meta::new();
    info!("meta created");
    meta.insert("version".to_owned(), "1.1.0");
    info!("inserted new entry");
    storage.write_with(key, value, meta).await.unwrap();
    info!("second write with finished");
}
