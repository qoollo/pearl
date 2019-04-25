#![allow(unused_attributes)]
#![feature(async_await, await_macro)]

use futures::{
    executor::block_on,
    future::{FutureExt, FutureObj},
    stream::{futures_unordered::FuturesUnordered, StreamExt},
    task::SpawnExt,
};
use std::{env, fs};

use pearl::{Builder, Storage};

pub async fn default_test_storage_in<S>(
    spawner: S,
    dir_name: &'static str,
) -> Result<Storage, String>
where
    S: SpawnExt + Clone + Send + 'static + Unpin + Sync,
{
    await!(create_test_storage(spawner, dir_name, 10_000))
}

pub async fn create_test_storage<S>(
    spawner: S,
    dir_name: &'static str,
    max_blob_size: u64,
) -> Result<Storage, String>
where
    S: SpawnExt + Clone + Send + 'static + Unpin + Sync,
{
    let path = env::temp_dir().join(dir_name);
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .max_data_in_blob(1_000)
        .key_size(8);
    let mut storage = builder.build().unwrap();
    await!(storage.init(spawner)).unwrap();
    Ok(storage)
}

pub fn create_indexes(threads: usize, writes: usize) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|i| (0..writes).map(|j| i * threads + j).collect())
        .collect()
}

pub fn clean(storage: Storage, dir: &str) {
    std::thread::sleep(std::time::Duration::from_millis(100));
    storage.close().unwrap();
    let path = env::temp_dir().join(dir);
    fs::remove_dir_all(path).unwrap();
}

pub async fn write(storage: Storage, base_number: u64) {
    let key = base_number.to_be_bytes().to_vec();
    let data = "omn".repeat(base_number as usize % 1_000_000);
    await!(storage.write(key, data.as_bytes().to_vec())).unwrap()
}

pub fn check_all_written(storage: &Storage, nums: Vec<usize>) -> Result<(), String> {
    let keys = nums.iter().map(|n| format!("{}key", n)).collect::<Vec<_>>();
    let read_futures: FuturesUnordered<_> = keys
        .into_iter()
        .map(|key: String| storage.read(key.as_bytes().to_vec()))
        .collect();
    println!("readed futures: {}", read_futures.len());
    let futures = read_futures.collect::<Vec<_>>();
    let expected_len = nums.len();
    let future_obj = FutureObj::new(Box::new(futures.map(move |records| {
        assert_eq!(records.len(), expected_len);
        records
            .iter()
            .filter_map(|res| res.as_ref().err())
            .for_each(|r| println!("{:?}", r))
    })));
    block_on(future_obj);
    Ok(())
}
