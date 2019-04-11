#![feature(futures_api, async_await, await_macro)]

use futures::{
    executor::block_on,
    future::{FutureExt, FutureObj},
    stream::{futures_unordered, StreamExt},
    task::SpawnExt,
};
use std::{env, fs};

use pearl::{Builder, Record, Storage};

pub async fn default_test_storage_in<S>(
    spawner: S,
    dir_name: &'static str,
) -> Result<Storage, String>
where
    S: SpawnExt + Clone + Send + 'static + Unpin + Sync,
{
    await!(create_test_storage(spawner, dir_name, 1_000_000))
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
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    await!(storage.init(spawner)).unwrap();
    Ok(storage)
}

pub fn create_indexes(threads: usize, writes: usize) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|i| (0..writes).map(|j| i * threads + j).collect())
        .collect()
}

pub fn clean(dir: &str) {
    let path = env::temp_dir().join(dir);
    fs::remove_dir_all(path).unwrap();
}

pub async fn write(storage: Storage, base_number: usize) {
    let key = format!("{}key", base_number);
    let data = "omn".repeat(base_number);
    let mut record = Record::new();
    record.set_body(key, data);
    await!(storage.write(record)).unwrap()
}

pub fn check_all_written(storage: &Storage, nums: Vec<usize>) -> Result<(), String> {
    let keys = nums.iter().map(|n| format!("{}key", n)).collect::<Vec<_>>();
    let read_futures = keys
        .into_iter()
        .map(|key: String| storage.read(key.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    println!("readed futures: {}", read_futures.len());
    let futures = futures_unordered(read_futures).collect::<Vec<_>>();
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
