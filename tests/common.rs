#![feature(
    futures_api,
    async_await,
    await_macro,
    repeat_generic_slice,
    duration_float
)]
use futures::{
    executor::ThreadPool,
    future::{FutureExt, FutureObj, TryFutureExt},
    stream::{futures_unordered, StreamExt},
    task::Spawn,
};
use std::{env, fs};

use pearl::{Builder, Record, Storage, WriteFuture};

pub async fn default_test_storage() -> Result<Storage, String> {
    let path = env::temp_dir().join("pearl_test/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    await!(storage.init());
    Ok(storage)
}

pub fn clean() {
    let path = env::temp_dir().join("pearl_test/");
    fs::remove_dir_all(path).unwrap();
}

pub async fn write(storage: &mut Storage, base_number: usize, mut executor: ThreadPool) -> WriteFuture {
    let key = format!("{}key", base_number);
    let data = "omn".repeat(base_number);
    let mut record = Record::new(key, data);
    await!(storage
        .write(&mut record)).unwrap()
}

pub fn check(
    storage: &mut Storage,
    nums: Vec<usize>,
    mut executor: ThreadPool,
) -> Result<(), String> {
    let keys = nums.iter().map(|n| {format!("{}key", n)}).collect::<Vec<_>>();
    let read_futures = keys
        .iter()
        .map(|key| {
            storage.read(key)
        })
        .collect::<Vec<_>>();
    println!("readed futures: {}", read_futures.len());
    let futures = futures_unordered(read_futures).collect::<Vec<_>>();
    let expected_len = nums.len();
    let future_obj = FutureObj::new(Box::new(
        futures.map(move |records| {
            assert_eq!(records.len(), expected_len);
            records.iter().for_each(|r| println!("{:?}", r))
            }),
    ));
    executor
        .spawn_obj(future_obj)
        .map_err(|e| format!("{:#?}", e))
}
