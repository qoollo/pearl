#![feature(futures_api, async_await, await_macro)]

use futures::{
    executor::ThreadPool,
    future::{FutureExt, FutureObj},
    stream::{futures_unordered, StreamExt},
};
use std::{env, fs, pin::Pin};

use pearl::{Builder, Record, Storage};

pub async fn default_test_storage_in(dir_name: &'static str) -> Result<Storage, String> {
    let path = env::temp_dir().join(dir_name);
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000)
        .max_data_in_blob(1_000);
    let mut storage = builder.build().unwrap();
    await!(storage.init()).unwrap();
    Ok(storage)
}

pub fn clean(dir: &str) {
    let path = env::temp_dir().join(dir);
    fs::remove_dir_all(path).unwrap();
}

pub async fn write<'a>(storage: &'a Pin<&'a mut Storage>, base_number: usize) {
    let key = format!("{}key", base_number);
    let data = "omn".repeat(base_number);
    let mut record = Record::new();
    record.set_body(key, data);
    await!(storage.write(record)).unwrap()
}

pub fn check(storage: &Storage, nums: Vec<usize>, mut executor: ThreadPool) -> Result<(), String> {
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
    executor.run(future_obj);
    Ok(())
}
