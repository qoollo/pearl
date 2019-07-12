#![allow(clippy::needless_lifetimes)]
#![allow(unused_attributes)]
#![feature(async_await, await_macro)]

use chrono::Local;
use env_logger::fmt::Color;
use log::Level;
use std::io::Write;
use std::{convert::TryInto, env, error::Error, fs};

use futures::{
    executor::block_on, future::FutureObj, stream::futures_unordered::FuturesUnordered,
    task::SpawnExt, FutureExt, StreamExt,
};
use rand::Rng;

use pearl::{Builder, Key, Storage};

#[derive(Debug)]
pub struct KeyTest(pub Vec<u8>);

impl AsRef<[u8]> for KeyTest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Key for KeyTest {
    const LEN: u16 = 4;
}

pub fn init_logger() {
    env_logger::builder()
        .format(|buf, record: &log::Record| {
            let mut style = buf.style();
            let color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Cyan,
                Level::Trace => Color::White,
            };
            style.set_color(color);
            writeln!(
                buf,
                "[{} {} {:>30}:{:^4}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                style.value(record.level()),
                record.module_path().unwrap_or(""),
                style.value(record.line().unwrap_or(0)),
                style.value(record.args())
            )
        })
        .filter_level(log::LevelFilter::Trace)
        .try_init()
        .unwrap_or(());
}

pub async fn default_test_storage_in<S>(
    spawner: S,
    dir_name: &str,
) -> Result<Storage<KeyTest>, String>
where
    S: SpawnExt + Clone + Send + 'static + Unpin + Sync,
{
    create_test_storage(spawner, dir_name, 10_000).await
}

pub async fn create_test_storage<S>(
    spawner: S,
    dir_name: &str,
    max_blob_size: u64,
) -> Result<Storage<KeyTest>, String>
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
    storage.init(spawner).await.map_err(|e| {
        dbg!(e.source());
        format!("{:?}", e)
    })?;
    Ok(storage)
}

pub fn create_indexes(threads: usize, writes: usize) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|i| (0..writes).map(|j| i * threads + j).collect())
        .collect()
}

pub async fn clean(storage: Storage<KeyTest>, dir: String) -> Result<(), String> {
    std::thread::sleep(std::time::Duration::from_millis(100));
    storage.close().await.map_err(|e| format!("{:?}", e))?;
    let path = env::temp_dir().join(dir);
    fs::remove_dir_all(path).map_err(|e| format!("{:?}", e))
}

pub async fn write(storage: Storage<KeyTest>, base_number: u64) {
    let key = KeyTest(base_number.to_be_bytes().to_vec());
    let data = "omn".repeat(base_number as usize % 1_000_000);
    storage.write(key, data.as_bytes().to_vec()).await.unwrap()
}

pub fn check_all_written(storage: &Storage<KeyTest>, nums: Vec<usize>) -> Result<(), String> {
    let keys = nums.iter().map(|n| format!("{}key", n)).collect::<Vec<_>>();
    let read_futures: FuturesUnordered<_> = keys
        .into_iter()
        .map(|key: String| storage.read(KeyTest(key.as_bytes().to_vec())))
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

pub fn generate_records(count: usize, avg_size: usize) -> Vec<Vec<u8>> {
    let mut gen = rand::thread_rng();
    (0..count)
        .map(|_i| {
            let diff = gen.gen::<i32>() % (avg_size / 10) as i32;
            let size = avg_size as i32 + diff;
            let mut buf = vec![0; size.try_into().unwrap()];
            gen.fill(buf.as_mut_slice());
            buf
        })
        .collect()
}
