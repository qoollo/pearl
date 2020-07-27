#![allow(unused_attributes)]

use chrono::Local;
use env_logger::fmt::Color;
use log::Level;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::{env, fs};

use futures::{
    executor::block_on, future::FutureObj, stream::futures_unordered::FuturesUnordered, FutureExt,
    StreamExt,
};
use rand::Rng;

use pearl::{Builder, Key, Storage};

#[derive(Debug, Clone)]
pub struct KeyTest(Vec<u8>);

impl AsRef<[u8]> for KeyTest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Key for KeyTest {
    const LEN: u16 = 4;
}

impl KeyTest {
    pub fn new(inner: u32) -> Self {
        Self(inner.to_be_bytes().to_vec())
    }
}

pub fn init(dir_name: &str) -> PathBuf {
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
                "[{} {:>24}:{:^4} {:^5}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap_or(""),
                record.line().unwrap_or(0),
                style.value(record.level()),
                record.args(),
            )
        })
        .filter_level(log::LevelFilter::Trace)
        .try_init()
        .unwrap_or(());
    env::temp_dir().join(format!(
        "pearl_test/{}/{}",
        std::time::UNIX_EPOCH.elapsed().unwrap().as_secs() % 1_563_100_000,
        dir_name
    ))
}

pub async fn default_test_storage_in(
    dir_name: impl AsRef<Path>,
) -> Result<Storage<KeyTest>, String> {
    create_test_storage(dir_name, 10_000).await
}

pub async fn create_test_storage(
    dir_name: impl AsRef<Path>,
    max_blob_size: u64,
) -> Result<Storage<KeyTest>, String> {
    let path = env::temp_dir().join(dir_name);
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(max_blob_size)
        .max_data_in_blob(100_000)
        .set_filter_config(Default::default())
        .allow_duplicates();
    let ioring = rio::new().expect("create uring");
    let mut storage = builder.build(ioring).unwrap();
    storage.init().await.map_err(|e| format!("{:?}", e))?;
    Ok(storage)
}

pub fn create_indexes(threads: usize, writes: usize) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|i| (0..writes).map(|j| i * threads + j).collect())
        .collect()
}

pub async fn clean(storage: Storage<KeyTest>, path: impl AsRef<Path>) -> Result<(), String> {
    std::thread::sleep(std::time::Duration::from_millis(100));
    storage.close().await.map_err(|e| e.to_string())?;
    fs::remove_dir_all(path).map_err(|e| e.to_string())
}

pub fn check_all_written(storage: &Storage<KeyTest>, keys: Vec<u32>) -> Result<(), String> {
    let read_futures: FuturesUnordered<_> = keys
        .iter()
        .map(|key| storage.read(KeyTest::new(*key)))
        .collect();
    let futures = read_futures.collect::<Vec<_>>();
    let expected_len = keys.len();
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

pub fn generate_records(count: usize, avg_size: usize) -> Vec<(u32, Vec<u8>)> {
    let mut gen = rand::thread_rng();
    (0..count)
        .map(|_i| {
            let diff = gen.gen::<i32>() % (avg_size / 10) as i32;
            let size = avg_size as i32 + diff;
            let mut buf = vec![0; size as usize];
            gen.fill(buf.as_mut_slice());
            (gen.gen(), buf)
        })
        .collect()
}
