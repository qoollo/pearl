#![allow(unused_attributes)]

use anyhow::Result as AnyResult;
use chrono::Local;
use env_logger::fmt::Color;
use futures::{future, stream::futures_unordered::FuturesUnordered, FutureExt, StreamExt};
use log::Level;
use rand::Rng;
use std::{env, fs, io::Write, path::Path, path::PathBuf};

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

#[cfg(feature = "aio")]
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
    storage.init().await.map_err(|e| e.to_string())?;
    Ok(storage)
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
    let mut storage = builder.build().unwrap();
    storage.init().await.map_err(|e| e.to_string())?;
    Ok(storage)
}

pub fn create_indexes(threads: usize, writes: usize) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|i| (0..writes).map(|j| i * threads + j).collect())
        .collect()
}

pub async fn clean(storage: Storage<KeyTest>, path: impl AsRef<Path>) -> AnyResult<()> {
    std::thread::sleep(std::time::Duration::from_millis(100));
    storage.close().await?;
    fs::remove_dir_all(path).map_err(Into::into)
}

pub async fn check_all_written(storage: &Storage<KeyTest>, keys: Vec<u32>) -> Result<(), String> {
    let mut read_futures: FuturesUnordered<_> = keys
        .iter()
        .map(|key| {
            storage
                .read(KeyTest::new(*key))
                .then(move |res| future::ready((res, *key)))
        })
        .collect();
    let mut ok_count: usize = 0;
    while let Some((res, key)) = read_futures.next().await {
        match res {
            Ok(_) => ok_count += 1,
            Err(e) => println!("[{}] error reading {}", key, e),
        }
    }
    if ok_count == keys.len() {
        Ok(())
    } else {
        Err("Failed to read all keys".to_string())
    }
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
