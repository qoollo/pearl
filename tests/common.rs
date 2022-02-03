#![allow(unused_attributes)]

use anyhow::Result;
use chrono::Local;
use env_logger::fmt::Color;
use futures::{future, stream::futures_unordered::FuturesUnordered, FutureExt, StreamExt};
use log::Level;
use rand::Rng;
use std::{
    env, fs,
    io::{Seek, SeekFrom, Write},
    path::Path,
    path::PathBuf,
};

use pearl::{Builder, Key, Storage};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyTest(Vec<u8>);

impl AsRef<[u8]> for KeyTest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<KeyTest> for KeyTest {
    fn as_ref(&self) -> &KeyTest {
        self
    }
}

impl Key for KeyTest {
    const LEN: u16 = 4;
}

impl Default for KeyTest {
    fn default() -> Self {
        Self(vec![0; 4])
    }
}

impl From<Vec<u8>> for KeyTest {
    fn from(mut v: Vec<u8>) -> Self {
        v.resize(KeyTest::LEN as usize, 0);
        Self(v)
    }
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
    let builder = if let Ok(ioring) = rio::new() {
        builder.enable_aio(ioring)
    } else {
        println!("current OS doesn't support AIO");
        builder
    };
    let mut storage = builder.build().unwrap();
    storage.init().await.map_err(|e| e.to_string())?;
    Ok(storage)
}

pub fn create_indexes(threads: usize, writes: usize) -> Vec<Vec<usize>> {
    (0..threads)
        .map(|i| (0..writes).map(|j| i * threads + j).collect())
        .collect()
}

pub async fn clean(storage: Storage<KeyTest>, path: impl AsRef<Path>) -> Result<()> {
    std::thread::sleep(std::time::Duration::from_millis(100));
    storage.close().await?;
    fs::remove_dir_all(path).map_err(Into::into)
}

pub async fn close_storage(storage: Storage<KeyTest>) -> Result<()> {
    std::thread::sleep(std::time::Duration::from_millis(100));
    storage.close().await?;
    Ok(())
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

pub enum CorruptionType {
    ZeroedAtBegin(u64),
}

pub fn corrupt_file(path: impl AsRef<Path>, corruption_type: CorruptionType) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(false)
        .write(true)
        .truncate(false)
        .open(path)?;
    let size = file.metadata()?.len();
    match corruption_type {
        CorruptionType::ZeroedAtBegin(zeroed_size) => {
            let write_size = zeroed_size.min(size);
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&vec![0u8; write_size as usize])?;
        }
    }
    file.sync_all()?;
    Ok(())
}

pub fn build_rep_data(data_size: usize, slice: &mut [u8], records_amount: usize) -> Vec<Vec<u8>> {
    let mut res = Vec::new();
    for i in 0..records_amount {
        slice[0] = (i % 256) as u8;
        let mut data = Vec::new();
        for _ in 0..(data_size / slice.len()) {
            data.extend(slice.iter());
        }
        res.push(data);
    }
    res
}

pub fn cmp_records_collections(mut got: Vec<Vec<u8>>, expected: &[Vec<u8>]) -> bool {
    if got.len() != expected.len() {
        false
    } else {
        got.sort();
        got.iter()
            .zip(expected.iter())
            .map(|(a, b)| a == b)
            .filter(|b| *b)
            .count()
            == got.len()
    }
}
