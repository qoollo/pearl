use rand::prelude::SliceRandom;

use super::prelude::*;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct KeyType(Vec<u8>);

struct RefKeyType<'a>(&'a [u8]);

impl<'a> RefKey<'a> for RefKeyType<'a> {}

impl<'a> Key<'a> for KeyType {
    const LEN: u16 = 8;

    type Ref = RefKeyType<'a>;
}

impl From<Vec<u8>> for KeyType {
    fn from(mut v: Vec<u8>) -> Self {
        v.resize(KeyType::LEN as usize, 0);
        Self(v)
    }
}

impl AsRef<[u8]> for KeyType {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Default for KeyType {
    fn default() -> Self {
        Self(vec![0_u8, KeyType::LEN as u8])
    }
}

type FileIndexStruct = BPTreeFileIndex<KeyType>;
//type FileIndexStruct = SimpleFileIndex;

fn generate_headers(records_amount: usize, key_mapper: fn(u32) -> u32) -> InMemoryIndex<KeyType> {
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (0..records_amount as u32)
        .map(key_mapper)
        .map(|i| serialize(&i).expect("can't serialize"))
        .for_each(|key| {
            let key: KeyType = key.into();
            let rh = RecordHeader::new(key.to_vec(), 1, 1, 1);
            if let Some(v) = inmem.get_mut(&key) {
                v.push(rh);
            } else {
                inmem.insert(key, vec![rh]);
            }
        });
    inmem
}

fn generate_meta(meta_size: usize) -> Vec<u8> {
    vec![0; meta_size]
}

#[tokio::test]
#[ignore]
async fn benchmark_from_records() {
    const RECORDS_AMOUNT: usize = 1_000_000;
    const META_SIZE: usize = 1_000;
    const TESTS_AMOUNT: usize = 10;
    const FILEPATH: &str = "/tmp/index_ser_bench.b";
    const KEY_MAPPER: fn(u32) -> u32 = |k| k % 100_000;

    let ioring = rio::new();
    if ioring.is_err() {
        println!("Current OS doesn't support AIO");
    }
    let ioring = ioring.ok();
    println!("Generating headers...");
    let headers = generate_headers(RECORDS_AMOUNT, KEY_MAPPER);
    let meta = generate_meta(META_SIZE);

    let time = Instant::now();
    println!("Running serialization benches...");
    for i in 0..TESTS_AMOUNT {
        println!("Test {}...", i + 1);
        let _ = FileIndexStruct::from_records(
            Path::new(FILEPATH),
            ioring.clone(),
            &headers,
            meta.clone(),
            true,
        )
        .await
        .unwrap();
    }
    println!(
        "from_records avg time: {}\n",
        time.elapsed().as_nanos() / TESTS_AMOUNT as u128
    );
}

#[tokio::test]
#[ignore]
async fn benchmark_from_file() {
    const RECORDS_AMOUNT: usize = 1_000_000;
    const META_SIZE: usize = 1_000;
    const TESTS_AMOUNT: usize = 1_000;
    const KEY_MAPPER: fn(u32) -> u32 = |k| k % 100_000;
    const DIR: &str = "/tmp";
    const PREFIX: &str = "index_from_file_bench";
    const ID: usize = 0;
    const EXTENSION: &str = "b";
    let filepath = format!("{}/{}.{}.{}", DIR, PREFIX, ID, EXTENSION);

    let ioring = rio::new();
    if ioring.is_err() {
        println!("Current OS doesn't support AIO");
    }
    let ioring = ioring.ok();
    println!("Generating headers...");
    let headers = generate_headers(RECORDS_AMOUNT, KEY_MAPPER);
    let meta = generate_meta(META_SIZE);

    println!("Creating index file...");
    {
        let _ = FileIndexStruct::from_records(
            Path::new(&filepath),
            ioring.clone(),
            &headers,
            meta,
            true,
        )
        .await
        .unwrap();
    }

    let time = Instant::now();
    for _ in 0..TESTS_AMOUNT {
        let _ = FileIndexStruct::from_file(
            FileName::new(
                PREFIX.to_owned(),
                ID,
                EXTENSION.to_owned(),
                PathBuf::from(DIR),
            ),
            ioring.clone(),
        )
        .await
        .unwrap();
    }
    println!(
        "from_file avg time: {}\n",
        time.elapsed().as_nanos() / TESTS_AMOUNT as u128
    );
}

#[tokio::test]
#[ignore]
async fn benchmark_get_any() {
    const RECORDS_AMOUNT: usize = 10_000_000;
    const META_SIZE: usize = 1_000;
    const FILEPATH: &str = "/tmp/index_get_any_bench.b";
    const KEY_MAPPER: fn(u32) -> u32 = |k| k;
    const KEY_FROM: u32 = 9_900_000;
    const KEY_TO: u32 = 10_100_000;
    const PRINT_EVERY: u32 = 10_000;

    let ioring = rio::new();
    if ioring.is_err() {
        println!("Current OS doesn't support AIO");
    }
    let ioring = ioring.ok();
    println!("Generating headers...");
    let headers = generate_headers(RECORDS_AMOUNT, KEY_MAPPER);
    let meta = generate_meta(META_SIZE);

    println!("Creating file index from headers...");
    let findex = FileIndexStruct::from_records(
        Path::new(FILEPATH),
        ioring.clone(),
        &headers,
        meta.clone(),
        true,
    )
    .await
    .unwrap();
    drop(headers);

    println!("Creating queries...");
    let mut queries: Vec<_> = (KEY_FROM..KEY_TO).collect();
    queries.shuffle(&mut rand::thread_rng());
    println!("Running get_any benches...");
    let time = Instant::now();
    for (i, q) in queries
        .iter()
        .map(|i| serialize(&i).expect("can't serialize"))
        .enumerate()
    {
        if (i as u32 + 1) % PRINT_EVERY == 0 {
            println!("Iteration: {}...", i + 1);
        }
        let _ = findex.get_any(&q.into()).await.unwrap();
    }
    println!(
        "get_any avg time: {}\n",
        time.elapsed().as_nanos() / (KEY_TO - KEY_FROM) as u128
    );
}

#[tokio::test]
#[ignore]
async fn benchmark_get_all() {
    const RECORDS_AMOUNT: usize = 10_000_000;
    const META_SIZE: usize = 1_000;
    const FILEPATH: &str = "/tmp/index_get_all_bench.b";
    const KEY_MAPPER: fn(u32) -> u32 = |k| k;
    const KEY_FROM: u32 = 9_900_000;
    const KEY_TO: u32 = 10_100_000;
    const PRINT_EVERY: u32 = 10_000;

    let ioring = rio::new();
    if ioring.is_err() {
        println!("Current OS doesn't support AIO");
    }
    let ioring = ioring.ok();
    println!("Generating headers...");
    let headers = generate_headers(RECORDS_AMOUNT, KEY_MAPPER);
    let meta = generate_meta(META_SIZE);

    println!("Creating file index from headers...");
    let findex = FileIndexStruct::from_records(
        Path::new(FILEPATH),
        ioring.clone(),
        &headers,
        meta.clone(),
        true,
    )
    .await
    .unwrap();
    drop(headers);

    println!("Creating queries...");
    let mut queries: Vec<_> = (KEY_FROM..KEY_TO).collect();
    queries.shuffle(&mut rand::thread_rng());
    println!("Running get_all benches...");
    let time = Instant::now();
    for (i, q) in queries
        .iter()
        .map(|i| serialize(&i).expect("can't serialize"))
        .enumerate()
    {
        if (i as u32 + 1) % PRINT_EVERY == 0 {
            println!("Iteration: {}...", i + 1);
        }
        let _ = findex.find_by_key(&q.into()).await.unwrap();
    }
    println!(
        "get_all avg time: {}\n",
        time.elapsed().as_nanos() / (KEY_TO - KEY_FROM) as u128
    );
}
