use crate::error::ValidationErrorKind;

use super::prelude::*;

const META_SIZE: usize = 100;
const META_VALUE: u8 = 17;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct KeyType(Vec<u8>);

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct RefKeyType<'a>(&'a [u8]);

impl<'a> From<&'a [u8]> for RefKeyType<'a> {
    fn from(v: &'a [u8]) -> Self {
        Self(v)
    }
}

impl<'a> RefKey<'a> for RefKeyType<'a> {}

impl<'a> Key<'a> for KeyType {
    const LEN: u16 = 8;

    const MEM_SIZE: usize = 8 + std::mem::size_of::<Vec<u8>>();

    type Ref = RefKeyType<'a>;
}

impl<'a> From<&'a [u8]> for KeyType {
    fn from(a: &[u8]) -> Self {
        let data = a.try_into().expect("key size mismatch");
        Self(data)
    }
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
        Self(vec![0_u8; Self::LEN as usize])
    }
}

impl From<usize> for KeyType {
    fn from(i: usize) -> Self {
        let mut v = serialize(&i).unwrap();
        v.resize(KeyType::LEN as usize, 0);
        Self(v)
    }
}

impl Into<usize> for KeyType {
    fn into(self) -> usize {
        deserialize(&self.0).unwrap()
    }
}

fn get_test_dir(dir_name: &str) -> PathBuf {
    let result = std::env::temp_dir()
        .join("pearl_bptree_test")
        .join(std::time::UNIX_EPOCH.elapsed().unwrap().as_secs().to_string())
        .join(dir_name);

    std::fs::create_dir_all(&result).expect("Directory created");

    return result;
}

fn clean(index: BPTreeFileIndex<KeyType>, path: impl AsRef<Path>) {
    std::mem::drop(index);
    std::fs::remove_dir_all(path).expect("Cleaning test dir error");
}

fn create_io_driver() -> IoDriver {
    IoDriver::new()
}

#[tokio::test(flavor = "multi_thread")]
async fn serialize_deserialize_file() {
    let test_dir = get_test_dir("serialize_deserialize_file");
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (0..10000).map(|i| i.into()).for_each(|key: KeyType| {
        let rh = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 1, 1, 1);
        inmem.insert(key, vec![rh]);
    });
    let meta = vec![META_VALUE; META_SIZE];
    let iodriver = create_io_driver();
    let findex = BPTreeFileIndex::<KeyType>::from_records(
        &test_dir.join("bptree_index.b"),
        iodriver,
        &inmem,
        meta,
        true,
        0,
    )
    .await
    .expect("Can't create file index");
    let (inmem_after, _size) = findex
        .get_records_headers(0)
        .await
        .expect("Can't get InMemoryIndex");
    assert_eq!(inmem, inmem_after);

    clean(findex, test_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_size_invalidation() {
    let test_dir = get_test_dir("blob_size_invalidation");
    let filename = test_dir.join("bptree_index.0.index");
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (0..10000).map(|i| i.into()).for_each(|key: KeyType| {
        let rh = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 1, 1, 1);
        inmem.insert(key, vec![rh]);
    });
    let meta = vec![META_VALUE; META_SIZE];
    let iodriver = create_io_driver();
    let findex = BPTreeFileIndex::<KeyType>::from_records(
        &filename,
        iodriver,
        &inmem,
        meta,
        true,
        100,
    )
    .await
    .expect("can't create file index");

    assert!(findex.validate(50).is_err());
    assert!(matches!(
        findex
            .validate(50)
            .unwrap_err()
            .downcast_ref::<Error>()
            .unwrap()
            .kind(),
        ErrorKind::Validation {
            kind: ValidationErrorKind::IndexBlobSize,
            ..
        }
    ));

    clean(findex, test_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn magic_byte_corruption() {
    let test_dir = get_test_dir("magic_byte_corruption");
    let filename = test_dir.join("bptree_index.0.index");
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (0..10000).map(|i| i.into()).for_each(|key: KeyType| {
        let rh = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 1, 1, 1);
        inmem.insert(key, vec![rh]);
    });
    let meta = vec![META_VALUE; META_SIZE];
    let iodriver = create_io_driver();
    let _ = BPTreeFileIndex::<KeyType>::from_records(
        &filename,
        iodriver.clone(),
        &inmem,
        meta,
        true,
        100,
    )
    .await
    .expect("can't create file index");
    // corrupt
    let mut file_content = std::fs::read(&filename).expect("failed to read file");
    for i in 0..8 {
        if i % 4 == 0 {
            file_content[i as usize] = 0;
        }
    }
    std::fs::write(&filename, file_content).expect("failed to write file");

    let findex = BPTreeFileIndex::<KeyType>::from_file(
        FileName::from_path(&filename).expect("failed to create filename"),
        iodriver,
    )
    .await
    .expect("can't read file index");

    assert!(findex.validate(100).is_err());
    assert!(matches!(
        findex
            .validate(100)
            .unwrap_err()
            .downcast_ref::<Error>()
            .unwrap()
            .kind(),
        ErrorKind::Validation {
            kind: ValidationErrorKind::IndexMagicByte,
            ..
        }
    ));

    clean(findex, test_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn check_get_any() {
    const RANGE_FROM: usize = 100;
    const RANGE_TO: usize = 9000;

    let test_dir = get_test_dir("check_get_any");
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (RANGE_FROM..RANGE_TO)
        .map(|i| i.into())
        .for_each(|key: KeyType| {
            let rh = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 1, 1, 1);
            inmem.insert(key, vec![rh]);
        });
    let meta = vec![META_VALUE; META_SIZE];
    let iodriver = create_io_driver();
    let findex = BPTreeFileIndex::<KeyType>::from_records(
        &test_dir.join("any_bptree_index.b"),
        iodriver,
        &inmem,
        meta,
        true,
        0,
    )
    .await
    .expect("Can't create file index");
    let presented_keys = RANGE_FROM..RANGE_TO;
    for key in presented_keys.map(|k| k.into()) {
        if let Ok(inner_res) = findex.get_latest(&key).await {
            if let Some(actual_header) = inner_res {
                let key_deserialized: usize = key.clone().into();
                assert_eq!(
                    inmem[&key][0], actual_header,
                    "Key doesn't exists: {}",
                    key_deserialized
                );
            } else {
                panic!("Key is not found");
            }
        } else {
            panic!("Error in get_any for file index");
        }
    }
    let not_presented_ranges = [0..RANGE_FROM, RANGE_TO..(RANGE_TO + 100)];
    for not_presented_keys in not_presented_ranges.iter() {
        for key in not_presented_keys.clone().map(|k| serialize(&k).unwrap()) {
            assert_eq!(None, findex.get_latest(&key.into()).await.unwrap());
        }
    }

    clean(findex, test_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn preserves_records_order() {
    const RANGE_FROM: usize = 100;
    const RANGE_TO: usize = 9000;

    let test_dir = get_test_dir("preserves_records_order");
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (RANGE_FROM..RANGE_TO)
        .map(|i| i.into())
        .for_each(|key: KeyType| {
            let rh1 = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 1, 1, 1);
            let rh2 = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 2, 2, 2);
            inmem.insert(key, vec![rh1, rh2]);
        });
    let meta = vec![META_VALUE; META_SIZE];
    let iodriver = create_io_driver();
    let findex = BPTreeFileIndex::<KeyType>::from_records(
        &test_dir.join("latest_bptree_index.b"),
        iodriver,
        &inmem,
        meta,
        true,
        0,
    )
    .await
    .expect("Can't create file index");
    let (deser, _) = findex
        .get_records_headers(0)
        .await
        .expect("Can't create index from file");
    for (k, v) in deser.iter() {
        assert!(v.last().is_some(), "Records are missing for key {:?}", k);
        assert_eq!(
            v.last().unwrap().data_size(),
            2,
            "Order of records is wrong"
        );
    }

    clean(findex, test_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn check_get() {
    const MAX_AMOUNT: usize = 3;
    const RANGE_FROM: usize = 100;
    const RANGE_TO: usize = 9000;

    let test_dir = get_test_dir("check_get");
    let mut inmem = InMemoryIndex::<KeyType>::new();
    (RANGE_FROM..RANGE_TO)
        .map(|i| (i % MAX_AMOUNT + 1, i.into()))
        .for_each(|(times, key): (_, KeyType)| {
            let rh = RecordHeader::new(key.to_vec(), BlobRecordTimestamp::now().into(), 1, 1, 1);
            let recs = (0..times).map(|_| rh.clone()).collect();
            inmem.insert(key, recs);
        });
    let meta = vec![META_VALUE; META_SIZE];
    let iodriver = create_io_driver();
    let findex = BPTreeFileIndex::<KeyType>::from_records(
        &test_dir.join("all_bptree_index.b"),
        iodriver,
        &inmem,
        meta,
        true,
        0,
    )
    .await
    .expect("Can't create file index");
    let presented_keys = RANGE_FROM..RANGE_TO;
    for key in presented_keys.map(|k| k.into()) {
        if let Ok(inner_res) = findex.get_latest(&key).await {
            if let Some(actual_header) = inner_res {
                let key_deserialized: usize = key.clone().into();
                assert_eq!(
                    inmem[&key][0], actual_header,
                    "Key doesn't exists: {}",
                    key_deserialized
                );
            } else {
                panic!("Key is not found");
            }
        } else {
            panic!("Error in get_any for file index");
        }
    }
    let not_presented_ranges = [0..RANGE_FROM, RANGE_TO..(RANGE_TO + 100)];
    for not_presented_keys in not_presented_ranges.iter() {
        for key in not_presented_keys.clone().map(|k| k.into()) {
            assert_eq!(None, findex.find_by_key(&key).await.unwrap());
        }
    }

    clean(findex, test_dir);
}
