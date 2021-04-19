use super::prelude::*;

const META_SIZE: usize = 100;
const META_VALUE: u8 = 17;

#[tokio::test]
async fn serialize_deserialize_file() {
    let mut inmem = InMemoryIndex::new();
    (0..10000u32)
        .map(|i| serialize(&i).expect("can't serialize"))
        .for_each(|key| {
            let rh = RecordHeader::new(key.clone(), 1, 1, 1);
            inmem.insert(key, vec![rh]);
        });
    let meta = vec![META_VALUE; META_SIZE];
    let findex = BPTreeFileIndexStruct::<Vec<u8>, RecordHeader>::from_records(
        &Path::new("/tmp/bptree_index.b"),
        None,
        &inmem,
        meta,
        true,
    )
    .await
    .expect("Can't create file index");
    let (inmem_after, _size) = findex
        .get_records_headers()
        .await
        .expect("Can't get InMemoryIndex");
    assert_eq!(inmem, inmem_after);
}

#[tokio::test]
async fn check_get_any() {
    const RANGE_FROM: u32 = 100;
    const RANGE_TO: u32 = 9000;

    let mut inmem = InMemoryIndex::new();
    (RANGE_FROM..RANGE_TO)
        .map(|i| serialize(&i).expect("can't serialize"))
        .for_each(|key| {
            let rh = RecordHeader::new(key.clone(), 1, 1, 1);
            inmem.insert(key, vec![rh]);
        });
    let meta = vec![META_VALUE; META_SIZE];
    let findex = BPTreeFileIndexStruct::from_records(
        &Path::new("/tmp/any_bptree_index.b"),
        None,
        &inmem,
        meta,
        true,
    )
    .await
    .expect("Can't create file index");
    let presented_keys = RANGE_FROM..RANGE_TO;
    for key in presented_keys.map(|k| serialize(&k).unwrap()) {
        assert_eq!(inmem[&key][0], findex.get_any(&key).await.unwrap().unwrap());
    }
    let not_presented_ranges = [0..RANGE_FROM, RANGE_TO..(RANGE_TO + 100)];
    for not_presented_keys in not_presented_ranges.iter() {
        for key in not_presented_keys.clone().map(|k| serialize(&k).unwrap()) {
            assert_eq!(None, findex.get_any(&key).await.unwrap());
        }
    }
}

#[tokio::test]
async fn check_get() {
    const MAX_AMOUNT: u32 = 3;
    const RANGE_FROM: u32 = 100;
    const RANGE_TO: u32 = 9000;

    let mut inmem = InMemoryIndex::new();
    (RANGE_FROM..RANGE_TO)
        .map(|i| (i % MAX_AMOUNT + 1, serialize(&i).expect("can't serialize")))
        .for_each(|(times, key)| {
            let rh = RecordHeader::new(key.clone(), 1, 1, 1);
            let recs = (0..times).map(|_| rh.clone()).collect();
            inmem.insert(key, recs);
        });
    let meta = vec![META_VALUE; META_SIZE];
    let findex = BPTreeFileIndexStruct::from_records(
        &Path::new("/tmp/all_bptree_index.b"),
        None,
        &inmem,
        meta,
        true,
    )
    .await
    .expect("Can't create file index");
    let presented_keys = RANGE_FROM..RANGE_TO;
    for key in presented_keys.map(|k| serialize(&k).unwrap()) {
        assert_eq!(
            inmem[&key],
            findex.find_by_key(&key).await.unwrap().unwrap()
        );
    }
    let not_presented_ranges = [0..RANGE_FROM, RANGE_TO..(RANGE_TO + 100)];
    for not_presented_keys in not_presented_ranges.iter() {
        for key in not_presented_keys.clone().map(|k| serialize(&k).unwrap()) {
            assert_eq!(None, findex.find_by_key(&key).await.unwrap());
        }
    }
}
