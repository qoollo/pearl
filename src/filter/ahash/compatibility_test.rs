use std::hash::Hasher;
use super::AHasher;

#[test]
fn test_hash_algorithm_compat() {
    test_hash(&(0..10).collect::<Vec<u8>>(), 3604729491498336444);
    test_hash(&(245..255).collect::<Vec<u8>>(), 4698010058046694585);
    test_hash(&(63..73).collect::<Vec<u8>>(), 7892047681755360091);
    test_hash(&(101..111).collect::<Vec<u8>>(), 15822444892006722439);
}

fn test_hash(data: &[u8], eq_to: u64) {
    let mut hasher_7 = AHasher::new_with_keys(1, 2);
    hasher_7.write(data);
    let hash_7 = hasher_7.finish();

    assert_eq!(hash_7, eq_to);
}

