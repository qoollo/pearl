use pearl::Builder;
use std::fs;
use std::path::Path;

#[test]
fn test_storage_init_new() {
    let path = Path::new("/tmp/pearl_new/");
    let builder = Builder::new().work_dir(path);
    let mut storage = builder.build::<u32>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 1);
    fs::remove_dir(path).unwrap();
}

#[test]
fn test_storage_read_write() {
    let path = Path::new("/tmp/pearl_rw/");
    let storage_builder = Builder::new().work_dir(path);
    let mut storage = storage_builder.build::<u64>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    fs::remove_dir(path).unwrap();
    // write
    // read
    // close
}

#[test]
fn test_storage_close() {
    let path = Path::new("/tmp/pearl_close/");
    let storage_builder = Builder::new().work_dir(path);
    let mut storage = storage_builder.build::<i32>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    fs::remove_dir(path).unwrap();
}
