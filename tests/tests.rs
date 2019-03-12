use pearl::Builder;
use std::{env, fs, path::Path};

#[test]
fn test_storage_init_new() {
    let path = env::temp_dir().join("pearl_new/");
    let builder = Builder::new().work_dir(&path);
    let mut storage = builder.build::<u32>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 1);
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_storage_read_write() {
    let path = env::temp_dir().join("pearl_rw/");
    let storage_builder = Builder::new().work_dir(&path);
    let mut storage = storage_builder.build::<u64>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
    // write
    // read
    // close
}

#[test]
fn test_storage_close() {
    let path = env::temp_dir().join("pearl_close/");
    let storage_builder = Builder::new().work_dir(&path);
    let mut storage = storage_builder.build::<i32>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}
