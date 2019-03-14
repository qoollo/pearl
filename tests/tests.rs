use pearl::{Builder, Record};
use std::{env, fs};

#[test]
fn test_storage_init_new() {
    let path = env::temp_dir().join("pearl_new/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build::<u32>().unwrap();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 1);
    let blob_file_path = path.join("test.0.blob");
    assert!(blob_file_path.exists());
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}

#[test]
fn test_storage_read_write() {
    let path = env::temp_dir().join("pearl_rw/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build().unwrap();
    let key = "test";
    storage.init().unwrap();
    let record = Record::new();
    storage.write(record).unwrap();
    assert!(storage.read(&key).is_ok());
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
    // write
    // read
    // close
}

#[test]
fn test_storage_close() {
    let path = env::temp_dir().join("pearl_close/");
    let builder = Builder::new()
        .work_dir(&path)
        .blob_file_name_prefix("test")
        .max_blob_size(1_000_000usize)
        .max_data_in_blob(1_000usize);
    let mut storage = builder.build::<i32>().unwrap();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    fs::remove_file(blob_file_path).unwrap();
    fs::remove_file(path.join("pearl.lock")).unwrap();
    fs::remove_dir(&path).unwrap();
}
