use pearl::Builder;

#[test]
fn test_storage_init_new() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build::<u32>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 1);
}

#[test]
fn test_storage_read_write() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build::<u64>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok())
    // write
    // read
    // close
}

#[test]
fn test_storage_close() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build::<i32>();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok())
}
