use pearl::Builder;

#[test]
fn test_storage_init_new() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok());
    assert_eq!(storage.blobs_count(), 1);
}

#[test]
fn test_storage_read() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok())
}

#[test]
fn test_storage_write() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok())
}

#[test]
fn test_storage_close() {
    let storage_builder = Builder::new().work_dir("/tmp/pearl/");
    let mut storage = storage_builder.build();
    assert!(storage.init().map_err(|e| eprintln!("{}", e)).is_ok())
}
