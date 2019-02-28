use pearl::Builder;

#[test]
fn test_storage_init() {
    let storage_builder = Builder::new().work_dir("/tmp/");
    let _storage = storage_builder.build();
}

#[test]
fn test_storage_read() {
    let storage_builder = Builder::new().work_dir("/tmp/");
    let mut storage = storage_builder.build();
    assert!(storage.init().is_ok())
}

#[test]
fn test_storage_write() {
    let storage_builder = Builder::new().work_dir("/tmp/");
    let mut storage = storage_builder.build();
    assert!(storage.write().is_ok())
}

#[test]
fn test_storage_close() {
    let storage_builder = Builder::new().work_dir("/tmp/");
    let mut storage = storage_builder.build();
    assert!(storage.close().is_ok())
}
