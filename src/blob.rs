/// A `Blob` struct for performing of database
/// # Examples
#[derive(Debug)]
pub struct Blob {
    header: Header,
}

/// # Description
/// # Examples
#[derive(Debug, Default)]
struct Header {
    magic_byte: u64,
    version: u32,
    key_size: u32,
    flags: u64,
}

#[test]
fn test_blob_new() {
    let _ = Blob {
        header: Default::default(),
    };
}
