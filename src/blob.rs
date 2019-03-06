use std::path::Path;

/// A `Blob` struct for performing of database
/// # Examples
#[derive(Debug, Default)]
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

impl Blob {
    pub fn from_file<P: AsRef<Path>>(_path: P) -> Result<Self, ()> {
        // @TODO implement
        unimplemented!()
    }

    pub fn is_opened(&self) -> bool {
        unimplemented!()
    }
}

#[test]
fn test_blob_new() {
    let _ = Blob {
        header: Default::default(),
    };
}
