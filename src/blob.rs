use std::path::Path;

/// A `Blob` struct for performing of database,
#[derive(Debug, Default)]
pub struct Blob<T> {
    header: Header,
    records: Vec<T>, // @TODO needs verification, created to yield generic T up
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

impl<T> Blob<T> where T: Default {
    pub fn from_file<P: AsRef<Path>>(_path: P) -> Result<Self, ()> {
        // @TODO implement
        Ok(Self::default())
    }
}

#[cfg(test)]
mod tests {
    use super::Blob;

    #[test]
    fn test_blob_new() {
        let _b: Blob<u32> = Blob {
            header: Default::default(),
            records: Vec::new(),
        };
    }
}
