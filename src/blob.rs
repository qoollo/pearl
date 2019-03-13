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

impl<K> Blob<K> where K: Default {
    /// # Description
    /// Create new blob from file
    // @TODO more useful result
    pub fn from_file<P: AsRef<Path>>(_path: P) -> Result<Self, ()> {
        // @TODO implement
        Ok(Self::default())
    }

    /// # Description
    /// Writes given slice to file, returns number of written bytes
    // @TODO more useful result
    pub fn write(&mut self, _key: &K, _value: &[u8]) -> Result<usize, ()>  {
        // @TODO implement
        Ok(0usize)
    }

    /// # Description
    /// Reads record data, yields `Ok(Vec<u8>)` if read successful,
    /// otherwise - `Err`
    // @TODO more useful result
    pub fn read(&self, _key: &K) -> Result<Vec<u8>, ()>  {
        // @TODO implement
        Ok(Vec::new())
    }

    /// # Description
    /// Closes current blob and sets blob file in readonly mode
    // @TODO more useful result
    pub fn close(&mut self) -> Result<(), ()> {
        // @TODO implement
        Ok(())

    }

    /// # Description
    /// Returns size of file in bytes
    // @TODO more useful result
    pub fn size(&self) -> Result<usize, ()> {
        // @TODO implement
        Ok(0usize)
    }

    /// # Description
    /// Returns number of records in current blob
    // @TODO more useful result
    pub fn len(&self) -> Result<usize, ()> {
        // @TODO implement
        Ok(0usize)
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
