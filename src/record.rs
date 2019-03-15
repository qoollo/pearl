use std::mem;

/// # Description
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Record<K> {
    header: Header<K>,
    data: Vec<u8>,
}

impl<K> Record<K>
where
    K: Default,
{
    /// # Description
    /// Creates new `Record`, temporarily as `Default`
    /// # Examples
    /// ```no-run
    /// use pearl::Record;
    /// let rec = Record::new();
    /// ```
    pub fn new() -> Self {
        Self {
            header: Default::default(),
            data: Vec::new(),
        }
    }

    /// # Description
    /// Get number of bytes, struct `Record` uses on disk
    pub fn size(&self) -> usize {
        // @TODO implement
        self.header.size as usize + Header::<K>::unaligned_heades_size()
    }

    /// # Description
    /// Get record key reference
    pub fn key(&self) -> &K {
        &self.header.key
    }

    /// # Description
    /// Set data to Record, replacing if exists
    pub fn set_data(&mut self, d: Vec<u8>) {
        self.data = d;
    }

    /// # Description
    pub fn set_key(&mut self, key: K) {
        self.header.key = key;
    }
}

/// # Description
/// # Examples
#[derive(Serialize, Deserialize, Debug, Default)]
struct Header<T>
where
    T: Sized,
{
    magic_byte: u64,
    size: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    key: T,
    data_checksum: u32,
    header_checksum: u32,
}

impl<K> Header<K> {
    fn unaligned_heades_size() -> usize {
        let sizeofu64 = mem::size_of::<u64>();
        let sizeofu32 = mem::size_of::<u32>();
        let sizeofu8 = mem::size_of::<u8>();
        let sizeofkey = mem::size_of::<K>();
        let unaligned = sizeofu64 * 4 + sizeofu32 * 2 + sizeofu8 + sizeofkey;
        let aligned = mem::size_of::<Self>();
        if unaligned > aligned {
            error!("bug in unaligned size calculation");
            aligned
        } else {
            unaligned
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Record;
    #[test]
    fn test_record_new() {
        let rec: Record<usize> = Record::new();
        assert_eq!(rec.header.magic_byte, 0);
    }
}
