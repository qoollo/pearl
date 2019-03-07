/// # Description
/// # Examples
pub struct Record<T> {
    header: Header<T>,
}

impl<T> Record<T>
where
    T: Default,
{
    pub fn new() -> Self {
        Self {
            header: Default::default(),
        }
    }
}

/// # Description
/// # Examples
#[derive(Debug, Default)]
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

#[cfg(test)]
mod tests {
    use super::Record;
    #[test]
    fn test_record_new() {
        let rec: Record<usize> = Record::new();
        assert_eq!(rec.header.magic_byte, 0);
    }
}
