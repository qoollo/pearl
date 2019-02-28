/// # Description
/// # Examples
#[derive(Debug, Default)]
struct Index {
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
fn test_index_new() {
    let _index = Index {
        header: Default::default(),
    };
}
