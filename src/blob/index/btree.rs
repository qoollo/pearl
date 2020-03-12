use super::prelude::*;

/// B+ Tree based index

#[derive(Debug)]
pub(crate) struct BTree {
    header: Header,
    nodes: Vec<Node>,
    name: FileName,
}

#[derive(Debug, Deserialize, Default, Serialize, Clone)]
struct Header {
    record_header_size: usize,
}

#[derive(Debug, Deserialize, Default, Serialize, Clone)]
struct Node {
    is_leaf: bool,
    keys: Vec<Pair>,
}

#[derive(Debug, Deserialize, Default, Serialize, Clone)]
struct Pair {
    key: u64,
    offset: usize,
}

impl BTree {
    pub(crate) fn new(name: FileName) -> Self {
        Self {
            header: Header::new(),
            nodes: Vec::new(),
            name,
        }
    }
}

impl Header {
    fn new() -> Self {
        Self {
            record_header_size: 0,
        }
    }
}
