use super::*;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct TreeMeta {
    pub(super) root_offset: u64,
    pub(super) leaves_offset: u64,
    pub(super) tree_offset: u64,
    pub(super) key_size: u64,
}

impl TreeMeta {
    pub(super) fn new(
        root_offset: u64,
        leaves_offset: u64,
        tree_offset: u64,
        key_size: u64,
    ) -> Self {
        Self {
            root_offset,
            leaves_offset,
            tree_offset,
            key_size,
        }
    }

    pub(super) fn serialized_size_default() -> bincode::Result<u64> {
        let meta = Self::default();
        meta.serialized_size()
    }

    #[inline]
    pub(super) fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    #[inline]
    pub(super) fn from_raw(buf: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(buf)
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(super) struct NodeMeta {
    pub(super) size: u64,
}

impl NodeMeta {
    pub(super) fn new(size: u64) -> Self {
        Self { size }
    }

    #[inline]
    pub(super) fn serialized_size(&self) -> bincode::Result<u64> {
        bincode::serialized_size(&self)
    }

    pub(super) fn serialized_size_default() -> bincode::Result<u64> {
        let meta = Self::default();
        meta.serialized_size()
    }
}
