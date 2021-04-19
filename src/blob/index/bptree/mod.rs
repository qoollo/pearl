use super::prelude::*;

mod core;
mod meta;
mod node;
mod serializer;
#[cfg(test)]
mod tests;

//pub(crate) use self::core::BPTreeFileIndex;
// FIXME: restrict visibility
pub(crate) use self::core::BPTreeFileIndexStruct;

mod prelude {
    pub(super) use super::core::BLOCK_SIZE;
    pub(super) use super::serializer::Serializer;
    pub(super) use super::*;
    pub(super) use meta::{NodeMeta, TreeMeta};
    pub(super) use node::Node;
}
