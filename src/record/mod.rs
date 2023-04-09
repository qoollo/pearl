mod partially_serialized;
mod record;

pub(crate) use partially_serialized::PartiallySerializedRecord;
pub use record::Meta;
pub(crate) use record::{Header, Record, RECORD_MAGIC_BYTE};
