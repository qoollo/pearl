mod partially_serialized;
mod record;

pub use record::Meta;
pub(crate) use record::{Header, Record, RECORD_MAGIC_BYTE};
