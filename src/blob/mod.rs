mod core;
mod entry;
mod file;
mod index;
mod simple_index;

pub(crate) use self::core::{Blob, FileName, Location};
pub(crate) use self::file::File;
pub use prelude::{Entries, Entry};

mod prelude {
    pub(crate) use super::core::{FileName, Location};
    pub use super::entry::{Entries, Entry};
    pub(crate) use super::file::File;
    pub(crate) use super::index::{ContainsKey, Count, Dump, Get, Index, Load, Push};
    pub(crate) use super::simple_index::State;
}
