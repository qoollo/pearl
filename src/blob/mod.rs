mod core;
mod entry;
mod file;
mod index;
mod simple_index;

pub(crate) use self::core::{Blob, FileName, Location};
pub(crate) use self::file::File;
pub(crate) use super::prelude::*;
pub use prelude::{Entries, Entry};

mod prelude {
    pub(crate) use super::*;

    pub(crate) use super::core::{FileName, Location};
    pub use entry::{Entries, Entry};
    pub(crate) use file::File;
    pub(crate) use index::{ContainsKey, Count, Dump, Get, Index, Load, Push};
    pub(crate) use simple_index::{SimpleIndex, State};
    pub(crate) use std::collections::VecDeque;
}
