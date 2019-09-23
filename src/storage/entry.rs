use crate::prelude::*;

use super::core::Result;

/// `Entry` similar to `Record`, but does not contain all of the data in memory.
#[derive(Debug)]
pub struct Entry {}

impl Entry {
    /// Converts `Entry` into `Record` by loading skipped parts from disk.
    pub async fn to_record(self) -> Result<Vec<u8>> {
        unimplemented!()
    }
}
