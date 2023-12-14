/// aHash implementation
mod ahash;
/// Bloom filter
pub mod bloom;
mod combined;
/// Hierarchical
pub mod hierarchical;
/// Range filter
pub mod range;
/// Traits
pub mod traits;
// AtomicBitVec
mod atomic_bitvec;

use super::prelude::*;
pub use bloom::*;
pub use combined::*;
pub use hierarchical::*;
use std::ops::Add;
pub use traits::*;

#[derive(PartialEq, Eq, Debug)]
/// Filter result
pub enum FilterResult {
    /// Need additional check
    NeedAdditionalCheck,
    /// Not contains
    NotContains,
}

impl Default for FilterResult {
    fn default() -> Self {
        Self::NeedAdditionalCheck
    }
}

impl Add for FilterResult {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (FilterResult::NotContains, FilterResult::NotContains) => FilterResult::NotContains,
            _ => FilterResult::NeedAdditionalCheck,
        }
    }
}
