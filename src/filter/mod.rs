/// aHash implementation
pub mod ahash;
/// Bloom filter
pub mod bloom;
/// Hierarchical
pub mod hierarchical;
/// Range filter
pub mod range;
/// Traits
pub mod traits;
/// Temp doc
pub mod wyhash3;

use self::{ahash::AHasher, wyhash3::WyHash};

use super::prelude::*;
pub use bloom::*;
pub use hierarchical::*;
pub use range::*;
use std::ops::Add;
pub use traits::*;

mod tests;

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

impl SeededHash for AHasher {
    fn new(seed: u128) -> Self {
        Self::new_with_keys(seed, seed + 1)
    }

    fn box_clone(&self) -> Box<dyn SeededHash>
    where
        Self: Sized,
    {
        Box::new(self.clone())
    }
}

impl SeededHash for WyHash {
    fn new(seed: u128) -> Self {
        Self::new(seed as u64, wyhash3::make_secret(seed as u64))
    }

    fn box_clone(&self) -> Box<dyn SeededHash>
    where
        Self: Sized,
    {
        Box::new(self.clone())
    }
}
