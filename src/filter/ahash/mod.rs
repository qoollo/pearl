// Original work: Copyright (c) 2018 Tom Kaitchuck
// Distributed under MIT license
// Taken from aHash version 0.7.4 (commit ffa04fcb81f39755f636c75c9b7aa06533c0ae75)

//! AHash is a hashing algorithm is intended to be a high performance, (hardware specific), keyed hash function.
//! This can be seen as a DOS resistant alternative to `FxHash`, or a fast equivalent to `SipHash`.
//! It provides a high speed hash algorithm, but where the result is not predictable without knowing a Key.
//! This allows it to be used in a `HashMap` without allowing for the possibility that an malicious user can
//! induce a collision.
//!
//! # How aHash works
//!
//! aHash uses the hardware AES instruction on x86 processors to provide a keyed hash function.
//! aHash is not a cryptographically secure hash.
#![deny(clippy::correctness, clippy::complexity, clippy::perf)]
#![allow(clippy::pedantic, clippy::cast_lossless, clippy::unreadable_literal)]

#[macro_use]
mod convert;
mod fallback_hash;
mod operations;

#[doc(hidden)]
pub const PI: [u64; 4] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];

pub use self::fallback_hash::AHasher;

#[cfg(feature = "std")]
#[cfg(test)]
mod test {
    use super::convert::Convert;
    use super::*;
    use core::hash::Hasher;
    use std::hash::Hash;

    #[test]
    fn test_conversion() {
        let input: &[u8] = b"dddddddd";
        let bytes: u64 = as_array!(input, 8).convert();
        assert_eq!(bytes, 0x6464646464646464);
    }

    #[test]
    fn test_non_zero() {
        let mut hasher1 = AHasher::new_with_keys(0, 0);
        let mut hasher2 = AHasher::new_with_keys(0, 0);
        "foo".hash(&mut hasher1);
        "bar".hash(&mut hasher2);
        assert_ne!(hasher1.finish(), 0);
        assert_ne!(hasher2.finish(), 0);
        assert_ne!(hasher1.finish(), hasher2.finish());

        let mut hasher1 = AHasher::new_with_keys(0, 0);
        let mut hasher2 = AHasher::new_with_keys(0, 0);
        3_u64.hash(&mut hasher1);
        4_u64.hash(&mut hasher2);
        assert_ne!(hasher1.finish(), 0);
        assert_ne!(hasher2.finish(), 0);
        assert_ne!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_ahasher_construction() {
        let _ = AHasher::new_with_keys(1234, 5678);
    }
}
