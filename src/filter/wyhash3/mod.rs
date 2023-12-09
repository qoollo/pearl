#![allow(all)]
mod functions;
pub use functions::{make_secret, wyhash, wyrng};

mod traits;
pub use traits::{WyHash, WyRng};

/// Temp doc
pub const P0: u64 = 0xa076_1d64_78bd_642f;
/// Temp doc
pub const P1: u64 = 0xe703_7ed1_a0b4_28db;
/// Temp doc
pub const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
/// Temp doc
pub const P3: u64 = 0x5899_65cc_7537_4cc3;

#[inline]
/// Temp doc
pub fn read32(data: &[u8]) -> u64 {
    u64::from(data[3]) << 24
        | u64::from(data[2]) << 16
        | u64::from(data[1]) << 8
        | u64::from(data[0])
}

#[inline]
/// Temp doc
pub fn read64(data: &[u8]) -> u64 {
    u64::from(data[7]) << 56
        | u64::from(data[6]) << 48
        | u64::from(data[5]) << 40
        | u64::from(data[4]) << 32
        | u64::from(data[3]) << 24
        | u64::from(data[2]) << 16
        | u64::from(data[1]) << 8
        | u64::from(data[0])
}
