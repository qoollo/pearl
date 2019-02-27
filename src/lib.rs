#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![cfg_attr(test, deny(warnings))]

//! # pearl
//!
//! The 'pearl' crate provides Append only key-value blob storage on disk

/// A 'Blob' struct for performing of database
#[derive(Debug)]
pub struct Blob<T> {
    header: Header<T>,
}

#[derive(Debug)]
struct Header<T>
where
    T: Sized,
{
    magic_byte: u64,
    key: T,
    flags: u8,
    size: u64,
    blob_offset: u64,
    created: u64,
    modified: u64,
    checksum: u32,
}
