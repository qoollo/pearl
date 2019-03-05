#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![cfg_attr(test, deny(warnings))]

//! # pearl
//!
//! The `pearl` crate provides Append only key-value blob storage on disk

// #[macro_use]
// extern crate log;

mod blob;
mod index;
mod record;
mod storage;

pub use storage::{Builder, Storage};
