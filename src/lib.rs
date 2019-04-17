#![feature(futures_api, await_macro, async_await, arbitrary_self_types)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![allow(clippy::needless_lifetimes)]

//! # pearl
//!
//! The `pearl` library is a Append only key-value blob storage on disk.
//! Crate `pearl` provides [`Futures 0.3`] interface.
//!
//! [`Futures 0.3`]: https://rust-lang-nursery.github.io/futures-api-docs#latest

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod blob;
mod record;
mod storage;

pub use record::Record;
pub use storage::{Builder, Storage};
