/// Error type defenition
pub mod error;
pub(crate) mod impls;
pub(crate) mod readers;
pub(crate) mod utils;

pub use utils::*;

pub(crate) mod prelude {
    pub(crate) use super::error::*;
    pub(crate) use super::impls::*;
    pub(crate) use super::readers::*;
    pub(crate) use super::utils::*;
    pub(crate) use crate::prelude::*;
    pub(crate) use anyhow::{Context, Result as AnyResult};
    pub(crate) use std::{
        fmt::Debug,
        fs::{File, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        path::Path,
    };
}
