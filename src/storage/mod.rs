mod builder;
mod config;
mod core;
mod observer;
mod observer_worker;
mod key;

pub use self::{
    builder::Builder,
    core::{Storage, ReadResult},
    key::{Key, RefKey, ArrayKey},
    observer::ActiveBlobPred,
    observer::ActiveBlobStat,
};

mod prelude {
    pub(crate) use {
        super::{
            config::Config, core::Inner, observer::Msg, observer::Observer,
            observer::OperationType, observer_worker::ObserverWorker, ActiveBlobPred,
            ActiveBlobStat,
        },
        crate::prelude::*,
    };
    pub(crate) use async_std::sync::RwLock as ASRwLock;
}
