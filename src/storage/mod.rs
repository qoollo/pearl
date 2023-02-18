mod builder;
mod config;
mod core;
mod key;
mod observer;
mod observer_worker;
mod read_result;

pub use self::{
    builder::Builder,
    core::Storage,
    key::{ArrayKey, Key, RefKey},
    observer::ActiveBlobPred,
    observer::ActiveBlobStat,
    read_result::{BlobRecordTimestamp, ReadResult},
};

mod prelude {
    pub(crate) use async_std::sync::RwLock as ASRwLock;
    pub(crate) use {
        super::{
            config::Config, core::Inner, observer::Msg, observer::Observer,
            observer::OperationType, observer_worker::ObserverWorker, ActiveBlobPred,
            ActiveBlobStat,
        },
        crate::prelude::*,
    };
}
