use super::prelude::*;
use thiserror::Error;

/// Error type
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to validate record header
    #[error("record header validation error: {0}")]
    RecordHeaderValidation(String),
    /// Failed to validate index header
    #[error("index header validation error: {0}")]
    IndexHeaderValidation(String),
    /// Failed to validate index
    #[error("index validation error: {0}")]
    IndexValidation(String),
    /// Failed to validate blob header
    #[error("blob header validation error: {0}")]
    BlobHeaderValidation(String),
    /// Failed to validate record
    #[error("record validation error: {0}")]
    RecordValidation(String),
    /// Skip wrong record is not possible
    #[error("skip data error: {0}")]
    SkipRecordData(String),
    /// Can't migrate blob
    #[error("unsupported migration from {0} to {1}")]
    UnsupportedMigration(u32, u32),
    /// Key size is not supported
    #[error("key size is not supported by this build")]
    UnsupportedKeySize(u16),
}

impl Error {
    pub(crate) fn record_header_validation_error(message: impl Into<String>) -> Self {
        Self::RecordHeaderValidation(message.into())
    }

    pub(crate) fn index_header_validation_error(message: impl Into<String>) -> Self {
        Self::IndexHeaderValidation(message.into())
    }

    pub(crate) fn blob_header_validation_error(message: impl Into<String>) -> Self {
        Self::BlobHeaderValidation(message.into())
    }

    pub(crate) fn record_validation_error(message: impl Into<String>) -> Self {
        Self::RecordValidation(message.into())
    }

    pub(crate) fn skip_record_data_error(message: impl Into<String>) -> Self {
        Self::SkipRecordData(message.into())
    }

    pub(crate) fn unsupported_migration(from: u32, to: u32) -> Self {
        Self::UnsupportedMigration(from, to)
    }

    pub(crate) fn unsupported_key_size(size: u16) -> Self {
        Self::UnsupportedKeySize(size)
    }
}
