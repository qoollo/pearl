use crate::prelude::*;
use anyhow::Error as AHError;

/// The error type for `Storage` operations.
#[derive(Debug, Error)]
pub struct Error {
    kind: Kind,
}

impl Error {
    /// Returns the corresponding `Kind` for this error.
    #[must_use]
    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    pub(crate) fn new(kind: Kind) -> Self {
        Self { kind }
    }

    pub(crate) fn file_pattern(path: PathBuf) -> Self {
        Self::new(Kind::WrongFileNamePattern(path))
    }

    pub(crate) fn validation(kind: ValidationErrorKind, cause: impl Into<String>) -> Self {
        let cause = cause.into();
        Self::new(Kind::Validation { kind, cause })
    }

    pub(crate) fn uninitialized() -> Self {
        Self::new(Kind::Uninitialized)
    }

    pub(crate) fn active_blob_not_set() -> Self {
        Self::new(Kind::ActiveBlobNotSet)
    }

    pub(crate) fn active_blob_doesnt_exist() -> Self {
        Self::new(Kind::ActiveBlobDoesntExist)
    }

    pub(crate) fn active_blob_already_exists() -> Self {
        Self::new(Kind::ActiveBlobExists)
    }

    #[allow(dead_code)]
    pub(crate) fn io(s: String) -> Self {
        Self::new(Kind::IO(s))
    }

    pub(crate) fn bincode(s: String) -> Self {
        Self::new(Kind::Bincode(s))
    }

    pub(crate) fn file_unavailable(kind: IOErrorKind) -> Self {
        Self::new(Kind::FileUnavailable(kind))
    }

    pub(crate) fn work_dir_unavailable(
        path: impl AsRef<Path>,
        msg: String,
        io_err_kind: IOErrorKind,
    ) -> Self {
        Self::new(Kind::WorkDirUnavailable {
            path: path.as_ref().into(),
            msg,
            io_err_kind,
        })
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self.kind, f)
    }
}

impl From<Kind> for Error {
    #[must_use]
    fn from(kind: Kind) -> Self {
        Self { kind }
    }
}

/// A list specifying categories of Storage error.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// Active blob not set, often initialization failed.
    ActiveBlobNotSet,
    /// Input configuration is wrong.
    WrongConfig,
    /// Probably storage initialization failed.
    Uninitialized,
    /// Work directory is locked by another storage.
    /// Or the operation lacked the necessary privileges to complete.
    /// Stop another storage or delete `*.lock` file
    WorkDirInUse,
    /// Happens when try to write/read from work dir that doesn't exist.
    /// In case when work dir wasn't created or disk was unmounted.
    /// Contains path to failed work dir, IOError description and IOErrorKind.
    WorkDirUnavailable {
        /// path of unavailable dir
        path: PathBuf,
        /// os error message (or custom one if we can't create directory during initialization)
        msg: String,
        /// IO Error Kind (`NotFound` or `Other`)
        io_err_kind: IOErrorKind,
    },
    /// Blob detects os errors during IO operation which indicate possible problems with disk
    FileUnavailable(IOErrorKind),
    /// Storage was initialized with different key size
    KeySizeMismatch,
    /// Active blob doesn't exist
    ActiveBlobDoesntExist,
    /// Active blob already exists
    ActiveBlobExists,
    /// Record with the same key and the same metadata already exists
    RecordExists,
    /// Any error not part of this list
    EmptyIndexBunch,
    /// Index error
    Index(String),
    /// Bincode serialization deserialization error
    Bincode(String),
    /// std::io::Error
    IO(String),
    /// Wrong file name pattern in config
    WrongFileNamePattern(PathBuf),
    /// Conversion error
    Conversion(String),
    /// Validation errors, eg. magic byte check
    Validation {
        /// Describes what check failed.
        kind: ValidationErrorKind,
        /// Description of an error cause.
        cause: String,
    },

    /// Other error
    Other,
}

/// Variants of validation errors.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationErrorKind {
    /// Blob key size.
    BlobKeySize,
    /// Blob magic byte.
    BlobMagicByte,
    /// Blob version.
    BlobVersion,
    /// Index checksum.
    IndexChecksum,
    /// Index version.
    IndexVersion,
    /// Index key size.
    IndexKeySize,
    /// Index magic byte
    IndexMagicByte,
    /// Record data checksum.
    RecordDataChecksum,
    /// Record header checksum.
    RecordHeaderChecksum,
    /// Record magic byte.
    RecordMagicByte,
    /// Index blob size
    IndexBlobSize,
    /// Index is not written (index header corrupted)
    IndexNotWritten,
}

/// Convenient helper for downcasting anyhow error to pearl error.
pub trait AsPearlError {
    /// Performs conversion.
    fn as_pearl_error(&self) -> Option<&Error>;
}

impl AsPearlError for anyhow::Error {
    fn as_pearl_error(&self) -> Option<&Error> {
        self.downcast_ref()
    }
}

/// Util function for UnexpectedEOF error conversion
pub fn unexpected_eof_converter(e: AHError) -> AHError {
    if let Some(io_error) = e.downcast_ref::<std::io::Error>() {
        if let std::io::ErrorKind::UnexpectedEof = io_error.kind() {
            return AHError::from(Error::bincode("unexpected eof".into()));
        }
    }
    e
}

/// Util function for UnexpectedEOF error conversion with additional context
pub fn unexpected_eof_converter_ctx(e: AHError, context: String) -> AHError {
    if let Some(io_error) = e.downcast_ref::<std::io::Error>() {
        if let std::io::ErrorKind::UnexpectedEof = io_error.kind() {
            return AHError::from(Error::bincode("unexpected eof".into())).context(context);
        }
    }
    e.context(context)
}