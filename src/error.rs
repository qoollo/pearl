use crate::prelude::*;

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

    pub(crate) fn validation(cause: impl Into<String>) -> Self {
        Self::new(Kind::Validation(cause.into()))
    }

    pub(crate) fn uninitialized() -> Self {
        Self::new(Kind::Uninitialized)
    }

    pub(crate) fn active_blob_not_set() -> Self {
        Self::new(Kind::ActiveBlobNotSet)
    }

    pub(crate) fn not_found() -> Self {
        Self::new(Kind::RecordNotFound)
    }

    pub(crate) fn io(s: String) -> Self {
        Self::new(Kind::IO(s))
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
    /// Record not found
    RecordNotFound,
    /// Work directory is locked by another storage.
    /// Or the operation lacked the necessary privileges to complete.
    /// Stop another storage or delete `*.lock` file
    WorkDirInUse,
    /// Storage was initialized with different key size
    KeySizeMismatch,
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
    /// Record validation errors, eg. magic byte check
    Validation(String),
    /// Other error
    Other,
}
