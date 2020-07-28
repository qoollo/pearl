use crate::prelude::*;

/// A specialized storage result type.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for `Storage` operations.
#[derive(Debug, Error)]
pub struct Error {
    repr: Repr,
}

impl Error {
    /// Returns the corresponding `ErrorKind` for this error.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match &self.repr {
            Repr::Inner(k) => k.clone(),
            _ => ErrorKind::Other,
        }
    }

    pub(crate) fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Self {
            repr: Repr::Other(error.into()),
        }
    }

    pub(crate) fn is(&self, other: &ErrorKind) -> bool {
        if let Repr::Inner(kind) = &self.repr {
            kind == other
        } else {
            false
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self.repr, f)
    }
}

impl From<ErrorKind> for Error {
    #[must_use]
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Inner(kind),
        }
    }
}

impl From<IOError> for Error {
    #[must_use]
    fn from(e: IOError) -> Self {
        ErrorKind::IO(e.to_string()).into()
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    #[must_use]
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        ErrorKind::Bincode(e.to_string()).into()
    }
}

impl From<TryFromIntError> for Error {
    #[must_use]
    fn from(e: TryFromIntError) -> Self {
        ErrorKind::Conversion(e.to_string()).into()
    }
}

#[derive(Debug)]
enum Repr {
    Inner(Kind),
    Other(Box<dyn error::Error + 'static + Send + Sync>),
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
    /// Other error
    Other,
}
