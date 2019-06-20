use std::{error, fmt, result};

#[derive(Debug)]
pub struct Error {
    repr: Repr,
}

impl Error {
    pub(crate) fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Self {
            repr: Repr::Other(error.into()),
        }
    }

    pub(crate) fn raw<M>(msg: M) -> Self
    where
        M: AsRef<str>,
    {
        Self {
            repr: Repr::Raw(msg.as_ref().to_owned()),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.repr {
            Repr::Inner(_) => None,
            Repr::Other(src) => Some(src.as_ref()),
            Repr::Raw(_) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> result::Result<(), fmt::Error> {
        self.repr.fmt(f)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Inner(kind),
        }
    }
}

#[derive(Debug)]
enum Repr {
    Inner(ErrorKind),
    Other(Box<dyn error::Error + 'static + Send + Sync>),
    Raw(String),
}

impl fmt::Display for Repr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> result::Result<(), fmt::Error> {
        match self {
            Repr::Inner(kind) => write!(f, "{}", kind.as_str()),
            Repr::Other(e) => e.fmt(f),
            Repr::Raw(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    ActiveBlobNotSet,
    WrongConfig,
    Uninitialized,
    RecordNotFound,
    WorkDirInUse,
    KeySizeMismatch,
}

impl ErrorKind {
    fn as_str(&self) -> &'static str {
        match *self {
            ErrorKind::ActiveBlobNotSet => "active blob not set",
            ErrorKind::WrongConfig => "wrong config",
            ErrorKind::Uninitialized => "storage unitialized",
            ErrorKind::RecordNotFound => "record not found",
            ErrorKind::WorkDirInUse => "work dir in use",
            ErrorKind::KeySizeMismatch => "key size mismatch",
        }
    }
}
