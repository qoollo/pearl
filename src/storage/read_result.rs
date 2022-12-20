/// Timestamp
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobRecordTimestamp(u64);

/// Result of read operations
#[derive(Debug, PartialEq, Eq)]
pub enum ReadResult<T> {
    /// Data was found
    Found(T),
    /// Data was deleted, contains creation timestamp
    Deleted(BlobRecordTimestamp),
    /// Data was not found
    NotFound,
}

impl BlobRecordTimestamp {
    pub(crate) fn new(t: u64) -> Self {
        BlobRecordTimestamp(t)
    }
}

impl<T> ReadResult<T> {
    pub(crate) fn is_presented(&self) -> bool {
        !matches!(self, ReadResult::NotFound)
    }

    /// Is this found result
    pub fn is_found(&self) -> bool {
        matches!(self, ReadResult::Found(_))
    }

    /// Is this deleted result
    pub fn is_deleted(&self) -> bool {
        matches!(self, ReadResult::Deleted(_))
    }

    /// Is this not found result
    pub fn is_not_found(&self) -> bool {
        matches!(self, ReadResult::NotFound)
    }

    /// Convert to option if data exists
    pub fn into_option(self) -> Option<T> {
        if let ReadResult::Found(d) = self {
            Some(d)
        } else {
            None
        }
    }

    /// Map data if it exists
    pub fn map<Y>(self, f: impl FnOnce(T) -> Y) -> ReadResult<Y> {
        match self {
            ReadResult::Found(d) => ReadResult::Found(f(d)),
            ReadResult::Deleted(ts) => ReadResult::Deleted(ts),
            ReadResult::NotFound => ReadResult::NotFound,
        }
    }

    /// Cast result to type only if does not contain value
    /// Warning: panics if result contains value
    pub fn cast<Y>(self) -> ReadResult<Y> {
        match self {
            ReadResult::Found(_) => panic!("Attempt to cast non-empty read result"),
            ReadResult::Deleted(ts) => ReadResult::Deleted(ts),
            ReadResult::NotFound => ReadResult::NotFound,
        }
    }

    /// Map data if it exists
    pub async fn map_async<Y, F>(self, f: impl FnOnce(T) -> F) -> ReadResult<Y>
    where
        F: futures::Future<Output = Y>,
    {
        match self {
            ReadResult::Found(d) => ReadResult::Found(f(d).await),
            ReadResult::Deleted(ts) => ReadResult::Deleted(ts),
            ReadResult::NotFound => ReadResult::NotFound,
        }
    }

    /// Unwrap into data, panics if no data is set
    pub fn unwrap(self) -> T {
        match self {
            ReadResult::Found(d) => d,
            _ => panic!("Cannot unwrap empty result"),
        }
    }
}
