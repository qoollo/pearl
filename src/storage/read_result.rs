use std::fmt::Display;
use crate::Entry;

/// Timestamp
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    /// Creates timestamp from user supplied number
    pub fn new(t: u64) -> Self {
        BlobRecordTimestamp(t)
    }

    /// Current UNIX timestamp
    pub fn now() -> Self {
        BlobRecordTimestamp(
            match std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH) {
                Ok(n) => n.as_secs(),
                Err(_) => 0,
            })
    }
}

impl Display for BlobRecordTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Into<u64> for BlobRecordTimestamp {
    fn into(self) -> u64 {
        self.0
    }
}

impl<T> ReadResult<T> {
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

impl ReadResult<BlobRecordTimestamp> {
    fn timestamp(&self) -> Option<BlobRecordTimestamp> {
        match &self {
            ReadResult::Found(ts) => Some(*ts),
            ReadResult::Deleted(ts) => Some(*ts),
            ReadResult::NotFound => None
        }
    }

    /// Returns [`ReadResult<BlobRecordTimestamp>`] with max timetamp.
    /// If timestamps are equal, then `self` is preserved
    pub fn latest(self, other: ReadResult<BlobRecordTimestamp>) -> ReadResult<BlobRecordTimestamp> {
        if other.timestamp() > self.timestamp() {
            other
        } else {
            self
        }
    }
}


impl ReadResult<Entry> {
    fn timestamp(&self) -> Option<BlobRecordTimestamp> {
        match &self {
            ReadResult::Found(entry) => Some(entry.timestamp()),
            ReadResult::Deleted(ts) => Some(*ts),
            ReadResult::NotFound => None
        }
    }

    /// Returns [`ReadResult<Entry>`] with max timetamp.
    /// If timestamps are equal, then `self` is preserved
    pub fn latest(self, other: ReadResult<Entry>) -> ReadResult<Entry> {
        if other.timestamp() > self.timestamp() {
            other
        } else {
            self
        }
    }
}