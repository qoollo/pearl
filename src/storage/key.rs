use std::fmt::Debug;
use std::cmp::Ordering;

/// Trait `Key`
pub trait Key<'a>:
    AsRef<[u8]> + From<Vec<u8>> + Debug + Clone + Send + Sync + Ord + Default
{
    /// Key must have fixed length
    const LEN: u16;

    /// Reference type for zero-copy key creation
    type Ref: RefKey<'a>;

    /// Convert `Self` into `Vec<u8>`
    fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }

    /// Convert `Self` to `Self::Ref`
    fn as_ref_key(&'a self) -> Self::Ref {
        Self::Ref::from(self.as_ref())
    }
}

/// Trait for reference key type
pub trait RefKey<'a>: Ord + From<&'a [u8]> {}

/// Key for demonstration purposes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorKey(Vec<u8>);

impl AsRef<VectorKey> for VectorKey {
    fn as_ref(&self) -> &VectorKey {
        self
    }
}

impl Default for VectorKey {
    fn default() -> Self {
        Self([0].to_vec())
    }
}

impl AsRef<[u8]> for VectorKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for VectorKey {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl PartialOrd for VectorKey {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&rhs.0)
    }
}

impl Ord for VectorKey {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.0.cmp(&rhs.0)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SliceKey<'a>(&'a [u8]);

impl<'a> From<&'a [u8]> for SliceKey<'a> {
    fn from(v: &'a [u8]) -> Self {
        Self(v)
    }
}

impl<'a> PartialOrd for SliceKey<'a> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        self.0.partial_cmp(rhs.0)
    }
}

impl<'a> Ord for SliceKey<'a> {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.0.cmp(rhs.0)
    }
}

impl<'a> RefKey<'a> for SliceKey<'a> {}

impl<'a> Key<'a> for VectorKey {
    const LEN: u16 = 1;
    type Ref = SliceKey<'a>;
}
