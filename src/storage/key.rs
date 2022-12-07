use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;

/// Trait `Key`
pub trait Key<'a>:
    AsRef<[u8]> + From<Vec<u8>> + From<&'a [u8]> + Debug + Clone + Send + Sync + Ord + Default
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
pub struct ArrayKey<const N: usize>([u8; N]);

impl<const N: usize> AsRef<ArrayKey<N>> for ArrayKey<N> {
    fn as_ref(&self) -> &ArrayKey<N> {
        self
    }
}

impl<const N: usize> Default for ArrayKey<N> {
    fn default() -> Self {
        Self([0; N])
    }
}

impl<const N: usize> AsRef<[u8]> for ArrayKey<N> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> From<Vec<u8>> for ArrayKey<N> {
    fn from(v: Vec<u8>) -> Self {
        Self(v.try_into().expect("size mismatch"))
    }
}

impl<const N: usize> From<&[u8]> for ArrayKey<N> {
    fn from(a: &[u8]) -> Self {
        Self(a.try_into().expect("size mismatch"))
    }
}

impl<const N: usize> PartialOrd for ArrayKey<N> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&rhs.0)
    }
}

impl<const N: usize> Ord for ArrayKey<N> {
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

impl<'a, const N: usize> Key<'a> for ArrayKey<N> {
    const LEN: u16 = N as u16;
    type Ref = SliceKey<'a>;
}
