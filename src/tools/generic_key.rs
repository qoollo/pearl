use super::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericKey<const N: usize>([u8; N]);

impl<'a> RefKey<'a> for &'a [u8] {}

impl<'a, const N: usize> KeyTrait<'a> for GenericKey<N> {
    type Ref = &'a [u8];
    const LEN: u16 = N as u16;
}

impl<T: Into<Vec<u8>>, const N: usize> From<T> for GenericKey<N> {
    fn from(t: T) -> Self {
        let mut v = t.into();
        v.resize(Self::LEN as usize, 0);
        Self(v.try_into().expect("have correct size"))
    }
}

impl<const N: usize> Default for GenericKey<N> {
    fn default() -> Self {
        Self([0_u8; N])
    }
}

impl<const N: usize> AsRef<[u8]> for GenericKey<N> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> AsRef<GenericKey<N>> for GenericKey<N> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<const N: usize> PartialOrd for GenericKey<N> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        for i in (0..(Self::LEN as usize)).rev() {
            let ord = self.0[i].cmp(&other.0[i]);
            if ord != Ordering::Equal {
                return Some(ord);
            }
        }
        Some(Ordering::Equal)
    }
}

impl<const N: usize> Ord for GenericKey<N> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}
