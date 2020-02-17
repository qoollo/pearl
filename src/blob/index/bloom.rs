use super::prelude::*;
use ahash::AHasher;
use bitvec::prelude::*;
use std::hash::Hasher;

#[derive(Debug)]
pub struct Bloom {
    inner: BitVec,
    hashers: Vec<AHasher>,
}

impl Bloom {
    pub fn new(elements: usize) -> Self {
        let hashes = 2i32;
        let hashers = (0..hashes)
            .map(|i| {
                AHasher::new_with_keys((i + 1).try_into().unwrap(), (i + 2).try_into().unwrap())
            })
            .collect();
        let len = elements as f64 * hashes as f64 / 2f64.ln();
        debug!("create bloom Bloom with len: {:.0}", len);
        let pr = (1f64 - 1f64.exp().powf(-hashes as f64 * elements as f64 / len)).powi(hashes);
        debug!("bloom Bloom false positive rate: {:.6}", pr);
        error!("@TODO");
        Self {
            inner: bitvec![0; len as usize],
            hashers,
        }
    }

    pub fn add(&mut self, item: impl AsRef<[u8]>) {
        let mut hashers = self.hashers.clone();
        let len = self.inner.len() as u64;
        for h in hashers.iter_mut().map(|hasher| {
            hasher.write(item.as_ref());
            hasher.finish() % len
        }) {
            *self
                .inner
                .get_mut(h as usize)
                .expect("impossible due to mod by len") = true;
        }
        debug!("value added to bloom Bloom");
    }

    pub fn contains(&self, item: impl AsRef<[u8]>) -> bool {
        let mut hashers = self.hashers.clone();
        let len = self.inner.len() as u64;
        let res = hashers
            .iter_mut()
            .map(|hasher| {
                hasher.write(item.as_ref());
                hasher.finish() % len
            })
            .all(|i| {
                *self
                    .inner
                    .get(i as usize)
                    .expect("impossible due to mod by len")
            });
        debug!("item definitely missed: {}", !res);
        res
    }

    pub fn save(&self) {
        unimplemented!()
    }
}
