use bitvec::prelude::*;

#[derive(Debug)]
pub struct Filter {
    inner: BitVec,
}

impl Filter {
    pub fn new(elements: usize) -> Self {
        let hashes = 2i32;
        let len = elements as f64 * hashes as f64 / 2f64.ln();
        debug!("create bloom filter with len: {:.0}", len);
        let pr = (1f64 - 1f64.exp().powf(-hashes as f64 * elements as f64 / len)).powi(hashes);
        debug!("bloom filter false positive rate: {:.6}", pr);
        error!("@TODO");
        Self {
            inner: bitvec![0; len as usize],
        }
    }

    pub fn add<T>(&self, item: T) {
        debug!("bloom filter add value");
        error!("@TODO");
    }

    pub fn contains<T>(&self, item: T) -> bool {
        debug!("bloom filter check contains");
        error!("@TODO");
        true
    }

    pub fn save(&self) {}
}
