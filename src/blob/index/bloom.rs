use super::prelude::*;

#[derive(Debug, Default, Clone)]
pub struct Bloom {
    inner: BitVec,
    hashers: Vec<AHasher>,
}

impl Bloom {
    pub fn new(elements: usize) -> Self {
        let elements = 10_000_000f64;
        let hashers = Self::hashers();
        let len = elements * hashers.len() as f64 / 2f64.ln();
        debug!("create bloom Bloom with len: {:.0}", len);
        let pr = (1f64
            - 1f64
                .exp()
                .powf((0f64 - hashers.len() as f64) * elements as f64 / len))
        .powi(hashers.len() as i32);
        debug!("bloom Bloom false positive rate: {:.6}", pr);
        error!("@TODO");
        Self {
            inner: bitvec![0; len as usize],
            hashers,
        }
    }

    pub fn hashers() -> Vec<AHasher> {
        error!("@TODO create configurable hashers");
        let hashes = 2i32;
        (0..hashes)
            .map(|i| {
                AHasher::new_with_keys((i + 1).try_into().unwrap(), (i + 2).try_into().unwrap())
            })
            .collect()
    }

    pub fn from_raw(buf: &[u8], bits: usize) -> Self {
        let hashers = Self::hashers();
        debug!("deserialize filter from buf, len = {}", buf.len());
        let buf = bincode::deserialize(buf).unwrap();
        let mut bit_vec = BitVec::from_vec(buf);
        bit_vec.truncate(bits);
        Self {
            inner: bit_vec,
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
        debug!("filter add: {}", self);
    }

    pub fn contains(&self, item: impl AsRef<[u8]>) -> bool {
        debug!("filter: {}", self);
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

    pub fn size(&self) -> u64 {
        bincode::serialized_size(self.inner.as_slice()).unwrap()
    }

    pub fn bits(&self) -> usize {
        self.inner.len()
    }

    pub fn as_slice(&self) -> &[usize] {
        self.inner.as_slice()
    }

    pub fn save(&self) {
        unimplemented!()
    }
}

impl Display for Bloom {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let output = format!("{:#?}", self.inner);
        let output = output.replace("0", " ");
        let output = output.replace("1", "█");
        write!(f, "{}", output)
    }
}
