use super::prelude::*;

pub struct Generator {
    limit: u64,
    written: u64,
    value: Vec<u8>,
}

impl Generator {
    pub fn new(avg_size_of_value: usize, limit: u64) -> Self {
        let mut value = vec![0; avg_size_of_value];
        let mut rng = ThreadRng::default();
        rng.fill_bytes(&mut value);

        Self {
            limit,
            written: 0,
            value,
        }
    }

    pub fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.written < self.limit * 1_000_000 {
            let key = self.written.to_be_bytes().to_vec();
            let data = self.value.clone();
            self.written += (key.len() + data.len()) as u64;
            Some((key, data))
        } else {
            None
        }
    }
}
