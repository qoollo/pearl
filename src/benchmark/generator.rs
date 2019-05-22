use rand::{rngs::*, RngCore};

pub struct Generator {
    config: Config,
    written: u64,
    value: Vec<u8>,
}

impl Generator {
    pub fn new(avg_size_of_value: usize, limit: usize) -> Self {
        let mut value = vec![0; avg_size_of_value];
        let mut rng = ThreadRng::default();
        rng.fill_bytes(&mut value);

        Self {
            config: Config { limit },
            written: 0,
            value,
        }
    }

    pub fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.written < self.config.limit as u64 * 1_000_000 {
            let key = self.written.to_be_bytes().to_vec();
            let data = self.value.clone();
            self.written += (key.len() + data.len()) as u64;
            Some((key, data))
        } else {
            None
        }
    }
}

pub struct Config {
    limit: usize,
}
