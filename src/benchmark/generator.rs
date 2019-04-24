use rand::{rngs::*, RngCore};

use pearl::Record;

pub struct Generator {
    config: Config,
    written: u128,
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

    pub fn next(&mut self) -> Option<Record> {
        if self.written < self.config.limit as u128 * 1_000_000 {
            let key = self.written.to_be_bytes().to_vec();
            let mut record = Record::new();
            record.set_body(key, self.value.clone());
            self.written += u128::from(record.full_len());
            Some(record)
        } else {
            None
        }
    }
}

pub struct Config {
    limit: usize,
}
