use futures::stream::Stream;
use futures::task::*;
use rand::{rngs::*, RngCore};
use std::pin::*;

pub struct Generator {
    config: Config,
    rng: ThreadRng,
    written: usize,
}

impl Generator {
    pub fn new(
        avg_size_of_value: usize,
        avg_size_of_key: usize,
        limit: usize,
        num_obj_pregen: usize,
    ) -> Self {
        Self {
            config: Config {
                avg_size_of_value,
                avg_size_of_key,
                limit,
                num_obj_pregen,
            },
            rng: ThreadRng::default(),
            written: 0,
        }
    }
}

impl Stream for Generator {
    type Item = (Vec<u8>, Vec<u8>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        println!("poll");
        let gen = &mut self;
        if gen.written < gen.config.limit {
            let mut value = vec![0; gen.config.avg_size_of_value];
            let mut key = vec![0; gen.config.avg_size_of_key];
            gen.rng.fill_bytes(&mut value);
            gen.rng.fill_bytes(&mut key);
            gen.written += value.len();
            gen.written += key.len();
            Poll::Ready(Some((value, key)))
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct Config {
    avg_size_of_value: usize,
    avg_size_of_key: usize,
    limit: usize,
    num_obj_pregen: usize,
}