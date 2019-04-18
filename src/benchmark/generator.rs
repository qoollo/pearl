use futures::stream::Stream;
use futures::task::*;
use rand::{rngs::*, RngCore};
use std::pin::*;

pub struct Generator {
    config: Config,
    rng: ThreadRng,
    written: u128,
    value: Vec<u8>,
}

impl Generator {
    pub fn new(
        avg_size_of_value: usize,
        avg_size_of_key: usize,
        limit: usize,
        num_obj_pregen: usize,
    ) -> Self {
        let mut value = vec![0; avg_size_of_value];
        let mut rng = ThreadRng::default();
        rng.fill_bytes(&mut value);

        Self {
            config: Config {
                avg_size_of_value,
                avg_size_of_key,
                limit,
                num_obj_pregen,
            },
            rng,
            written: 0,
            value,
        }
    }
}

impl Stream for Generator {
    type Item = (Vec<u8>, Vec<u8>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        print!("\rpoll ");
        let gen = &mut self;
        print!("written: {} ", gen.written);
        if gen.written < gen.config.limit as u128 {
            let key = gen.written.to_be_bytes().to_vec();
            gen.written += gen.value.len() as u128;
            gen.written += key.len() as u128;
            Poll::Ready(Some((key, self.value.clone())))
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