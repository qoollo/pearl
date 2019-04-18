use futures::stream::Stream;
use futures::task::*;
use rand::{rngs::*, RngCore};
use std::pin::*;

pub struct Generator {
    config: Config,
    rng: ThreadRng,
}

impl Generator {
    pub fn new(
        avg_size_of_value: usize,
        avg_size_of_key: usize,
        memory_limit: usize,
        num_obj_pregen: usize,
    ) -> Self {
        Self {
            config: Config {
                avg_size_of_value,
                avg_size_of_key,
                memory_limit,
                num_obj_pregen,
            },
            rng: ThreadRng::default(),
        }
    }
}

impl Stream for Generator {
    type Item = (Vec<u8>, Vec<u8>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        println!("poll");
        let mut value = vec![0; self.as_ref().get_ref().config.avg_size_of_value];
        let mut key = vec![0; self.as_ref().get_ref().config.avg_size_of_key];
        self.rng.fill_bytes(&mut value);
        self.rng.fill_bytes(&mut key);
        Poll::Ready(Some((value, key)))
    }
}

pub struct Config {
    avg_size_of_value: usize,
    avg_size_of_key: usize,
    memory_limit: usize,
    num_obj_pregen: usize,
}
