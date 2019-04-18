use futures::stream::Stream;
use futures::task::*;
use std::pin::*;

pub struct Generator {
    config: Config,
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
        }
    }
}

impl Stream for Generator {
    type Item = (Vec<u8>, Vec<u8>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        println!("poll");
        Poll::Ready(Some((Vec::new(), Vec::new())))
    }
}

pub struct Config {
    avg_size_of_value: usize,
    avg_size_of_key: usize,
    memory_limit: usize,
    num_obj_pregen: usize,
}