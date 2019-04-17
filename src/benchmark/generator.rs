use futures::stream::Stream;
use futures::task::*;
use std::pin::*;

pub struct Generator;

impl Generator {
    pub fn new() -> Self {
        Self
    }
}

impl Stream for Generator {
    type Item = ((), ());

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        println!("poll");
        Poll::Ready(Some(((), ())))
    }
}
