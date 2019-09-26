use crate::prelude::*;

/// @TODO
#[derive(Debug)]
pub struct ReadAll {}

impl Stream for ReadAll {
    type Item = Entry;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}
