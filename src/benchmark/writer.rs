use crate::statistics::Report;
use pearl::*;

pub struct Writer {
    speed: usize,
}

impl Writer {
    pub fn new(speed: usize) -> Self {
        Self { speed }
    }

    pub async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Report {
        println!("write: key {}, value: {}", key.len(), value.len());
        Report::new()
    }
}
