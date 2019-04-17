pub struct Writer;

impl Writer {
    pub fn new() -> Self {
        Self
    }

    pub async fn write(&self) {
        println!("write");
    }
}
