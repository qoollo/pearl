pub struct Statistics;

impl Statistics {
    pub fn new() -> Self {
        Self
    }

    pub async fn add<T>(&self, report: T) {
        println!("add");
    }
}

pub struct Report;

impl Report {
    pub fn new() -> Self {
        Self
    }
}
