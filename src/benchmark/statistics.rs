pub struct Statistics {
    max_reports: usize,
    pile: Vec<Report>,
}

impl Statistics {
    pub fn new(max_reports: usize) -> Self {
        Self {
            max_reports,
            pile: Vec::new(),
        }
    }

    pub async fn add(&mut self, report: Report) {
        println!("add");
        self.pile.push(report);
        if self.pile.len() >= self.max_reports {
            await!(self.merge());
        }
        await!(self.display_rt());
    }

    pub async fn merge(&mut self) {
        println!("merge");
    }

    pub async fn display_rt(&mut self) {
        println!("display real time");
    }

    pub async fn display(&mut self) {
        println!("display current");
    }
}

pub struct Report;

impl Report {
    pub fn new() -> Self {
        Self
    }
}
