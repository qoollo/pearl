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
        print!("add ");
        self.pile.push(report);
        if self.pile.len() >= self.max_reports {
            await!(self.merge());
        }
        await!(self.display_rt());
    }

    pub async fn merge(&mut self) {
        let new_pile: Vec<_> = self
            .pile
            .chunks(2)
            .map(|pair| pair.iter().fold(Report::new(), |ra, rb| ra + *rb))
            .collect();
        print!(
            "merge: before {}, after {} ",
            self.pile.len(),
            new_pile.len()
        );
        self.pile = new_pile;
    }

    pub async fn display_rt(&mut self) {
        print!("[{}] display real time                ", self.pile.len());
    }

    pub async fn display(&mut self) {
        println!("\n - - - - - - - - - - - - - - - -");
        println!("display final: {}", self.pile.len());
    }
}

#[derive(Clone, Copy)]
pub struct Report;

impl Report {
    pub fn new() -> Self {
        Self
    }
}

use std::ops::Add;

impl Add for Report {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {}
    }

}