use super::prelude::*;

pub struct Statistics {
    max_reports: usize,
    pile: Vec<Report>,
    start: Instant,
    last: Instant,
}

impl Statistics {
    pub fn new(max_reports: usize) -> Self {
        Self {
            max_reports,
            pile: Vec::new(),
            start: Instant::now(),
            last: Instant::now(),
        }
    }

    pub fn add(&mut self, report: Report) {
        self.pile.push(report);
        if self.pile.len() >= self.max_reports && self.max_reports != 0 {
            self.merge();
        }
        self.last = Instant::now();
    }

    pub fn merge(&mut self) {
        let new_pile: Vec<_> = self
            .pile
            .chunks(2)
            .map(|p| if p.len() == 2 { p[0] + p[1] } else { p[0] })
            .collect();
        debug!(
            "merge: before {}, after {} ",
            self.pile.len(),
            new_pile.len()
        );
        self.pile = new_pile;
    }

    pub fn display(&mut self) {
        println!("\n\n{:-^40}", "RESULTS");
        let total_count = self.pile.iter().fold(0, |acc, r| acc + r.count);
        Self::print("reports total:", total_count);
        Self::print("reports collected:", self.pile.len());
        let test_duration_ms = (self.last - self.start).as_secs_f64() * 1000.0;
        Self::print("test duration:", test_duration_ms / 1000.0);
        let total_size = self
            .pile
            .iter()
            .fold(0, |acc, rec| acc + rec.key_len + rec.value_len) as f64;
        Self::print("written total, MB:", total_size / 1_000_000.0);
        let rate = total_size / test_duration_ms;
        Self::print("rate, MB/s:", rate / 1_000.0);
        Self::print(
            "rate, recs/s",
            total_count as f64 * 1_000.0 / test_duration_ms,
        );
        let avg_latency = self
            .pile
            .iter()
            .map(|rec| rec.count as u32 * rec.latency)
            .fold(Duration::from_millis(0), |acc, x| acc + x)
            / total_count as u32;
        Self::print("avg latency:", avg_latency);
    }

    fn print<T>(name: &str, value: T)
    where
        T: std::fmt::Debug,
    {
        println!("{:>5}{:<20}{:>10.3?}", "", name, value);
    }
}

#[derive(Clone, Copy)]
pub struct Report {
    count: usize,
    timestamp: Instant,
    value_len: usize,
    key_len: usize,
    latency: Duration,
}

impl Report {
    pub fn new(key_len: usize, value_len: usize) -> Self {
        Self {
            count: 1,
            timestamp: Instant::now(),
            value_len,
            key_len,
            latency: Duration::default(),
        }
    }

    pub fn set_latency(&mut self, start: Instant) {
        self.latency = Instant::now() - start;
    }
}

impl Add for Report {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            count: self.count + rhs.count,
            timestamp: self.timestamp,
            value_len: self.value_len + rhs.value_len,
            key_len: self.key_len + rhs.key_len,
            latency: ((self.count as u32 * self.latency) + (rhs.count as u32 * self.latency))
                / (self.count + rhs.count) as u32,
        }
    }
}
