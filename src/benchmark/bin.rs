#![feature(async_await, await_macro, futures_api)]
extern crate pearl;

mod generator;
mod statistics;
mod writer;

use clap::App;
use futures::{executor::*, stream::*};

use generator::Generator;
use statistics::Statistics;
use writer::Writer;

fn main() {
    let mut pool = ThreadPool::new().unwrap();
    let spawner = pool.clone();
    let app = start_app(spawner);
    pool.run(app);
}

async fn start_app<S>(spawner: S) {
    let matches = App::new("benchmark");
    println!("Hello Async World");
    let mut generator = Generator::new();
    let writer = Writer::new();
    let statistics = Statistics::new();
    while let Some((key, value)) = await!(generator.next()) {
        let report = await!(writer.write());
        await!(statistics.add(report));
    }
}
