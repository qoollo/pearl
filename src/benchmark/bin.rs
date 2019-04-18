#![feature(async_await, await_macro, futures_api)]
#![allow(clippy::needless_lifetimes)]
extern crate pearl;
#[macro_use]
extern crate log;

use clap::{App, Arg, *};
use futures::{executor::*, stream::*};
use std::time::Instant;

use log::LevelFilter;
use writer::Writer;
mod generator;
mod statistics;
mod writer;

use generator::Generator;
use statistics::Statistics;

fn main() {
    println!("{:_^41}", "PEARL_BENCHMARK");
    env_logger::Builder::new()
        .filter_module("benchmark", LevelFilter::Debug)
        .init();
    let mut pool = ThreadPool::new().unwrap();
    let spawner = pool.clone();
    let app = start_app(spawner);
    pool.run(app);
}

async fn start_app(spawner: ThreadPool) {
    info!("Hello Async World");
    info!("Prepare app matches");
    let matches = prepare_matches();

    info!("Create new generator");
    let mut generator = Generator::new(
        matches.value_of("value_size").unwrap().parse().unwrap(),
        matches.value_of("key_size").unwrap().parse().unwrap(),
        matches.value_of("limit").unwrap().parse().unwrap(),
        matches.value_of("pregen").unwrap().parse().unwrap(),
    );

    info!("Create new writer");
    let mut writer = Writer::new(
        matches.value_of("speed").unwrap().parse().unwrap(),
        matches.value_of("dst_dir").unwrap().parse().unwrap(),
    );

    info!("Init writer");
    await!(writer.init(spawner));

    info!("Create new statistics");
    let mut statistics = Statistics::new(matches.value_of("max_reports").unwrap().parse().unwrap());

    info!("Start write cycle");
    while let Some((key, value)) = await!(generator.next()) {
        let now = Instant::now();
        let mut report = await!(writer.write(key, value));
        report.set_latency(now);
        await!(statistics.add(report));
    }
    await!(statistics.display());
    writer.close();
}

fn prepare_matches<'a>() -> ArgMatches<'a> {
    App::new("benchmark")
        .arg(
            Arg::with_name("value_size")
                .short("v")
                .default_value("90000"),
        )
        .arg(Arg::with_name("key_size").short("k").default_value("100"))
        .arg(
            Arg::with_name("limit")
                .short("l")
                .default_value("100000000")
                .help("by default 100MB"),
        )
        .arg(Arg::with_name("pregen").short("p").default_value("0"))
        .arg(
            Arg::with_name("speed")
                .short("s")
                .default_value("0")
                .help("0 - unlimited"),
        )
        .arg(
            Arg::with_name("max_reports")
                .short("r")
                .default_value("100"),
        )
        .arg(Arg::with_name("dst_dir").long("dir").default_value("/tmp"))
        .get_matches()
}
