#![feature(async_await, await_macro)]
#![allow(clippy::needless_lifetimes)]

extern crate pearl;
#[macro_use]
extern crate log;

mod generator;
mod statistics;
mod writer;

use clap::{App, Arg, ArgMatches};
use futures::{
    channel::mpsc::{channel, Sender},
    executor::ThreadPool,
    stream::{FuturesUnordered, StreamExt},
};
use log::LevelFilter;
use pearl::Key;
use std::{sync::Arc, time::Instant};

use generator::Generator;
use statistics::Statistics;
use writer::Writer;

fn main() {
    println!("{:_^41}", "PEARL_BENCHMARK");
    env_logger::Builder::new()
        .filter_module("benchmark", LevelFilter::Debug)
        .filter_module("pearl", LevelFilter::Error)
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
    let limit = matches.value_of("limit").unwrap().parse().unwrap();
    let value_size_kb: usize = matches.value_of("value_size").unwrap().parse().unwrap();
    let mut generator = Generator::new(value_size_kb * 1000, limit);

    info!("Create new writer");
    let mut writer = Writer::new(
        matches.value_of("dst_dir").unwrap().parse().unwrap(),
        matches.value_of("max_size").unwrap().parse().unwrap(),
        matches.value_of("max_data").unwrap().parse().unwrap(),
    );

    info!("Init writer");
    await!(writer.init(spawner.clone()));

    info!("Create new statistics");
    let mut statistics = Statistics::new(matches.value_of("max_reports").unwrap().parse().unwrap());

    info!("Start write cycle");
    let (tx, rx) = channel::<statistics::Report>(limit);
    let awriter = Arc::new(writer);
    let mut counter = 0;

    let futures_limit = matches.value_of("futures_limit").unwrap().parse().unwrap();

    let prepared: Vec<_> = (0..futures_limit)
        .map(|_| generator.next().unwrap())
        .collect();

    use statistics::Report;

    async fn write(
        lawriter: Arc<Writer<Key128>>,
        key: Key128,
        data: Vec<u8>,
        mut ltx: Sender<Report>,
    ) {
        let now = Instant::now();
        let mut report = await!(lawriter.write(key, data));
        report.set_latency(now);
        ltx.try_send(report).unwrap();
    }

    let mut futures_pool: FuturesUnordered<_> = prepared
        .into_iter()
        .map(|(key, data)| {
            let lawriter = awriter.clone();
            let ltx = tx.clone();
            counter += 1;
            write(lawriter, key.into(), data, ltx)
        })
        .collect();
    println!(
        "{:<10}{:<10}{:<10}{:<10}{:<10}",
        "Completed", "Active", "Limit", "Total", "%"
    );
    let write_limit = limit * 1000 / value_size_kb;
    let mut prev_p = 0;
    while let Some(_) = await!(futures_pool.next()) {
        let percent = counter * 1000 / write_limit;
        if prev_p != percent {
            print!(
                "\r{:<10}{:<10}{:<10}{:<10}{:<10}",
                counter,
                futures_pool.len(),
                futures_limit,
                write_limit,
                percent / 10
            );
            if percent % 50 == 0 {
                println!();
            }
        }
        prev_p = percent;
        if futures_pool.len() < futures_limit as usize {
            if let Some((key, data)) = generator.next() {
                let lawriter = awriter.clone();
                let ltx = tx.clone();
                counter += 1;
                futures_pool.push(write(lawriter, key.into(), data, ltx));
            }
        }
    }

    info!("start await ");
    await!(rx
        .take(counter as u64)
        .map(|r| statistics.add(r))
        .collect::<Vec<_>>());
    info!("end await ");
    await!(statistics.display());
    awriter.close();
}

fn prepare_matches<'a>() -> ArgMatches<'a> {
    App::new("benchmark")
        .arg(
            Arg::with_name("value_size")
                .short("v")
                .default_value("90")
                .help("KB, by default 90"),
        )
        .arg(
            Arg::with_name("limit")
                .short("l")
                .default_value("100")
                .help("MB, by default 100"),
        )
        .arg(
            Arg::with_name("max_reports")
                .short("m")
                .default_value("0")
                .help("0 - unlimited"),
        )
        .arg(Arg::with_name("dst_dir").short("d").default_value("/tmp"))
        .arg(
            Arg::with_name("max_size")
                .short("s")
                .default_value("1000")
                .help("MB, limit of the blob file size"),
        )
        .arg(
            Arg::with_name("max_data")
                .short("x")
                .default_value("1000")
                .help("MB, limit of the records number in blob"),
        )
        .arg(
            Arg::with_name("futures_limit")
                .long("futures")
                .default_value("10"),
        )
        .get_matches()
}

struct Key128(Vec<u8>);

impl Key for Key128 {
    const LEN: u16 = 8;
}

impl From<Vec<u8>> for Key128 {
    fn from(v: Vec<u8>) -> Self {
        assert_eq!(Self::LEN as usize, v.len());
        Self(v)
    }
}

impl AsRef<[u8]> for Key128 {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}