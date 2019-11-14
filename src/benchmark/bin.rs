#![allow(clippy::needless_lifetimes)]
// #![warn(clippy::pedantic)]

extern crate pearl;
#[macro_use]
extern crate log;

mod generator;
mod statistics;
mod writer;

mod prelude {
    pub(crate) use super::{
        generator::Generator,
        statistics::{Report, Statistics},
        writer::Writer,
    };
    pub(crate) use chrono::Local;
    pub(crate) use clap::{App, Arg, ArgMatches};
    pub(crate) use env_logger::fmt::Color;
    pub(crate) use futures::{
        channel::mpsc::{channel, Sender},
        stream::{FuturesUnordered, StreamExt},
    };
    pub(crate) use log::{Level, LevelFilter};
    pub(crate) use pearl::{Builder, Key, Storage};
    pub(crate) use rand::{rngs::ThreadRng, RngCore};
    pub(crate) use std::{
        io::Write,
        ops::Add,
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, Instant},
    };
}

use prelude::*;

#[tokio::main]
async fn main() {
    println!("{:_^41}", "PEARL_BENCHMARK");
    env_logger::Builder::new()
        .format(|buf, record: &log::Record| {
            let mut style = buf.style();
            let color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Cyan,
                Level::Trace => Color::White,
            };
            style.set_color(color);
            writeln!(
                buf,
                "[{} {:>24}:{:^4} {:^5}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap_or(""),
                record.line().unwrap_or(0),
                style.value(record.level()),
                record.args(),
            )
        })
        .filter_module("benchmark", LevelFilter::Info)
        .filter_module("pearl", LevelFilter::Info)
        .init();
    start_app().await;
}

async fn write(lawriter: Arc<Writer<Key128>>, key: Key128, data: Vec<u8>, mut ltx: Sender<Report>) {
    let now = Instant::now();
    let mut report = lawriter.write(key, data).await;
    report.set_latency(now);
    ltx.try_send(report).unwrap();
}

async fn start_app() {
    info!("Hello Async World");
    info!("Prepare app matches");
    let matches = prepare_matches();

    info!("Create new generator");
    let limit = matches.value_of("limit").unwrap().parse().unwrap();
    let value_size_kb: usize = matches.value_of("value_size").unwrap().parse().unwrap();
    let mut generator = Generator::new(value_size_kb * 1000, limit);

    info!("Create new writer");
    let mut writer = Writer::new(
        &matches
            .value_of("dst_dir")
            .unwrap()
            .parse::<PathBuf>()
            .unwrap(),
        matches.value_of("max_size").unwrap().parse().unwrap(),
        matches.value_of("max_data").unwrap().parse().unwrap(),
    );

    info!("Init writer");
    writer.init().await;

    info!("Create new statistics");
    let mut statistics = Statistics::new(matches.value_of("max_reports").unwrap().parse().unwrap());

    info!("Start write cycle");
    let (tx, rx) = channel::<statistics::Report>(limit);
    let writer = Arc::new(writer);
    let mut counter = 0;

    let futures_limit: usize = matches.value_of("futures_limit").unwrap().parse().unwrap();

    let prepared: Vec<_> = (0..futures_limit)
        .map(|_| generator.next().unwrap())
        .collect();

    let mut futures_pool: FuturesUnordered<_> = prepared
        .into_iter()
        .map(|(key, data)| {
            let lawriter = writer.clone();
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
    while let Some(_) = futures_pool.next().await {
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
        if futures_pool.len() < futures_limit {
            if let Some((key, data)) = generator.next() {
                let lawriter = writer.clone();
                let ltx = tx.clone();
                counter += 1;
                futures_pool.push(write(lawriter, key.into(), data, ltx));
            }
        }
    }

    info!("start await ");
    let _ = rx
        .take(counter)
        .map(|r| statistics.add(r))
        .collect::<Vec<_>>()
        .await;
    info!("end await ");
    statistics.display().await;
    writer.close().await;
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

#[derive(Debug)]
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
