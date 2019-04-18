#![feature(async_await, await_macro, futures_api)]
#![allow(clippy::needless_lifetimes)]
extern crate pearl;

mod generator;
mod statistics;
mod writer;

use clap::{App, Arg, *};
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

async fn start_app(spawner: ThreadPool) {
    println!("Hello Async World");
    println!("Prepare app matches");
    let matches = prepare_matches();

    println!("Create new generator");
    let mut generator = Generator::new(
        matches.value_of("value_size").unwrap().parse().unwrap(),
        matches.value_of("key_size").unwrap().parse().unwrap(),
        matches.value_of("memory").unwrap().parse().unwrap(),
        matches.value_of("pregen").unwrap().parse().unwrap(),
    );

    println!("Create new writer");
    let mut writer = Writer::new(matches.value_of("speed").unwrap().parse().unwrap());

    println!("Init writer");
    await!(writer.init(spawner));

    println!("Create new statistics");
    let statistics = Statistics::new();

    println!("Start write cycle");
    while let Some((key, value)) = await!(generator.next()) {
        let report = await!(writer.write(key, value));
        await!(statistics.add(report));
    }
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
            Arg::with_name("memory")
                .short("m")
                .default_value("1000000000"),
        )
        .arg(Arg::with_name("pregen").short("p").default_value("0"))
        .arg(
            Arg::with_name("speed")
                .short("s")
                .default_value("0")
                .help("0 - unlimited"),
        )
        .get_matches()
}
