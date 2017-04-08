#![feature(box_syntax, fnbox)]

extern crate rand;
extern crate log;

#[macro_use]
extern crate clap;
extern crate futures;
extern crate rmp_serde as rmps;
extern crate rmpv;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;

#[macro_use(o, slog_log, slog_info, slog_warn)]
extern crate slog;
extern crate slog_term;
extern crate tokio_core;
extern crate tokio_minihttp;
extern crate tokio_proto;
extern crate tokio_service;
extern crate itertools;
extern crate net2;
extern crate hyper;

#[macro_use]
extern crate cocaine;

use clap::{App, Arg};

use config::Config;

mod config;
mod monitoring;
mod server;

fn main() {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .arg(Arg::with_name("config")
                 .short("c")
                 .long("config")
                 .required(true)
                 .value_name("FILE")
                 .help("Path to the configuration file")
                 .takes_value(true))
        .get_matches();

    let path = matches.value_of("config").expect("failed to extract configuration path");

    let config = match Config::load(path) {
        Ok(path) => path,
        Err(err) => {
            println!("ERROR: failed to load configuration: {}", err);
            std::process::exit(1);
        }
    };

    server::run(config).expect("failed to run the server");
}
