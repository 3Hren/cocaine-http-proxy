#![feature(box_syntax)]

extern crate rand;
#[macro_use] extern crate log;

#[macro_use] extern crate clap;
extern crate futures;
#[macro_use] extern crate serde_derive;
extern crate serde_yaml;
extern crate tokio_core;
extern crate tokio_minihttp;
extern crate tokio_proto;
extern crate tokio_service;

extern crate rmp_serde as rmps;
extern crate rmpv;
extern crate cocaine;

use clap::{App, Arg};

use config::Config;

mod config;
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

    let path = matches.value_of("config")
        .expect("failed to extract configuration path");

    let config = Config::from(path)
        .expect("failed to read configuration file");

    server::run(config)
        .expect("failed to run the server");
}
