#[macro_use]
extern crate clap;
extern crate cocaine_http_proxy;

use clap::{App, Arg};

use cocaine_http_proxy::Config;

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

    let thread = cocaine_http_proxy::util::run_dashboard(config.clone());
    cocaine_http_proxy::run(config).expect("failed to run the server");
    thread.join().unwrap().unwrap();
}
