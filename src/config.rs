use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::net::IpAddr;

use serde_yaml;

#[derive(Deserialize)]
pub struct ThreadConfig {
    http: Option<usize>,
    network: usize,
}

impl ThreadConfig {
    pub fn http(&self) -> usize {
        self.http.unwrap_or(1)
    }

    pub fn network(&self) -> usize {
        self.network
    }

    fn sanitize(&self) -> Result<(), Box<Error>> {
        if let Some(0) = self.http {
            return Err("number of HTTP threads must be positive value (or absent)".into());
        }

        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Config {
    addr: IpAddr,
    port: u16,
    threads: ThreadConfig,
    locators: Vec<(IpAddr, u16)>,
}

impl Config {
    pub fn from<P: AsRef<Path>>(path: P) -> Result<Config, Box<Error>> {
        let cfg = serde_yaml::from_reader(&File::open(path)?)?;

        Config::sanitize(&cfg)?;

        Ok(cfg)
    }

    fn sanitize(cfg: &Config) -> Result<(), Box<Error>> {
        cfg.threads().sanitize()?;
        Ok(())
    }

    pub fn addr(&self) -> &IpAddr {
        &self.addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn threads(&self) -> &ThreadConfig {
        &self.threads
    }

    pub fn locators(&self) -> &Vec<(IpAddr, u16)> {
        &self.locators
    }
}
