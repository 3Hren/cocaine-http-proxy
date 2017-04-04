use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::net::IpAddr;
use std::str::FromStr;

use serde_yaml;
use serde::de::{self, Deserialize, Deserializer};

use cocaine::logging::Severity;

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
            return Err("number of HTTP threads must be a positive value (or absent)".into());
        }

        Ok(())
    }
}

fn deserialize_from_str<D>(de: D) -> Result<Severity, D::Error>
    where D: Deserializer
{
    let s: String = Deserialize::deserialize(de)?;
    Severity::from_str(&s).map_err(de::Error::custom)
}

#[derive(Deserialize)]
pub struct LoggingConfig {
    name: String,
    prefix: String,
    #[serde(deserialize_with = "deserialize_from_str")]
    severity: Severity,
}

impl LoggingConfig {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }
}

#[derive(Deserialize)]
pub struct Config {
    addr: IpAddr,
    port: u16,
    threads: ThreadConfig,
    locators: Vec<(IpAddr, u16)>,
    logging: LoggingConfig,
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

    pub fn logging(&self) -> &LoggingConfig {
        &self.logging
    }
}
