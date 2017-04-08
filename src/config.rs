use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::net::IpAddr;
use std::str::FromStr;

use serde_yaml;
use serde::de::{self, Deserialize, Deserializer};

use cocaine::logging::Severity;

fn deserialize_from_str<D>(de: D) -> Result<Severity, D::Error>
    where D: Deserializer
{
    let s: String = Deserialize::deserialize(de)?;
    Severity::from_str(&s).map_err(de::Error::custom)
}

#[derive(Clone, Debug, Deserialize)]
pub struct NetworkConfig {
    addr: (IpAddr, u16),
    backlog: i32,
}

impl NetworkConfig {
    pub fn addr(&self) -> &(IpAddr, u16) {
        &self.addr
    }

    pub fn backlog(&self) -> i32 {
        self.backlog
    }
}

#[derive(Deserialize)]
pub struct LoggingBaseConfig {
    name: String,
    source: String,
    #[serde(deserialize_with = "deserialize_from_str")]
    severity: Severity,
}

impl LoggingBaseConfig {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }
}

#[derive(Deserialize)]
pub struct LoggingConfig {
    common: LoggingBaseConfig,
    access: LoggingBaseConfig,
}

impl LoggingConfig {
    pub fn common(&self) -> &LoggingBaseConfig {
        &self.common
    }

    pub fn access(&self) -> &LoggingBaseConfig {
        &self.access
    }
}

#[derive(Debug, Deserialize)]
pub struct MonitoringConfig {
    addr: (IpAddr, u16),
}

impl MonitoringConfig {
    pub fn addr(&self) -> &(IpAddr, u16) {
        &self.addr
    }
}

#[derive(Deserialize)]
pub struct Config {
    network: NetworkConfig,
    threads: Option<usize>,
    locators: Vec<(IpAddr, u16)>,
    logging: LoggingConfig,
    monitoring: MonitoringConfig,
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Config, Box<Error>> {
        let cfg = serde_yaml::from_reader(&File::open(path)?)?;

        Config::sanitize(&cfg)?;

        Ok(cfg)
    }

    fn sanitize(cfg: &Config) -> Result<(), Box<Error>> {
        if let Some(0) = cfg.threads {
            return Err("number of worker threads must be a positive value (or absent)".into());
        }

        Ok(())
    }

    pub fn network(&self) -> &NetworkConfig {
        &self.network
    }

    pub fn threads(&self) -> usize {
        self.threads.unwrap_or(1)
    }

    pub fn locators(&self) -> &Vec<(IpAddr, u16)> {
        &self.locators
    }

    pub fn logging(&self) -> &LoggingConfig {
        &self.logging
    }

    pub fn monitoring(&self) -> &MonitoringConfig {
        &self.monitoring
    }
}
