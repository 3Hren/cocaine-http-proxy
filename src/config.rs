use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use serde::Serializer;
use serde::de::{self, Deserialize, Deserializer};
use serde_yaml;

use cocaine::logging::Severity;

fn serialize_into_str<S>(severity: &Severity, se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    se.serialize_str(&format!("{}", severity))
}

fn deserialize_from_str<'de, D>(de: D) -> Result<Severity, D::Error>
    where D: Deserializer<'de>
{
    let s: String = Deserialize::deserialize(de)?;
    Severity::from_str(&s).map_err(de::Error::custom)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetworkConfig {
    addr: (IpAddr, u16),
    backlog: i32,
}

impl NetworkConfig {
    pub fn addr(&self) -> SocketAddr {
        let (addr, port) = self.addr;
        SocketAddr::new(addr, port)
    }

    pub fn backlog(&self) -> i32 {
        self.backlog
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LoggingBaseConfig {
    name: String,
    source: String,
    #[serde(serialize_with = "serialize_into_str", deserialize_with = "deserialize_from_str")]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MonitoringConfig {
    addr: (IpAddr, u16),
}

impl MonitoringConfig {
    pub fn addr(&self) -> SocketAddr {
        let (addr, port) = self.addr;
        SocketAddr::new(addr, port)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ServicePoolConfig {
    limit: usize,
    lifespan: u64,
    reconnection_ratio: f64,
}

impl ServicePoolConfig {
    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn lifespan(&self) -> u64 {
        self.lifespan
    }

    pub fn reconnection_ratio(&self) -> f64 {
        self.reconnection_ratio
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct DetailPoolConfig {
    limit: Option<usize>,
    lifespan: Option<u64>,
    reconnection_ratio: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PoolConfig {
    limit: usize,
    lifespan: u64,
    reconnection_ratio: f64,
    services: HashMap<String, DetailPoolConfig>,
}

impl PoolConfig {
    pub fn config(&self, name: &str) -> ServicePoolConfig {
        match self.services.get(name) {
            Some(cfg) => {
                ServicePoolConfig {
                    limit: cfg.limit.unwrap_or(self.limit),
                    lifespan: cfg.lifespan.unwrap_or(self.lifespan),
                    reconnection_ratio: cfg.reconnection_ratio.unwrap_or(self.reconnection_ratio),
                }
            }
            None => {
                ServicePoolConfig {
                    limit: self.limit,
                    lifespan: self.lifespan,
                    reconnection_ratio: self.reconnection_ratio,
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    network: NetworkConfig,
    threads: Option<usize>,
    locators: Vec<(IpAddr, u16)>,
    logging: LoggingConfig,
    monitoring: MonitoringConfig,
    pool: PoolConfig,
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

    pub fn pool(&self) -> &PoolConfig {
        &self.pool
    }
}
