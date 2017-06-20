//! Configuration mapping.

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use num_cpus;
use serde::Serializer;
use serde::de::{self, Deserialize, Deserializer};
use serde_yaml;

use cocaine::logging::Severity;

fn serialize_into_str<S>(severity: &Severity, se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    se.serialize_str(&severity.to_string())
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
pub struct TracingConfig {
    path: String,
    header: String,
    probability: f64,
}

impl TracingConfig {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn header(&self) -> &str {
        &self.header
    }

    pub fn probability(&self) -> f64 {
        self.probability
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimeoutsConfig {
    path: String,
}

impl TimeoutsConfig {
    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LoadTestingConfig {
    enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    network: NetworkConfig,
    threads: Option<usize>,
    locators: Vec<(IpAddr, u16)>,
    logging: LoggingConfig,
    unicorn: String,
    monitoring: MonitoringConfig,
    pool: PoolConfig,
    tracing: TracingConfig,
    timeout: u64,
    timeouts: TimeoutsConfig,
    load_testing: Option<LoadTestingConfig>,
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

        if cfg.tracing.probability < 0.0 || cfg.tracing.probability > 1.0 {
            return Err("tracing probability must fit in [0.0; 1.0]".into());
        }

        Ok(())
    }

    pub fn network(&self) -> &NetworkConfig {
        &self.network
    }

    /// Returns the number of worker threads.
    ///
    /// Can be omitted in the config, in that case the number of CPUs of the current machine will
    /// be returned.
    pub fn threads(&self) -> usize {
        self.threads.unwrap_or(num_cpus::get())
    }

    pub fn locators(&self) -> &Vec<(IpAddr, u16)> {
        &self.locators
    }

    pub fn logging(&self) -> &LoggingConfig {
        &self.logging
    }

    /// Returns the name of the Unicorn service that will be used for dynamic configuration and
    /// subscriptions.
    pub fn unicorn(&self) -> &str {
        &self.unicorn
    }

    pub fn monitoring(&self) -> &MonitoringConfig {
        &self.monitoring
    }

    pub fn pool(&self) -> &PoolConfig {
        &self.pool
    }

    pub fn tracing(&self) -> &TracingConfig {
        &self.tracing
    }

    /// Returns proxy timeout.
    pub fn timeout(&self) -> Duration {
        Duration::new(self.timeout, 0)
    }

    pub fn timeouts(&self) -> &TimeoutsConfig {
        &self.timeouts
    }

    /// Returns `true` when a load testing plugin is enabled.
    pub fn is_load_testing_enabled(&self) -> bool {
        self.load_testing.as_ref().map(|v| v.enabled).unwrap_or(false)
    }
}
