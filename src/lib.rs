//! Roadmap:
//! - [x] code style.
//! - [x] GET config.
//! - [x] decomposition.
//! - [x] basic metrics: counters, rates.
//! - [x] enable application services.
//! - [x] smart reconnection in the pool.
//! - [x] RG support for immediate updates.
//! - [x] pass pool settings from config.
//! - [x] fixed-size pool balancing.
//! - [x] unicorn support for tracing.
//! - [x] unicorn support for timeouts.
//! - [x] headers in the framework.
//! - [x] tracing.
//! - [x] retry policy for applications.
//! - [x] request timeouts.
//! - [ ] timeouts.
//! - [ ] forward Authorization header.
//! - [ ] chunked transfer encoding.
//! - [ ] clean code.
//! - [ ] metrics: histograms.
//! - [ ] JSON RPC.
//! - [ ] MDS direct.
//! - [ ] Streaming logging through HTTP.
//! - [ ] plugin system.
//! - [ ] logging review.
//! - [ ] support Cocaine authentication.
//! - [ ] count 2xx, 4xx, 5xx HTTP codes.

#![feature(box_syntax, fnbox, integer_atomics)]

extern crate byteorder;
#[macro_use]
extern crate cocaine;
extern crate console;
extern crate futures;
#[macro_use]
extern crate hyper;
extern crate itertools;
extern crate log;
extern crate net2;
extern crate num_cpus;
extern crate rand;
extern crate rmp_serde as rmps;
extern crate rmpv;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate time;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate uuid;

use std::error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use futures::Future;
use futures::sync::mpsc;
use serde::Serializer;
use serde::ser::SerializeMap;

use cocaine::{Builder, Core};
use cocaine::logging::Severity;
use cocaine::service::{Locator, Unicorn};

use self::metrics::{Count, Counter, Meter, RateMeter};
pub use self::config::Config;
use self::logging::{Loggers};
use self::pool::{SubscribeTask, Event, EventDispatch, RoutingGroupsUpdateTask};
use self::route::{AppRoute, PerfRoute, Router};
use self::server::{ServerConfig, ServerGroup};
use self::service::cocaine::ProxyServiceFactoryFactory;
use self::service::monitor::MonitorServiceFactoryFactory;

mod config;
mod logging;
mod metrics;
mod pool;
mod route;
mod server;
mod service;
pub mod util;

const THREAD_NAME_PERIODIC: &str = "periodic";

type ServiceBuilder<T> = Builder<T>;

#[derive(Debug, Default, Serialize)]
struct ConnectionMetrics {
    #[serde(serialize_with = "serialize_counter")]
    active: Counter,
    #[serde(serialize_with = "serialize_counter")]
    accepted: Counter,
}

fn serialize_counter<S>(counter: &Counter, se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    se.serialize_i64(counter.get())
}

fn serialize_meter<S>(meter: &RateMeter, se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    let mut map = se.serialize_map(Some(4))?;
    map.serialize_key("count")?;
    map.serialize_value(&meter.count())?;
    map.serialize_key("m01rate")?;
    map.serialize_value(&meter.m01rate())?;
    map.serialize_key("m05rate")?;
    map.serialize_value(&meter.m05rate())?;
    map.serialize_key("m15rate")?;
    map.serialize_value(&meter.m15rate())?;
    map.end()
}

#[derive(Debug, Default, Serialize)]
pub struct Metrics {
    connections: ConnectionMetrics,
    #[serde(serialize_with = "serialize_meter")]
    requests: RateMeter,
}

pub fn run(config: Config) -> Result<(), Box<error::Error>> {
    let locator_addrs = config.locators()
        .iter()
        .map(|&(addr, port)| SocketAddr::new(addr, port))
        .collect::<Vec<SocketAddr>>();

    let logging = Loggers::from(config.logging());
    let metrics = Arc::new(Metrics::default());

    cocaine_log!(logging.common(), Severity::Debug, "starting Cocaine HTTP Proxy with {:?}", config);

    // Here we create several event channels that will deliver control events to services pools.
    // We could create a separate thread pool for processing Cocaine invocation events with their
    // own event loops, but it appeared that having common thread pool with both HTTP events and
    // Cocaine one gives more RPS with lower latency.
    let (txs, rxs) = itertools::repeat_call(|| mpsc::unbounded())
        .take(config.threads())
        .unzip();

    let dispatch = EventDispatch::new(txs);

    // Start all periodic jobs in a separate thread that will produce control events for pools.
    let thread: JoinHandle<Result<(), io::Error>> = {
        let cfg = config.clone();
        let log = logging.common().clone();
        let dispatch = dispatch.clone();
        thread::Builder::new().name(THREAD_NAME_PERIODIC.into()).spawn(move || {
            let mut core = Core::new()?;
            let locator = ServiceBuilder::new("locator")
                .locator_addrs(locator_addrs.clone())
                .build(&core.handle());
            let unicorn = ServiceBuilder::new(cfg.unicorn().to_owned())
                .locator_addrs(locator_addrs)
                .build(&core.handle());

            let future = RoutingGroupsUpdateTask::new(core.handle(), Locator::new(locator), dispatch.clone(), log.clone());
            let tracing = {
                let log = log.clone();
                let dispatch = dispatch.clone();

                SubscribeTask::new(
                    cfg.tracing().path().into(),
                    Unicorn::new(unicorn.clone()),
                    log.clone(),
                    core.handle(),
                    move |tracing| {
                        cocaine_log!(log, Severity::Info, "updated tracing config with {} entries", tracing.len());
                        dispatch.send_all(|| Event::OnTracingUpdates(tracing.clone()));
                    }
                )
            };
            core.run(future.join(tracing)).unwrap();

            Ok(())
        })?
    };

    let mut router = Router::new();
    router.add(Arc::new(AppRoute::new(dispatch.clone(), config.tracing().header().into(), logging.access().clone())));
    router.add(Arc::new(PerfRoute::new(dispatch.clone(), logging.access().clone())));

    let factory = ProxyServiceFactoryFactory::new(
        dispatch.into_senders(),
        rxs,
        logging.common().clone(),
        metrics.clone(),
        router,
        config.clone()
    );

    let proxy_cfg = ServerConfig::new(config.network().addr())
        .backlog(config.network().backlog())
        .threads(config.threads());
    let monitoring_cfg = ServerConfig::new(config.monitoring().addr())
        .godfather(|id| format!("monitor {:02}", id));

    cocaine_log!(logging.common(), Severity::Info, "started HTTP proxy at {}", config.network().addr());
    ServerGroup::new()?
        .expose(proxy_cfg, factory)?
        .expose(monitoring_cfg, MonitorServiceFactoryFactory::new(Arc::new(config), metrics))?
        .run()?;

    thread.join().unwrap()?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::THREAD_NAME_PERIODIC;

    #[test]
    fn test_thread_names_fit_in_system_bounds() {
        // For NPTL the thread name is a meaningful C language string, whose length is restricted
        // to 16 characters, including the terminating null byte ('\0').
        assert!(THREAD_NAME_PERIODIC.len() < 16);
    }
}
