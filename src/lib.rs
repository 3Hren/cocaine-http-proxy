#![feature(box_syntax, fnbox, integer_atomics, never_type)]

#[macro_use]
extern crate cocaine;
extern crate futures;
#[macro_use]
extern crate hyper;
extern crate itertools;
extern crate jsonrpc_core;
extern crate libc;
extern crate net2;
extern crate num_cpus;
extern crate rand;
extern crate regex;
extern crate rmp_serde as rmps;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate time;
extern crate tokio_core;
extern crate tokio_service;
extern crate uuid;

use std::collections::HashMap;
use std::error;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures::Future;
use futures::sync::mpsc;
use serde::Serializer;
use serde::ser::SerializeMap;

use cocaine::{Core, ServiceBuilder};
use cocaine::logging::Severity;
use cocaine::service::{Locator, Tvm, Unicorn};
use cocaine::service::tvm::Grant;

pub use self::config::Config;
use self::logging::Loggers;
use self::metrics::{Count, Counter, Meter, RateMeter};
use self::pool::{Event, EventDispatch, RoutingGroupsAction, SubscribeAction, TicketFactory};
use self::retry::Retry;
use self::route::{AppRoute, JsonRpc, PerfRoute, Router};
use self::server::{ServerConfig, ServerGroup};
use self::service::cocaine::ProxyServiceFactoryFactory;
use self::service::monitor::MonitorServiceFactoryFactory;

mod common;
mod config;
mod logging;
mod metrics;
mod net;
mod pool;
mod retry;
pub mod route;
mod server;
mod service;

const DEFAULT_LOCATOR_NAME: &str = "locator";
const THREAD_NAME_PERIODIC: &str = "periodic";

#[derive(Debug, Default, Serialize)]
struct ConnectionMetrics {
    #[serde(serialize_with = "serialize_counter")]
    active: Counter,
    #[serde(serialize_with = "serialize_counter")]
    accepted: Counter,
}

fn serialize_counter<S>(counter: &Counter, se: S) -> Result<S::Ok, S::Error>
where
    S: Serializer
{
    se.serialize_i64(counter.get())
}

fn serialize_meter<S>(meter: &RateMeter, se: S) -> Result<S::Ok, S::Error>
where
    S: Serializer
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
struct ResponseMetrics {
    #[serde(serialize_with = "serialize_meter")]
    c5xx: RateMeter,
}

#[derive(Debug, Default, Serialize)]
pub struct Metrics {
    connections: ConnectionMetrics,
    #[serde(serialize_with = "serialize_meter")]
    requests: RateMeter,
    responses: ResponseMetrics,
}

pub fn run(config: Config) -> Result<(), Box<error::Error>> {
    let locator_addrs = config.locators()
        .iter()
        .map(|&(addr, port)| SocketAddr::new(addr, port))
        .collect::<Vec<SocketAddr>>();

    let logging = Loggers::from(config.logging());
    let metrics = Arc::new(Metrics::default());

    cocaine_log!(logging.common().logger(), Severity::Debug, "starting Cocaine HTTP Proxy with {:?}", config);

    // Here we create several event channels that will deliver control events to services pools.
    // We could create a separate thread pool for processing Cocaine invocation events with their
    // own event loops, but it appeared that having common thread pool with both HTTP events and
    // Cocaine one gives more RPS with lower latency.
    let (txs, rxs): (Vec<_>, Vec<_>) = itertools::repeat_call(|| mpsc::unbounded())
        .take(config.threads())
        .unzip();

    let dispatch = EventDispatch::new(txs);

    // Start all periodic jobs in a separate thread that will produce control events for pools.
    let thread: JoinHandle<Result<(), io::Error>> = {
        let cfg = config.clone();
        let log = logging.common().logger().clone();
        let dispatch = dispatch.clone();
        thread::Builder::new().name(THREAD_NAME_PERIODIC.into()).spawn(move || {
            let mut core = Core::new()?;

            let tvm = ServiceBuilder::new(cfg.auth().service().to_owned())
                .locator_addrs(locator_addrs.clone())
                .build(&core.handle());

            let locator = ServiceBuilder::new(DEFAULT_LOCATOR_NAME)
                .locator_addrs(locator_addrs.clone())
                .build(&core.handle());

            let unicorn = ServiceBuilder::new(cfg.unicorn().to_owned())
                .locator_addrs(locator_addrs)
                .build(&core.handle());

            let exponential_backoff = |v| Duration::from_secs(2u64.pow(std::cmp::min(6, v)));

            let groups = {
                let action = RoutingGroupsAction::new(Locator::new(locator), dispatch.clone(), log.clone());
                Retry::new(action, (0..).map(&exponential_backoff), core.handle())
            };

            let on_tracing = {
                let log = log.clone();
                let dispatch = dispatch.clone();
                move |tracing: HashMap<String, f64>| {
                    cocaine_log!(log, Severity::Info, "updated tracing config with {} entries", tracing.len());
                    dispatch.send_all(|| Event::OnTracingUpdates(tracing.clone()));
                }
            };

            let tm = TicketFactory::new(
                Tvm::new(tvm),
                cfg.auth().client_id(),
                cfg.auth().client_secret().to_owned(),
                Grant::ClientCredentials
            );

            let tracing = {
                let action = SubscribeAction::new(
                    cfg.tracing().path().into(),
                    tm.clone(),
                    Unicorn::new(unicorn.clone()),
                    &on_tracing,
                    log.clone()
                );
                Retry::new(action, (0..).map(&exponential_backoff), core.handle())
            };

            let on_timeouts = {
                let log = log.clone();
                let dispatch = dispatch.clone();
                move |timeouts: HashMap<String, f64>| {
                    cocaine_log!(log, Severity::Info, "updated timeout config with {} entries", timeouts.len());
                    dispatch.send_all(|| Event::OnTimeoutUpdates(timeouts.clone()));
                }
            };

            let timeouts = {
                let action = SubscribeAction::new(
                    cfg.timeouts().path().into(),
                    tm,
                    Unicorn::new(unicorn.clone()),
                    &on_timeouts,
                    log.clone()
                );
                Retry::new(action, (0..).map(&exponential_backoff), core.handle())
            };

            core.run(groups.join3(tracing, timeouts))
                .map_err(|err| io::Error::new(ErrorKind::Other, err.to_string()))?;

            Ok(())
        })?
    };

    let mut router = Router::new();
    router.add(Arc::new(AppRoute::new(dispatch.clone(), logging.access().logger().clone())
        .with_tracing_header(config.tracing().header().to_owned())
        .with_headers_mapping(config.headers().clone())
    ));
    router.add(Arc::new(JsonRpc::new(dispatch.clone(), logging.access().logger().clone())));

    if config.is_load_testing_enabled() {
        router.add(Arc::new(PerfRoute::new(dispatch.clone(), logging.access().logger().clone())));
        cocaine_log!(logging.common().logger(), Severity::Debug, "enabled performance measuring route");
    }

    let factory = ProxyServiceFactoryFactory::new(
        dispatch.into_iter().zip(rxs),
        config.clone(),
        router,
        metrics.clone(),
        logging.common().logger().clone(),
    );

    let proxy_cfg = ServerConfig::new(config.network().addr())
        .backlog(config.network().backlog())
        .threads(config.threads());
    let monitoring_cfg = ServerConfig::new(config.monitoring().addr())
        .godfather(|id| format!("monitor {:02}", id));

    let monitoring = MonitorServiceFactoryFactory::new(
        Arc::new(config.clone()),
        Arc::new(logging.clone()),
        metrics,
    );

    cocaine_log!(logging.common().logger(), Severity::Info, "started HTTP proxy at {}", config.network().addr());
    ServerGroup::new(logging.common().logger().clone())?
        .expose(proxy_cfg, factory)?
        .expose(monitoring_cfg, monitoring)?
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
