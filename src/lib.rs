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
//! - [ ] timeouts.
//! - [ ] forward Authorization header.
//! - [ ] request timeouts.
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
use self::pool::{SubscribeTask, Event, RoutingGroupsUpdateTask};
use self::route::Router;
use self::route::app::AppRoute;
use self::route::perf::PerfRoute;
use self::server::{ServerBuilder, ServerGroup};
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

    let log = Loggers::from(config.logging());
    let metrics = Arc::new(Metrics::default());

    let mut txs = Vec::new();
    let mut rxs = Vec::new();

    for _ in 0..config.threads() {
        let (tx, rx) = mpsc::unbounded();
        txs.push(tx);
        rxs.push(rx);
    }

    // Start all periodic jobs.
    let thread: JoinHandle<Result<(), io::Error>> = {
        let cfg = config.tracing().clone();
        let log = log.common().clone();
        let txs = txs.clone();
        thread::Builder::new().name("periodic".into()).spawn(move || {
            let mut core = Core::new()?;
            let locator = Builder::new("locator")
                .locator_addrs(locator_addrs.clone())
                .build(&core.handle());
            let unicorn = Builder::new("unicorn")
                .locator_addrs(locator_addrs)
                .build(&core.handle());

            let future = RoutingGroupsUpdateTask::new(core.handle(), Locator::new(locator), txs.clone(), log.clone());
            let tracing = {
                let txs = txs.clone();

                SubscribeTask::new(
                    cfg.path().into(),
                    Unicorn::new(unicorn),
                    log.clone(),
                    core.handle(),
                    move |tracing| {
                        cocaine_log!(log, Severity::Info, "updated tracing config with {} entries", tracing.len());
                        for tx in &txs {
                            tx.send(Event::OnTracingUpdates(tracing.clone())).unwrap();
                        }
                    }
                )
            };
            core.run(future.join(tracing)).unwrap();

            Ok(())
        })?
    };

    let mut router = Router::new();
    router.add(Arc::new(AppRoute::new(txs.clone(), config.tracing().header().into(), log.access().clone())));
    router.add(Arc::new(PerfRoute::new(txs.clone(), log.access().clone())));

    let factory = Arc::new(ProxyServiceFactoryFactory::new(
        txs,
        rxs,
        log.common().clone(),
        metrics.clone(),
        router,
        config.clone()
    ));

    let proxy = ServerBuilder::new(config.network().addr())
        .backlog(config.network().backlog())
        .threads(config.threads());
    let monitoring = ServerBuilder::new(config.monitoring().addr())
        .threads(1);

    cocaine_log!(log.common(), Severity::Info, "started HTTP proxy at {}", config.network().addr());
    ServerGroup::new()?
        .expose(proxy, factory)?
        .expose(monitoring, MonitorServiceFactoryFactory::new(Arc::new(config), metrics))?
        .run()?;

    thread.join().unwrap()?;

    Ok(())
}
