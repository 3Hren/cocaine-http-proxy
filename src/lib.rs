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
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::vec::IntoIter;

use futures::{future, Future};
use futures::sync::mpsc;
use hyper::StatusCode;
use hyper::server::{Request, Response};
use serde::Serializer;
use serde::ser::SerializeMap;
use tokio_core::reactor::{Core, Handle};
use tokio_service::{Service};

use cocaine::Builder;
use cocaine::logging::{Logger, Severity};
use cocaine::service::{Locator, Unicorn};

use self::metrics::{Count, Counter, Meter, RateMeter};
pub use self::config::Config;
use self::logging::{Loggers};
use self::pool::{SubscribeTask, Event, PoolTask, RoutingGroupsUpdateTask};
use self::route::Route;
use self::route::app::AppRoute;
use self::route::performance::PerformanceRoute;
use self::server::{ServerBuilder, ServerGroup};
use self::service::{ServiceFactory, ServiceFactorySpawn};
use self::service::monitor::MonitorServiceFactoryFactory;

mod config;
mod logging;
mod metrics;
mod pool;
mod route;
mod server;
mod service;
pub mod util;

struct ProxyService {
    log: Logger,
    metrics: Arc<Metrics>,
    addr: Option<SocketAddr>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl Service for ProxyService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        self.metrics.requests.mark(1);

        for route in &self.routes {
            if let Some(future) = route.process(&req) {
                return future;
            }
        }

        future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
    }
}

impl Drop for ProxyService {
    fn drop(&mut self) {
        if let Some(addr) = self.addr.take() {
            cocaine_log!(self.log, Severity::Info, "closed connection from {}", addr);
        } else {
            cocaine_log!(self.log, Severity::Info, "closed connection from Unix socket");
        }

        self.metrics.connections.active.add(-1);
    }
}

#[derive(Clone)]
struct ProxyServiceFactory {
    log: Logger,
    metrics: Arc<Metrics>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl ServiceFactory for ProxyServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = ProxyService;
    type Error    = hyper::Error;

    fn create_service(&mut self, addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        self.metrics.connections.active.add(1);
        self.metrics.connections.accepted.add(1);
        if let Some(addr) = addr {
            cocaine_log!(self.log, Severity::Info, "accepted connection from {}", addr);
        } else {
            cocaine_log!(self.log, Severity::Info, "accepted connection from Unix socket");
        }

        let service = ProxyService {
            log: self.log.clone(),
            metrics: self.metrics.clone(),
            addr: addr,
            routes: self.routes.clone(),
        };

        Ok(service)
    }
}

struct ProxyServiceFactoryFactory {
    tx: Mutex<IntoIter<mpsc::UnboundedSender<Event>>>,
    rx: Mutex<IntoIter<mpsc::UnboundedReceiver<Event>>>,
    log: Logger,
    metrics: Arc<Metrics>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
    cfg: Config,
}

impl ServiceFactorySpawn for ProxyServiceFactoryFactory {
    type Factory = ProxyServiceFactory;

    fn create_factory(&self, handle: &Handle) -> Self::Factory {
        let tx = self.tx.lock().unwrap().next()
            .expect("number of event channels must be exactly the same as the number of threads");
        let rx = self.rx.lock().unwrap().next()
            .expect("number of event channels must be exactly the same as the number of threads");

        // This will stop after all associated connections are closed.
        let pool = PoolTask::new(handle.clone(), self.log.clone(), tx, rx, self.cfg.clone());

        handle.spawn(pool);
        ProxyServiceFactory {
            log: self.log.clone(),
            metrics: self.metrics.clone(),
            routes: self.routes.clone(),
        }
    }
}

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

    // let routes = make_routes(txs);
    let mut routes = Vec::new();
    routes.push(Arc::new(AppRoute::new(txs.clone(), config.tracing().header().into(), log.access().clone())) as Arc<_>);
    routes.push(Arc::new(PerformanceRoute::new(txs.clone(), log.access().clone())) as Arc<_>);

    let factory = Arc::new(ProxyServiceFactoryFactory {
        tx: Mutex::new(txs.clone().into_iter()),
        rx: Mutex::new(rxs.into_iter()),
        log: log.common().clone(),
        metrics: metrics.clone(),
        routes: routes,
        cfg: config.clone(),
    });

    let thread: JoinHandle<Result<(), io::Error>> = {
        let cfg = config.tracing().clone();
        let log = log.common().clone();
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
