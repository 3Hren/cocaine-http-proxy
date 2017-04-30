//! Roadmap:
//! - [x] code style.
//! - [x] GET config.
//! - [x] decomposition.
//! - [x] basic metrics: counters, rates.
//! - [x] enable application services.
//! - [x] smart reconnection in the pool.
//! - [x] RG support for immediate updates.
//! - [ ] fixed-size pool balancing.
//! - [ ] pass pool settings from config.
//! - [ ] request timeouts.
//! - [ ] headers in the framework.
//! - [ ] unicorn support for tracing.
//! - [ ] unicorn support for timeouts.
//! - [ ] retry policy for applications.
//! - [ ] metrics: histograms.
//! - [ ] plugin system.
//! - [ ] JSON RPC.
//! - [ ] MDS direct.
//! - [ ] Streaming logging through HTTP.

use std::borrow::Cow;
use std::error;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::vec::IntoIter;

use futures::{future, Future};
use futures::sync::mpsc;
use hyper::{self, StatusCode};
use hyper::server::{Request, Response};
use itertools::Itertools;
use tokio_core::reactor::{Core, Handle};
use tokio_service::{Service};

use slog;
use slog_term;
use slog::DrainExt;

use cocaine::{Builder, Error};
use cocaine::logging::{Logger, Severity};
use cocaine::service::Locator;

use config::{Config, PoolConfig};
use logging::{Loggers};
use pool::{Event, PoolTask, RoutingGroupsUpdateTask};
use route::Route;
use route::app::AppRoute;
use route::performance::PerformanceRoute;
use server::{ServerBuilder, ServerGroup};
use service::{ServiceFactory, ServiceFactorySpawn};
use service::monitor::MonitorServiceFactoryFactory;

// TODO: Move to service/proxy.rs
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
        self.metrics.connections.active.add(-1);
        cocaine_log!(self.log, Severity::Info, "closed connection from {}", self.addr.take().unwrap());
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
        cocaine_log!(self.log, Severity::Info, "accepted connection from {}", addr.unwrap());

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
    cfg: PoolConfig,
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

fn check_connection<N>(name: N, locator_addrs: Vec<SocketAddr>, core: &mut Core) -> Result<(), Error>
    where N: Into<Cow<'static, str>>
{
    let service = Builder::new(name)
        .locator_addrs(locator_addrs)
        .build(&core.handle());

    core.run(service.connect())
}

fn check_prerequisites(config: &Config, locator_addrs: &Vec<SocketAddr>) -> Result<(), Error> {
    let mut core = Core::new()
        .map_err(Error::Io)?;

    let log = slog::Logger::root(
        slog_term::streamer().stdout().compact().build().fuse(),
        o!("🛠️  Configure" => crate_name!())
    );

    slog_info!(log, "mount cocaine HTTP proxy server on {}", config.network().addr());
    slog_info!(log, "mount monitor server on {0}, for more information about monitoring API visit `GET http://{0}/help`", config.monitoring().addr());

    let mut incomplete = false;

    let lg = log.new(o!("Service" => format!("locator on {}", locator_addrs.iter().join(", "))));
    match check_connection("locator", locator_addrs.clone(), &mut core) {
        Ok(()) => slog_info!(lg, "configured cloud entry points using locator(s) specified above"),
        Err(err) => {
            incomplete = true;
            slog_warn!(lg, "failed to establish connection to the locator(s) specified above: {}", err);
        }
    }

    let lg = log.new(o!("Service" => "logging"));
    for &(ty, ref cfg) in &[("common", config.logging().common()), ("access", config.logging().access())] {
        match check_connection(cfg.name().to_string(), locator_addrs.clone(), &mut core) {
            Ok(()) => slog_info!(lg, "configured `{}` logging using `{}` service with `{}` source and `{}` severity", ty, cfg.name(), cfg.source(), cfg.severity()),
            Err(err) => {
                incomplete = true;
                slog_warn!(lg, "failed to establish connection to the logging service `{}`: {}", cfg.name(), err);
            }
        }
    }

    let lg = log.new(o!("Service" => "unicorn"));
    match check_connection("unicorn", locator_addrs.clone(), &mut core) {
        Ok(()) => slog_info!(lg, "configured unicorn service"),
        Err(err) => {
            incomplete = true;
            slog_warn!(lg, "failed to establish connection to the unicorn service: {}", err);
        }
    }

    if incomplete {
        slog_warn!(log, "some of required services has failed checking - ensure that `cocaine-runtime` is running and properly configured");
    }

    slog_info!(log, "launching ...");

    Ok(())
}

use serde::Serializer;
use serde::ser::SerializeMap;
use metrics::{Count, Counter, Meter, RateMeter};

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

    check_prerequisites(&config, &locator_addrs)?;

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
    routes.push(Arc::new(AppRoute::new(txs.clone(), log.access().clone())) as Arc<_>);
    routes.push(Arc::new(PerformanceRoute::new(txs.clone(), log.access().clone())) as Arc<_>);

    let factory = Arc::new(ProxyServiceFactoryFactory {
        tx: Mutex::new(txs.clone().into_iter()),
        rx: Mutex::new(rxs.into_iter()),
        log: log.common().clone(),
        metrics: metrics.clone(),
        routes: routes,
        cfg: config.pool().clone(),
    });

    let thread: JoinHandle<Result<(), io::Error>> = {
        let log = log.common().clone();
        thread::Builder::new().name("periodic".into()).spawn(move || {
            let mut core = Core::new()?;
            let locator = Builder::new("locator")
                .locator_addrs(locator_addrs)
                .build(&core.handle());

            let future = RoutingGroupsUpdateTask::new(core.handle(), Locator::new(locator), txs, log);
            core.run(future)
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
