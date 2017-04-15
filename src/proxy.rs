//! Roadmap:
//! - [x] code style.
//! - [x] GET config.
//! - [x] decomposition.
//! - [x] basic metrics: counters, rates.
//! - [x] enable application services.
//! - [ ] RG support for immediate updates.
//! - [ ] request timeouts.
//! - [ ] unicorn support for tracing.
//! - [ ] unicorn support for timeouts.
//! - [ ] retry policy for applications.
//! - [ ] smart reconnection in the pool.
//! - [ ] metrics: histograms.
//! - [ ] plugin system.

use std::borrow::{Cow};
use std::error;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

use rand;
use futures::{future, Future};
use futures::sync::{oneshot, mpsc};
use hyper::{self, StatusCode};
use hyper::server::{Request, Response};
use itertools::Itertools;
use tokio_core::reactor::{Core, Handle};
use tokio_service::{Service};

use slog;
use slog_term;
use slog::DrainExt;

use rmps;
use rmpv::ValueRef;

use cocaine::{self, Builder, Dispatch, Error};
use cocaine::protocol::{self, Flatten};
use cocaine::logging::{Logger, Severity};

use config::Config;
use logging::{AccessLogger, Loggers};
use pool::{Event, PoolTask};
use route::Route;
use route::performance::PerformanceRoute;
use server::{ServerBuilder, ServerGroup};
use service::{ServiceFactory, ServiceFactorySpawn};
use service::monitor::MonitorServiceFactoryFactory;

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }

// TODO: Move to route/app.rs
struct MainRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
    log: Logger,
}

impl Route for MainRoute {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: &Request) -> Option<Self::Future> {
        let service = req.headers().get::<XCocaineService>();
        let event = req.headers().get::<XCocaineEvent>();

        match (service, event) {
            (Some(service), Some(event)) => {
                let service = service.to_string();
                let event = event.to_string();

                let (tx, rx) = oneshot::channel();

                let ev = Event::Service {
                    name: service,
                    func: box move |service: &cocaine::Service| {
                        let future = service.call(0, &vec![event], AppReadDispatch {
                            tx: tx,
                            body: None,
                            response: Some(Response::new()),
                        })
                        .and_then(|tx| {
                            // TODO: Proper arguments.
                            let buf = rmps::to_vec(&("GET", "/", 1, &[("Content-Type", "text/plain")], "")).unwrap();
                            tx.send(0, &[unsafe { ::std::str::from_utf8_unchecked(&buf) }]);
                            tx.send(2, &[0; 0]);
                            Ok(())
                        })
                        .then(|_| Ok(()));

                        future.boxed()
                    },
                };

                let x = rand::random::<usize>();
                let rolled = x % self.txs.len();
                self.txs[rolled].send(ev).unwrap();

                let log = AccessLogger::new(self.log.clone(), req);
                let future = rx.and_then(move |(mut res, bytes_sent)| {
                    res.headers_mut().set_raw("X-Powered-By", "Cocaine");
                    log.commit(x, res.status().into(), bytes_sent);
                    Ok(res)
                }).map_err(|err| hyper::Error::Io(io::Error::new(ErrorKind::Other, format!("{}", err))));

                Some(future.boxed())
            }
            (Some(..), None) | (None, Some(..)) => {
                let res = Response::new()
                    .with_status(StatusCode::BadRequest)
                    .with_body("Either none or both `X-Cocaine-Service` and `X-Cocaine-Event` headers must be specified");
                Some(future::ok(res).boxed())
            }
            (None, None) => None,
        }
    }
}

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

#[derive(Deserialize)]
struct MetaInfo {
    code: u32,
    headers: Vec<(String, String)>
}

struct AppReadDispatch {
    tx: oneshot::Sender<(Response, u64)>,
    body: Option<Vec<u8>>,
    response: Option<Response>,
}

impl Dispatch for AppReadDispatch {
    fn process(mut self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        match protocol::deserialize::<protocol::Streaming<rmps::Raw>>(ty, data)
            .flatten()
        {
            Ok(Some(data)) => {
                if self.body.is_none() {
                    let meta: MetaInfo = rmps::from_slice(data.as_bytes()).unwrap();
                    let mut res = self.response.take().unwrap();
                    res.set_status(StatusCode::from_u16(meta.code as u16));
                    for (name, value) in meta.headers {
                        res.headers_mut().set_raw(name, value);
                    }
                    self.response = Some(res);
                    self.body = Some(Vec::with_capacity(64));
                } else {
                    self.body.as_mut().unwrap().extend(data.as_bytes());
                }
                Some(self)
            }
            Ok(None) => {
                let body = String::from_utf8_lossy(&self.body.take().unwrap()).into_owned();
                let body_len = body.len() as u64;

                let mut res = self.response.take().unwrap();
                res.set_body(body);
                drop(self.tx.send((res, body_len)));
                None
            }
            Err(err) => {
                let body = format!("{}", err);
                let body_len = body.len() as u64;

                let res = Response::new()
                    .with_status(StatusCode::InternalServerError)
                    .with_body(body);
                drop(self.tx.send((res, body_len)));
                None
            }
        }
    }

    fn discard(self: Box<Self>, err: &Error) {
        let body = format!("{}", err);
        let body_len = body.as_bytes().len() as u64;

        let res = Response::new()
            .with_status(StatusCode::InternalServerError)
            .with_body(body);
        drop(self.tx.send((res, body_len)));
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
        self.metrics.connections.accepted.add(1);
        self.metrics.connections.active.add(1);
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
    rx: Mutex<IntoIter<mpsc::UnboundedReceiver<Event>>>,
    log: Logger,
    metrics: Arc<Metrics>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl ServiceFactorySpawn for ProxyServiceFactoryFactory {
    type Factory = ProxyServiceFactory;

    fn create_factory(&self, handle: &Handle) -> Self::Factory {
        let rx = self.rx.lock().unwrap().next()
            .expect("number of event channels must be exactly the same as the number of threads");

        // This will stop after all associated connections are closed.
        handle.spawn(PoolTask::new(handle.clone(), rx));
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
    routes.push(Arc::new(MainRoute { txs: txs.clone(), log: log.access().clone() }) as Arc<_>);
    routes.push(Arc::new(PerformanceRoute::new(txs, log.access().clone())) as Arc<_>);

    let factory = Arc::new(ProxyServiceFactoryFactory {
        rx: Mutex::new(rxs.into_iter()),
        log: log.common().clone(),
        metrics: metrics.clone(),
        routes: routes,
    });

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

    Ok(())
}
