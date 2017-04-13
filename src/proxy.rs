//! Roadmap:
//! - [x] code style.
//! - [ ] decomposition.
//! - [ ] basic metrics: counters, rates.
//! - [ ] enable application services.
//! - [ ] request timeouts.
//! - [ ] RG support for immediate updates.
//! - [ ] unicorn support for tracing.
//! - [ ] unicorn support for timeouts.
//! - [ ] retry policy for applications.
//! - [ ] smart reconnection in the pool.
//! - [ ] metrics: histograms.
//! - [ ] plugin system.

use std;
use std::borrow::{Cow};
use std::boxed::FnBox;
use std::collections::HashMap;
use std::error;
use std::io::{self, ErrorKind};
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::vec::IntoIter;

use rand;
use futures::{future, Async, Future, Poll, Stream};
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
use server::{ServerBuilder, ServerGroup};
use service::{ServiceFactory, ServiceFactorySpawn};
use service::monitor::MonitorServiceFactoryFactory;

header! { (XCocaineService, "X-Cocaine-Service") => [String] }

enum Event {
    Service {
        name: String,
        func: Box<FnBox(&cocaine::Service) -> Box<Future<Item=(), Error=()> + Send> + Send>,
    }
}

trait Route: Send + Sync {
    type Future: Future<Item = Response, Error = hyper::Error>;

    /// Tries to process the request, returning a future (doesn't matter success or not) on
    /// conditions match, `None` otherwise.
    ///
    /// A route can process the request fully if all conditions are met, for example if it requires
    /// some headers and all of them are specified.
    /// Also it may decide to fail the request, because of incomplete prerequisites, for example if
    /// it detects all required headers, but fails to match the request method.
    /// At last a route can be neutral to the request, returning `None`.
    fn process(&self, request: &Request) -> Option<Self::Future>;
}

struct MainRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
    log: Logger,
}

impl Route for MainRoute {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: &Request) -> Option<Self::Future> {
        let service = req.headers().get::<XCocaineService>();
        let event = req.headers().get::<XCocaineService>();

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
                let future = rx.and_then(move |(mut res, status, bytes_sent)| {
                    res.headers_mut().set_raw("X-Powered-By", "Cocaine");
                    log.commit(x, status, bytes_sent);
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

struct GeobaseRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
    log: Logger,
}

impl Route for GeobaseRoute {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: &Request) -> Option<Self::Future> {
        let (tx, rx) = oneshot::channel();

        let ev = Event::Service {
            name: "geobase".into(),
            func: box move |service: &cocaine::Service| {
                let future = service.call(0, &vec!["8.8.8.8"], SingleChunkReadDispatch { tx: tx })
                    .then(|tx| {
                        drop(tx);
                        Ok(())
                    });
                future.boxed()
            },
        };

        let x = rand::random::<usize>();
        let rolled = x % self.txs.len();
        self.txs[rolled].send(ev).unwrap();

        let log = AccessLogger::new(self.log.clone(), req);
        let future = rx.and_then(move |(mut res, status, bytes_sent)| {
            res.headers_mut().set_raw("X-Powered-By", "Cocaine");
            log.commit(x, status, bytes_sent);
            Ok(res)
        }).map_err(|err| hyper::Error::Io(io::Error::new(ErrorKind::Other, format!("{}", err))));

        Some(future.boxed())
    }
}

struct ProxyService {
    log: Logger,
    addr: Option<SocketAddr>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl Service for ProxyService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
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
        // TODO: We can also log the following info: duration, metrics.
        cocaine_log!(self.log, Severity::Info, "closed connection from {}", self.addr.take().unwrap());
    }
}

struct SingleChunkReadDispatch {
    tx: oneshot::Sender<(Response, u32, u64)>,
}

impl Dispatch for SingleChunkReadDispatch {
    fn process(self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        let (code, body) = match ty {
            0 => {
                (200, format!("{}", data))
            }
            1 => {
                (500, format!("{}", data))
            }
            m => {
                (500, format!("unknown type: {} {}", m, data))
            }
        };

        let body_len = body.as_bytes().len() as u64;

        let mut res = Response::new();
        res.set_status(StatusCode::from_u16(code as u16));
        res.set_body(body);

        drop(self.tx.send((res, code, body_len)));

        None
    }

    fn discard(self: Box<Self>, err: &Error) {
        let body = format!("{}", err);
        let body_len = body.as_bytes().len() as u64;

        let mut res = Response::new();
        res.set_status(StatusCode::InternalServerError);
        res.set_body(body);

        drop(self.tx.send((res, 500, body_len)));
    }
}

struct AppReadDispatch {
    tx: oneshot::Sender<(Response, u32, u64)>,
    body: Option<Vec<u8>>,
    response: Option<Response>,
}

#[derive(Deserialize)]
struct MetaInfo {
    code: u32,
    headers: Vec<(String, String)>
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
                drop(self.tx.send((res, 200, body_len)));
                None
            }
            Err(err) => {
                let body = format!("{}", err);
                let body_len = body.len() as u64;

                let res = Response::new()
                    .with_status(StatusCode::InternalServerError)
                    .with_body(body);
                drop(self.tx.send((res, 500, body_len)));
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
        drop(self.tx.send((res, 500, body_len)));
    }
}

struct ServicePool {
    /// Next service.
    counter: usize,
    name: String,
    /// Reconnect threshold.
    threshold: Duration,
    handle: Handle,
    last_traverse: SystemTime,

    services: Vec<(cocaine::Service, SystemTime)>,
}

impl ServicePool {
    fn new(name: String, limit: usize, handle: &Handle) -> Self {
        let now = SystemTime::now();

        Self {
            counter: 0,
            name: name.clone(),
            threshold: Duration::new(60, 0),
            handle: handle.clone(),
            last_traverse: now,
            services: std::iter::repeat(name)
                .take(limit)
                .map(|name| (cocaine::Service::new(name.clone(), handle), now))
                .collect()
        }
    }

    fn next(&mut self) -> &cocaine::Service {
        let now = SystemTime::now();

        // No often than every 5 seconds we're traversing services for reconnection.
        if now.duration_since(self.last_traverse).unwrap() > Duration::new(5, 0) {
            self.last_traverse = now;

            let mut killed = 0;

            let kill_limit = self.services.len() / 2;
            for &mut (ref mut service, ref mut birth) in self.services.iter_mut() {
                if now.duration_since(*birth).unwrap() > self.threshold {
                    killed += 1;
                    mem::replace(birth, now);
                    mem::replace(service, cocaine::Service::new(self.name.clone(), &self.handle));

                    if killed > kill_limit {
                        break;
                    }
                }
                // TODO: Tiny pre-connection optimizations.
            }
        }

        self.counter = (self.counter + 1) % self.services.len();
        &self.services[self.counter].0
    }
}

/// This one lives until all associated senders live:
/// - HTTP handlers.
/// - Timers.
/// - Unicorn notifiers.
/// - RG notifiers.
struct Infinity {
    handle: Handle,
    rx: mpsc::UnboundedReceiver<Event>,

    pool: HashMap<String, ServicePool>,
}

impl Infinity {
    fn new(handle: Handle, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        Self {
            handle: handle,
            rx: rx,
            pool: HashMap::new(),
        }
    }
}

impl Infinity {
    fn select_service(&mut self, name: String, handle: &Handle) -> &cocaine::Service {
        let mut pool = self.pool.entry(name.clone())
            .or_insert_with(|| ServicePool::new(name, 10, handle));
        pool.next()
    }
}

impl Future for Infinity {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.rx.poll() {
                Ok(Async::Ready(Some(Event::Service { name, func }))) => {
                    // Select the next service that is not reconnecting right now. No more than N/2
                    // services can be in reconnecting state concurrently.
                    let handle = self.handle.clone();
                    let ref service = self.select_service(name, &handle);

                    handle.spawn(func.call_box((service,)));
                }
                // TODO: RG updates.
                // TODO: Unicorn timeout updates.
                // TODO: Unicorn tracing chance updates.
                Ok(Async::NotReady) => {
                    break;
                }
                Ok(Async::Ready(None)) | Err(..) => {
                    return Ok(Async::Ready(()));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

#[derive(Clone)]
struct ProxyServiceFactory {
    log: Logger,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl ServiceFactory for ProxyServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = ProxyService;
    type Error    = hyper::Error;

    fn create_service(&mut self, addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        cocaine_log!(self.log, Severity::Info, "accepted connection from {}", addr.unwrap());
        // TODO: Increment accepted connections counter.

        let service = ProxyService {
            log: self.log.clone(),
            addr: addr,
            routes: self.routes.clone(),
        };

        Ok(service)
    }
}

struct ProxyServiceFactoryFactory {
    rx: Mutex<IntoIter<mpsc::UnboundedReceiver<Event>>>,
    log: Logger,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl ServiceFactorySpawn for ProxyServiceFactoryFactory {
    type Factory = ProxyServiceFactory;

    fn create_factory(&self, handle: &Handle) -> Self::Factory {
        let rx = self.rx.lock().unwrap().next()
            .expect("number of event channels must be exactly the same as the number of threads");

        // This will stop after all associated connections are closed.
        handle.spawn(Infinity::new(handle.clone(), rx));
        ProxyServiceFactory {
            log: self.log.clone(),
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

pub fn run(config: Config) -> Result<(), Box<error::Error>> {
    let locator_addrs = config.locators()
        .iter()
        .map(|&(addr, port)| SocketAddr::new(addr, port))
        .collect::<Vec<SocketAddr>>();

    check_prerequisites(&config, &locator_addrs)?;

    let log = Loggers::from(config.logging());

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
    routes.push(Arc::new(GeobaseRoute { txs: txs, log: log.access().clone() }) as Arc<_>);

    let factory = Arc::new(ProxyServiceFactoryFactory {
        rx: Mutex::new(rxs.into_iter()),
        log: log.common().clone(),
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
        .expose(monitoring, MonitorServiceFactoryFactory)?
        .run()?;

    Ok(())
}
