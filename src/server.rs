//! It is unclear what type of HTTP backend will be chosen finally.

// TODO: unix/ping -> health check.
// TODO: unix/config -> dump config (often config file changes, but daemon not restarted).
// TODO: unix/metrics -> dump metrics.
// TODO: show info about service pool. Draw cluster map. Count response times.

use std;
use std::borrow::Cow;
use std::boxed::FnBox;
use std::collections::HashMap;
use std::error;
use std::io::{self, ErrorKind};
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use net2::TcpBuilder;
use rand;
use futures::{future, Async, Future, Poll, Stream};
use futures::sync::{oneshot, mpsc};
use hyper::{self, Method, StatusCode};
use hyper::server::{Http, Request, Response};
use itertools::Itertools;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Handle};
use tokio_service::{Service, NewService};

use slog;
use slog_term;
use slog::DrainExt;

use rmps;
use rmpv::ValueRef;

use cocaine::{self, Builder, Dispatch, Error};
use cocaine::protocol::{self, Flatten};
use cocaine::logging::{Logger, LoggerContext, Severity};

use config::Config;

type Event = (String, Box<FnBox(&cocaine::Service) -> Box<Future<Item=(), Error=()> + Send> + Send>);

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

//struct MainRoute {
//    txs: Vec<mpsc::UnboundedSender<Event>>,
//}

//impl Route for MainRoute {
//    type Future = Box<Future<Item = Response, Error = io::Error>>;
//
//    fn process(&self, req: &Request) -> Option<Self::Future> {
//        let service = req.headers().find(|&(name, ..)| name == "X-Cocaine-Service");
//        let event = req.headers().find(|&(name, ..)| name == "X-Cocaine-Event");
//
//        match (service, event) {
//            (Some(service), Some(event)) => {
//                let service = String::from_utf8(service.1.to_vec()).unwrap();
//                let event = String::from_utf8(event.1.to_vec()).unwrap();
//
//                let (tx, rx) = oneshot::channel();
//                let x = rand::random::<usize>();
//                let rolled = x % self.txs.len();
//                self.txs[rolled].send((service, box move |service: &cocaine::Service| {
//                    let future = service.call(0, &vec![event], AppReadDispatch {
//                        tx: tx,
//                        body: None,
//                        response: Some(Response::new()),
//                    })
//                    .and_then(|tx| {
//                        // TODO: Proper arguments.
//                        let buf = rmps::to_vec(&("GET", "/", 1, &[("Content-Type", "text/plain")], "")).unwrap();
//                        tx.send(0, &[unsafe { ::std::str::from_utf8_unchecked(&buf) }]);
//                        tx.send(2, &[0; 0]);
//                        Ok(())
//                    })
//                    .then(|_| Ok(()));
//
//                    future.boxed()
//                })).unwrap();
//
//                let future = rx.and_then(move |mut response| {
//                    response.header("X-Powered-By", "Cocaine");
//                    Ok(response)
//                }).map_err(|err| io::Error::new(ErrorKind::Other, format!("{}", err)));
//
//                Some(future.boxed())
//            }
//            (Some(..), None) | (None, Some(..)) => {
//                let mut res = Response::new();
//                res.status_code(400, "Bad Request");
//                res.body(&"Either none or both `X-Cocaine-Service` and `X-Cocaine-Event` headers must be specified");
//                Some(future::ok(res).boxed())
//            }
//            (None, None) => None,
//        }
//    }
//}

struct AccessLogger {
    birth: Instant,
    method: String,
    path: String,
    version: String,
    log: Logger,
}

impl AccessLogger {
    fn new(log: Logger, req: &Request) -> Self {
        Self {
            birth: Instant::now(),
            method: req.method().as_ref().to_owned(),
            path: req.path().to_owned(),
            version: format!("HTTP/1.{}", req.version()),
            log: log,
        }
    }

    fn commit(self, trace: usize, status: u32, bytes_sent: u64) {
        let elapsed = self.birth.elapsed();
        let elapsed_ms = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64 / 1e6;

        cocaine_log!(self.log, Severity::Info, "request finished in {:.3} ms", [elapsed_ms], {
            request_id: trace,
            duration: elapsed_ms / 1000.0,
            method: self.method,
            path: self.path,
            version: self.version,
            status: status,
            bytes_sent: bytes_sent,
        });
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
        let x = rand::random::<usize>();
        let rolled = x % self.txs.len();
        self.txs[rolled].send(("geobase".into(), box move |service: &cocaine::Service| {
            let future = service.call(0, &vec!["8.8.8.8"], SingleChunkReadDispatch { tx: tx })
                .then(|tx| {
                    drop(tx);
                    Ok(())
                });
            future.boxed()
        })).unwrap();

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
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl ProxyService {
    fn not_found(&self) -> Response {
        Response::new().with_status(StatusCode::NotFound)
    }
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

        future::ok(self.not_found()).boxed()
    }
}

// impl Drop for Proxy {
//     fn drop(&mut self) {
//         println!("dropped");
//     }
// }

struct SingleChunkReadDispatch {
    tx: oneshot::Sender<(Response, u32, u64)>,
}

impl Dispatch for SingleChunkReadDispatch {
    fn process(self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        let (code, status, body) = match ty {
            0 => {
                (200, "OK", format!("{}", data))
            }
            1 => {
                (500, "Internal Server Error", format!("{}", data))
            }
            m => {
                (500, "Internal Server Error", format!("unknown type: {} {}", m, data))
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

//struct AppReadDispatch {
//    tx: oneshot::Sender<Response>,
//    body: Option<Vec<u8>>,
//    response: Option<Response>,
//}
//
//#[derive(Deserialize)]
//struct MetaInfo {
//    code: u32,
//    headers: Vec<(String, String)>
//}
//
//impl Dispatch for AppReadDispatch {
//    fn process(mut self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
//        match protocol::deserialize::<protocol::Streaming<rmps::Raw>>(ty, data)
//            .flatten()
//        {
//            Ok(Some(data)) => {
//                if self.body.is_none() {
//                    let meta: MetaInfo = rmps::from_slice(data.as_bytes()).unwrap();
//                    let mut res = self.response.take().unwrap();
//                    res.status_code(meta.code, "OK");
//                    for &(ref name, ref value) in &meta.headers {
//                        res.header(&name, &value);
//                    }
//                    self.response = Some(res);
//                    self.body = Some(Vec::with_capacity(64));
//                } else {
//                    self.body.as_mut().unwrap().extend(data.as_bytes());
//                }
//                Some(self)
//            }
//            Ok(None) => {
//                let mut res = self.response.take().unwrap();
//                res.body(&unsafe{ ::std::str::from_utf8_unchecked(&self.body.take().unwrap()) });
//                drop(self.tx.send(res));
//                None
//            }
//            Err(err) => {
//                let mut res = Response::new();
//                res.status_code(500, "Internal Server Error");
//                res.body(&format!("{}", err));
//                drop(self.tx.send(res));
//                None
//            }
//        }
//    }
//
//    fn discard(self: Box<Self>, err: &Error) {
//        let mut res = Response::new();
//        res.status_code(500, "Internal Server Error");
//        res.body(&format!("{}", err));
//
//        drop(self.tx.send(res));
//    }
//}

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

struct Infinity {
    handle: Handle,
    rx: mpsc::UnboundedReceiver<Event>,

    pool: HashMap<String, ServicePool>,
}

impl Infinity {
    fn new(handle: Handle, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        Infinity {
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
                Ok(Async::Ready(Some((name, func)))) => {
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
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl NewService for ProxyServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = ProxyService;
    type Error    = hyper::Error;

    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        let service = ProxyService {
            routes: self.routes.clone(),
        };

        Ok(service)
    }
}

fn check_connection<N>(name: N, locator_addrs: Vec<SocketAddr>) -> Result<(), Error>
    where N: Into<Cow<'static, str>>
{
    let mut core = Core::new()
        .map_err(Error::Io)?;

    let service = Builder::new(name)
        .locator_addrs(locator_addrs)
        .build(&core.handle());

    core.run(service.connect())
}

const CONFIG_LOCATOR_SUCC: &str = "configured cloud entry points using locator(s) specified above";
const CONFIG_LOCATOR_FAIL: &str = "failed to establish connection to the locator(s) specified above - ensure that `cocaine-runtime` is running and the `locator` is properly configured";

const CONFIG_LOGGING_FAIL: &str = "failed to establish connection to the logging service";

use monitoring::MonitoringServer;

struct HttpService {
    rx: mpsc::UnboundedReceiver<(TcpStream, SocketAddr)>,
    handle: Handle,
    protocol: Http,
    factory: ProxyServiceFactory,
}

impl Future for HttpService {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.rx.poll() {
                Ok(Async::Ready(Some((sock, addr)))) => {
                    self.protocol.bind_connection(&self.handle, sock, addr, self.factory.new_service().unwrap());
                }
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

pub fn run(config: Config) -> Result<(), Box<error::Error>> {
    // NOTE: Create and run monitoring server if needed.
    let root_log = slog::Logger::root(slog_term::streamer().stdout().compact().build().fuse(), o!());

    let (addr, port) = config.monitoring().addr().clone();
    let addr = SocketAddr::new(addr, port);
    let log = root_log.new(o!("🚀  Mount" => format!("monitoring server on {}", addr)));
    slog_info!(log, "for more information about monitoring API visit `GET http://{}/help`", addr);
    MonitoringServer::new(addr).run().unwrap();

    // NOTE: Create and run HTTP proxy.
    let (addr, port) = config.addr().clone();
    let addr = SocketAddr::new(addr, port);
    let log = root_log.new(o!("🚀  Mount" => format!("cocaine proxy server on {}", addr)));
    slog_info!(log, "cocaine http proxy server is ready");

    let locator_addrs = config.locators()
        .iter()
        .map(|&(addr, port)| SocketAddr::new(addr.clone(), port))
        .collect::<Vec<SocketAddr>>();
    let log = root_log.new(o!("Service" => format!("locator on {}", locator_addrs.iter().join(", "))));
    if let Ok(..) = check_connection("locator", locator_addrs.clone()) {
        slog_info!(log, CONFIG_LOCATOR_SUCC);
    } else {
        slog_warn!(log, CONFIG_LOCATOR_FAIL);
    }

    let log = root_log.new(o!("Service" => "logging", "name" => config.logging().name().to_string()));
    if let Ok(..) = check_connection(config.logging().name().to_string(), locator_addrs.clone()) {
        slog_info!(log, "configured cloud logging `{}` prefix and `{}` severity - all further logs will be written there", config.logging().prefix(), config.logging().severity());
    } else {
        slog_warn!(log, CONFIG_LOGGING_FAIL);
    }

    // TODO: Check connection to unicorn service.

    let ctx = LoggerContext::new(config.logging().name().to_owned());
    ctx.filter().set(config.logging().severity().into());

    let log = ctx.create(format!("{}/common", config.logging().prefix()));

    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    let mut threads: Vec<JoinHandle<Result<(), io::Error>>> = Vec::new();
    let mut dispatchers = Vec::new();

    for _ in 0..config.threads() {
        let (tx, rx) = mpsc::unbounded();
        txs.push(tx);
        rxs.push(Some(rx));
    }

    let mut routes = Vec::new();
    //    routes.push(Arc::new(MainRoute { txs: txs.clone() }) as Arc<_>);
    routes.push(Arc::new(GeobaseRoute {
        txs: txs,
        log: ctx.create(format!("{}/access", config.logging().prefix())),
    }) as Arc<_>);

    let factory = ProxyServiceFactory { routes: routes };

    for id in 0..config.threads() {
        let (tx, rx) = mpsc::unbounded();
        let irx = mem::replace(&mut rxs[id], None).unwrap();

        let factory = factory.clone();
        let thread = thread::Builder::new().name(format!("worker {:02}", id)).spawn(move || {
            let mut core = Core::new()?;
            let handle = core.handle();

            handle.spawn(Infinity::new(handle.clone(), irx));
            core.run(HttpService { rx: rx, handle: handle, protocol: Http::new(), factory: factory }).unwrap();

            Ok(())
        })?;

        threads.push(thread);
        dispatchers.push(tx);
    }

    cocaine_log!(log, Severity::Info, "started HTTP proxy at {}", addr);

    let listener = TcpBuilder::new_v6()?
        .bind(addr)?
        .listen(config.network().backlog())?;

    let mut core = Core::new()?;
    let listener = TcpListener::from_listener(listener, &addr, &core.handle())?;

    let mut iter = dispatchers.iter().cycle();
    core.run(listener.incoming().for_each(move |(sock, addr)| {
        iter.next().expect("iterator is infinite").send((sock, addr)).unwrap();
        Ok(())
    }))?;

    Ok(())
}
