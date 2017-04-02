//! It is unclear what type of HTTP backend will be chosen finally.

// TODO: unix/ping -> health check.
// TODO: unix/config -> dump config (often config file changes, but daemon not restarted).
// TODO: unix/metrics -> dump metrics.
// TODO: show info about service pool. Draw cluster map. Count response times.

use std;
use std::boxed::FnBox;
use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use rand;

use futures::{future, Async, Future, Poll, Stream};
use futures::sync::{oneshot, mpsc};
use tokio_core::reactor::{Core, Handle};
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};

use slog;
use slog_term;
use slog::DrainExt;

use rmps;
use rmpv::ValueRef;
use cocaine::{self, Dispatch, Error};
use cocaine::protocol::{self, Flatten};
use cocaine::logging::{LoggerContext, Sev};

use config::Config;

type Event = (String, Box<FnBox(&cocaine::Service) -> Box<Future<Item=(), Error=()> + Send> + Send>);

trait Route: Send + Sync {
    type Future: Future<Item = Response, Error = io::Error>;

    /// Tries to process the request, returning a future (doesn't matter success or not) on
    /// conditions match, `None` otherwise.
    ///
    /// A route can process the request fully if all conditions are met, for example if it requires
    /// some headers and all of them are specified.
    /// Also it may decide to fail the request, because of incomplete prerequests, for example if
    /// it detects all required headers, but failes to match the request method.
    /// At last a route can be neutral to the request, returning `None`.
    fn process(&self, request: &Request) -> Option<Self::Future>;
}

struct MainRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
}

impl Route for MainRoute {
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn process(&self, req: &Request) -> Option<Self::Future> {
        let service = req.headers().find(|&(name, ..)| name == "X-Cocaine-Service");
        let event = req.headers().find(|&(name, ..)| name == "X-Cocaine-Event");

        match (service, event) {
            (Some(service), Some(event)) => {
                let service = String::from_utf8(service.1.to_vec()).unwrap();
                let event = String::from_utf8(event.1.to_vec()).unwrap();

                let (tx, rx) = oneshot::channel();
                let x = rand::random::<usize>();
                let rolled = x % self.txs.len();
                self.txs[rolled].send((service, box move |service: &cocaine::Service| {
                    let future = service.call(0, &vec![event], AppReadDispatch {
                        tx: tx,
                        body: None,
                        response: Some(Response::new()),
                    })
                    .and_then(|tx| {
                        let buf = rmps::to_vec(&("GET", "/", 1, &[("Content-Type", "text/plain")], "")).unwrap();
                        tx.send(0, &[unsafe { ::std::str::from_utf8_unchecked(&buf) }]);
                        tx.send(2, &[0; 0]);
                        Ok(())
                    })
                    .then(|_| Ok(()));

                    future.boxed()
                })).unwrap();

                let future = rx.and_then(move |mut response| {
                    response.header("X-Powered-By", "Cocaine");
                    Ok(response)
                }).map_err(|err| io::Error::new(ErrorKind::Other, format!("{}", err)));

                Some(future.boxed())
            }
            (Some(..), None) | (None, Some(..)) => {
                let mut res = Response::new();
                res.status_code(400, "Bad Request");
                res.body(&"Either none or both `X-Cocaine-Service` and `X-Cocaine-Event` headers must be specified");
                Some(future::ok(res).boxed())
            }
            (None, None) => None,
        }
    }
}

struct GeobaseRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
}

impl Route for GeobaseRoute {
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn process(&self, _req: &Request) -> Option<Self::Future> {
        let (tx, rx) = oneshot::channel();
        let x = rand::random::<usize>();
        // let birth = Instant::now();
        let rolled = x % self.txs.len();
        self.txs[rolled].send(("geobase".into(), box move |service: &cocaine::Service| {
            let future = service.call(0, &vec!["8.8.8.8"], SingleChunkReadDispatch { tx: tx })
                .then(|tx| {
                    drop(tx);
                    Ok(())
                });
            future.boxed()
        })).unwrap();

        let future = rx.and_then(move |mut response| {
            response.header("X-Powered-By", "Cocaine");

            // TODO: Write to `proxy/access` logs.
            // let elapsed = birth.elapsed();
            // info!("request finished [{:#018.18x}] in {:.3} ms", x, (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64 / 1e6);

            Ok(response)
        }).map_err(|err| io::Error::new(ErrorKind::Other, format!("{}", err)));

        Some(future.boxed())
    }
}

struct ProxyService {
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = io::Error>>>>>,
}

impl ProxyService {
    fn not_found(&self) -> Response {
        let mut res = Response::new();
        res.status_code(404, "Bad Request");
        res.body(&"Not found");
        res
    }
}

impl Service for ProxyService {
    type Request  = Request;
    type Response = Response;
    type Error    = io::Error;
    type Future   = Box<Future<Item = Response, Error = io::Error>>;

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
    tx: oneshot::Sender<Response>,
}

impl Dispatch for SingleChunkReadDispatch {
    fn process(self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        let mut response = Response::new();

        match ty {
            0 => {
                response.status_code(200, "OK");
                response.body(&format!("{}", data));
            }
            1 => {
                let mut response = Response::new();
                response.status_code(500, "Internal Server Error");
                response.body(&format!("{}", data));
            }
            m => {
                let mut response = Response::new();
                response.status_code(500, "Internal Server Error");
                response.body(&format!("unknown type: {} {}", m, data));
            }
        }

        drop(self.tx.send(response));

        None
    }

    fn discard(self: Box<Self>, err: &Error) {
        let mut response = Response::new();
        response.status_code(500, "Internal Server Error");
        response.body(&format!("{}", err));

        drop(self.tx.send(response));
    }
}

struct AppReadDispatch {
    tx: oneshot::Sender<Response>,
    body: Option<Vec<u8>>,
    response: Option<Response>,
}

#[derive(Deserialize)]
struct MetaInfo{
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
                    let mut response = self.response.take().unwrap();
                    response.status_code(meta.code, "OK");
                    for &(ref name, ref value) in &meta.headers {
                        response.header(&name, &value);
                    }
                    self.response = Some(response);
                    self.body = Some(Vec::with_capacity(64));
                } else {
                    self.body.as_mut().unwrap().extend(data.as_bytes());
                }
                Some(self)
            }
            Ok(None) => {
                let mut response = self.response.take().unwrap();
                response.body(&unsafe{ ::std::str::from_utf8_unchecked(&self.body.take().unwrap()) });
                drop(self.tx.send(response));
                None
            }
            Err(err) => {
                let mut response = Response::new();
                response.status_code(500, "Internal Server Error");
                response.body(&format!("{}", err));
                drop(self.tx.send(response));
                None
            }
        }
    }

    fn discard(self: Box<Self>, err: &Error) {
        let mut response = Response::new();
        response.status_code(500, "Internal Server Error");
        response.body(&format!("{}", err));

        drop(self.tx.send(response));
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

            for id in 0..self.services.len() {
                let (.., birth) = self.services[id];
                if now.duration_since(birth).unwrap() > self.threshold {
                    killed += 1;
                    self.services[id] = (cocaine::Service::new(self.name.clone(), &self.handle), now);
                }

                if killed > self.services.len() / 2 {
                    break;
                }

                // TODO: Tiny preconnection optimization.
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
                Ok(Async::NotReady) => {
                    break;
                }
                Ok(Async::Ready(None)) | Err(..) => {
                    unreachable!();
                }
            }
        }

        Ok(Async::NotReady)
    }
}

struct ProxyServiceFactory {
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = io::Error>>>>>,
}

impl NewService for ProxyServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = ProxyService;
    type Error    = io::Error;

    fn new_service(&self) -> Result<Self::Instance, Self::Error> {
        let service = ProxyService {
            routes: self.routes.clone(),
        };

        Ok(service)
    }
}

//fn check_connection(name: &str) -> Result<(), Error> {
//    let mut core = Core::new()
//        .expect("failed to initialize event loop");
//    let service = cocaine::Service::new(name, &core.handle())
//        .connect();
//    Ok(())
//}

pub fn run(config: Config) -> Result<(), Box<Error>> {
    let rlog = slog::Logger::root(slog_term::streamer().stdout().compact().build().fuse(), o!());

//    let mlog = rlog.new(o!("🚀  Mount" => "monitoring server on [::]:10000"));
//    slog_info!(mlog, "GET /help - information");
//    slog_info!(mlog, "GET /ping - health checking");
//    slog_info!(mlog, "GET /config - fetching configuration");
//    slog_info!(mlog, "GET /metrics - fetching runtime metrics");
//    slog_info!(mlog, "GET /severity - getting logging severity");
//    slog_info!(mlog, "PUT /severity - setting logging severity");

    let mlog = rlog.new(o!("🚀  Mount" => format!("cocaine proxy server on {}:{}", config.addr(), config.port())));
    slog_info!(mlog, "XXX / - entry point for each request");

    let slog = rlog.new(o!("🌳  Service" => "logging"));

    // TODO: Check connection to logging service.
    slog_info!(slog, "configured cloud logging with `proxy` prefix and `INFO` severity - all further logs will be written there");

    // TODO: Check connection to unicorn service.

    let ctx = LoggerContext::default();
    let log = ctx.create("proxy/common");

    let mut txs = Vec::new();
    let mut threads = Vec::new();

    for tid in 0..config.threads().network() {
        let (tx, rx) = mpsc::unbounded();
        let thread = thread::Builder::new()
            .name(format!("W#{:02}", tid))
            .spawn(move ||
        {
            let mut core = Core::new()
                .expect("failed to initialize event loop");
            let handle = core.handle();

            core.run(Infinity::new(handle, rx)).unwrap();
        });

        txs.push(tx);
        threads.push(thread);
    }

    let addr = SocketAddr::new(config.addr().clone(), config.port());
    let mut srv = TcpServer::new(Http, addr);
    srv.threads(config.threads().http());

    let mut routes = Vec::new();
    routes.push(Arc::new(MainRoute { txs: txs.clone() }) as Arc<_>);
    routes.push(Arc::new(GeobaseRoute { txs: txs }) as Arc<_>);

    let factory = ProxyServiceFactory { routes: routes };

//    log!(log, "started HTTP proxy at {}:{}", config.addr(), config.port());
    srv.serve(factory);

    Ok(())
}

// logging
// [ ] Check logging service on start.
// [ ] Write reconnection logs there.
// [ ] Write access logs.
// [ ] Check performance (into the file sink and into the null sink).
