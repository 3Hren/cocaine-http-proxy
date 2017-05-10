use std::fmt;
use std::io::{self, ErrorKind};
use std::str;

use byteorder::{ByteOrder, LittleEndian};

use rand;

use futures::{future, Async, BoxFuture, Future, Poll};
use futures::sync::{oneshot, mpsc};

use hyper::{self, Method, StatusCode};
use hyper::header::{self, Header, Raw};
use hyper::server::{Response, Request};

use rmps;
use rmpv::ValueRef;

use cocaine::{Dispatch, Error, Service};
use cocaine::hpack;
use cocaine::logging::Logger;
use cocaine::protocol::{self, Flatten};

use logging::AccessLogger;
use pool::Event;
use route::Route;

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }

#[derive(Clone, Copy, Debug, PartialEq)]
struct XRequestId(u64);

impl Into<u64> for XRequestId {
    fn into(self) -> u64 {
        match self {
            XRequestId(v) => v
        }
    }
}

impl Header for XRequestId {
    fn header_name() -> &'static str {
        "X-Request-Id"
    }

    fn parse_header(raw: &Raw) -> Result<XRequestId, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                if let Ok(val) = u64::from_str_radix(line, 16) {
                    return Ok(XRequestId(val));
                }
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&format!("{:x}", self.0))
    }
}

/// A future that retries application invocation on receiving "safe" error, meaning that it is safe
/// to retry it again until the specified limit reached.
///
/// In this context "safety" means, that the request is guaranteed not to be delivered into the
/// worker, for example when the queue is full.
struct AppWithSafeRetry {
    attempts: u32,
    limit: u32,

    method: Method,

    service: String,
    event: String,
    txs: Vec<mpsc::UnboundedSender<Event>>,

    headers: Vec<hpack::Header>,
    current: Option<BoxFuture<Option<(Response, u64)>, hyper::Error>>,
}

impl AppWithSafeRetry {
    fn new(service: String, event: String, trace: u64, request: &Request, txs: Vec<mpsc::UnboundedSender<Event>>, limit: u32) -> Self {
        let mut headers = Vec::with_capacity(4);

        let mut buf = vec![0; 8];
        LittleEndian::write_u64(&mut buf[..], trace);
        headers.push(hpack::Header::new(&b"trace_id"[..], buf.clone()));
        // TODO: Well, we should generate separate span for each attempt.
        headers.push(hpack::Header::new(&b"span_id"[..], buf.clone()));
        LittleEndian::write_u64(&mut buf[..], 0);
        headers.push(hpack::Header::new(&b"parent_id"[..], buf));

        let mut res = Self {
            attempts: 1,
            limit: limit,
            method: request.method().clone(),
            service: service,
            event: event,
            txs: txs,
            headers: headers,
            current: None,
        };

        res.current = Some(res.make_future());

        res
    }

    fn make_future(&self) -> BoxFuture<Option<(Response, u64)>, hyper::Error> {
        let event = self.event.clone();
        let (tx, rx) = oneshot::channel();

        let method = self.method.clone();
        let mut headers = self.headers.clone();
        let ev = Event::Service {
            name: self.service.clone(),
            func: box move |service: &Service, trace_bit: bool| {
                if trace_bit {
                    headers.push(hpack::Header::new(&b"trace_bit"[..], &b"1"[..]));
                }

                let future = service.call(0, &vec![event], headers, AppReadDispatch {
                    tx: tx,
                    body: None,
                    response: Some(Response::new()),
                }).and_then(move |tx| {
                    // TODO: Proper arguments.
                    let buf = rmps::to_vec(&(method.to_string(), "/", 1, &[("Content-Type", "text/plain")], "")).unwrap();
                    tx.send(0, &[unsafe { ::std::str::from_utf8_unchecked(&buf) }]);
                    tx.send(2, &[0; 0]);
                    Ok(())
                }).then(|_| {
                    Ok(())
                });

                future.boxed()
            }
        };

        let x = rand::random::<usize>();
        let rolled = x % self.txs.len();
        self.txs[rolled].send(ev).unwrap();

        let future = rx.map_err(|err| {
            hyper::Error::Io(io::Error::new(ErrorKind::Other, format!("{}", err)))
        });

        future.boxed()
    }
}

impl Future for AppWithSafeRetry {
    type Item = (Response, u64);
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut future = self.current.take().unwrap();

        match future.poll() {
            Ok(Async::Ready(Some((res, bytes)))) => return Ok(Async::Ready((res, bytes))),
            Ok(Async::Ready(None)) => {
                if self.attempts < self.limit {
                    self.current = Some(self.make_future());
                    self.attempts += 1;
                    return self.poll();
                } else {
                    let body = "Retry limit exceeded: queue is full";
                    let bytes = body.len() as u64;
                    let res = Response::new()
                        .with_status(StatusCode::InternalServerError)
                        .with_body(body);
                    return Ok(Async::Ready((res, bytes)));
                }
            }
            Ok(Async::NotReady) => {}
            Err(err) => {
                return Err(err);
            }
        }

        self.current = Some(future);
        Ok(Async::NotReady)
    }
}

pub struct AppRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
    trace_header: String,
    log: Logger,
}

impl AppRoute {
    // TODO: Make `tracing_header` optional. Use builder.
    pub fn new(txs: Vec<mpsc::UnboundedSender<Event>>, trace_header: String, log: Logger) -> Self {
        Self {
            txs: txs,
            trace_header: trace_header,
            log: log,
        }
    }
}

impl Route for AppRoute {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: &Request) -> Option<Self::Future> {
        let service = req.headers().get::<XCocaineService>();
        let event = req.headers().get::<XCocaineEvent>();

        match (service, event) {
            (Some(service), Some(event)) => {
                let service = service.to_string();
                let event = event.to_string();
                let trace = if let Some(trace) = req.headers().get_raw(&self.trace_header) {
                    match XRequestId::parse_header(trace) {
                        Ok(v) => v.into(),
                        Err(..) => {
                            let res = Response::new()
                                .with_status(StatusCode::BadRequest)
                                .with_body(format!("Invalid `{}` header value", self.trace_header));
                            return Some(future::ok(res).boxed());
                        }
                    }
                } else {
                    rand::random::<u64>()
                };

                let log = AccessLogger::new(self.log.clone(), req);
                let future = AppWithSafeRetry::new(service.clone(), event.clone(), trace, req, self.txs.clone(), 3)
                    .and_then(move |(mut res, bytes_sent)| {
                        res.headers_mut().set_raw("X-Powered-By", "Cocaine");
                        log.commit(trace, res.status().into(), bytes_sent);
                        Ok(res)
                    });
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

#[derive(Deserialize)]
struct MetaInfo {
    code: u32,
    headers: Vec<(String, String)>
}

struct AppReadDispatch {
    tx: oneshot::Sender<Option<(Response, u64)>>,
    body: Option<Vec<u8>>,
    response: Option<Response>,
}

impl Dispatch for AppReadDispatch {
    fn process(mut self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        match protocol::deserialize::<protocol::Streaming<rmps::RawRef>>(ty, data).flatten() {
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
                drop(self.tx.send(Some((res, body_len))));
                None
            }
            // TODO: Make names for category and code.
            Err(Error::Service(ref err)) if err.category() == 0x52ff && err.code() == 1 => {
                drop(self.tx.send(None));
                None
            }
            Err(err) => {
                let body = format!("{}", err);
                let body_len = body.len() as u64;

                // TODO: If service is not available - write 503 (or other custom code).
                let res = Response::new()
                    .with_status(StatusCode::InternalServerError)
                    .with_body(body);
                drop(self.tx.send(Some((res, body_len))));
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
        drop(self.tx.send(Some((res, body_len))));
    }
}
