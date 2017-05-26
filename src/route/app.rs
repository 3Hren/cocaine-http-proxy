use std::borrow::Cow;
use std::error;
use std::fmt::{self, Display, Formatter};
use std::str;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use rand;

use futures::{self, Async, BoxFuture, Future, Poll, future};
use futures::sync::oneshot;

use hyper::{self, HttpVersion, Method, StatusCode};
use hyper::header::{self, Header, Raw};
use hyper::server::{Request, Response};

use rmps;
use rmpv::ValueRef;

use cocaine::{self, Dispatch, Service};
use cocaine::hpack;
use cocaine::logging::Log;
use cocaine::protocol::{self, Flatten};

use logging::AccessLogger;
use pool::{Event, EventDispatch};
use route::{Match, Route};

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }

#[derive(Clone, Copy, Debug, PartialEq)]
struct XRequestId(u64);

impl Into<u64> for XRequestId {
    fn into(self) -> u64 {
        match self {
            XRequestId(v) => v,
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

trait Call {
    type Call: Fn(&Service, bool) -> Box<Future<Item=(), Error=()> + Send> + Send;
    type Future: Future<Item = Response, Error = Error>;

    fn call_service(&self, name: String, f: Self::Call) -> Self::Future;
}

pub struct AppRoute<L> {
    dispatcher: EventDispatch,
    tracing_header: Cow<'static, str>,
    log: L,
}

impl<L: Log + Clone + Send + Sync + 'static> AppRoute<L> {
    pub fn new(dispatcher: EventDispatch, log: L) -> Self {
        Self {
            dispatcher: dispatcher,
            tracing_header: XRequestId::header_name().into(),
            log: log,
        }
    }

    pub fn with_tracing_header<H>(mut self, header: H) -> Self
        where H: Into<Cow<'static, str>>
    {
        self.tracing_header = header.into();
        self
    }

    /// Extracts required parameters from the request.
    fn extract_parameters(&self, req: &Request) -> Option<Result<(String, String), Error>> {
        let service = req.headers().get::<XCocaineService>();
        let event = req.headers().get::<XCocaineEvent>();

        match (service, event) {
            (Some(service), Some(event)) => {
                Some(Ok((service.to_string(), event.to_string())))
            }
            (Some(..), None) | (None, Some(..)) => Some(Err(Error::IncompleteHeadersMatch)),
            (None, None) => None,
        }
    }

    fn invoke(&self, service: String, event: String, req: Request)
        -> Box<Future<Item = Response, Error = Error>>
    {
        let trace = if let Some(trace) = req.headers().get_raw(&self.tracing_header) {
            match XRequestId::parse_header(trace) {
                Ok(v) => v.into(),
                Err(..) => {
                    let err = Error::InvalidRequestIdHeader(self.tracing_header.clone());
                    return future::err(err).boxed()
                }
            }
        } else {
            // TODO: Log (debug) trace id source (header or generated).
            rand::random::<u64>()
        };

        let log = AccessLogger::new(self.log.clone(), &req);
        // TODO: Collect body first of all.
        let future = AppWithSafeRetry::new(AppRequest::new(service, event, trace, req), self.dispatcher.clone(), 3)
            .and_then(move |(mut resp, bytes_sent)| {
                resp.headers_mut().set_raw("X-Powered-By", "Cocaine");
                log.commit(trace, resp.status().into(), bytes_sent);
                Ok(resp)
            });
        future.boxed()
    }
}

impl<L: Log + Clone + Send + Sync + 'static> Route for AppRoute<L> {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: Request) -> Match<Self::Future> {
        match self.extract_parameters(&req) {
            Some(Ok((service, event))) => {
                let future = self.invoke(service, event, req).then(|resp| {
                    resp.or_else(|err| {
                        let resp = Response::new()
                            .with_status(err.code())
                            .with_body(err.to_string());
                        Ok(resp)
                    })
                });
                Match::Some(Box::new(future))
            }
            Some(Err(err)) => {
                let resp = Response::new()
                    .with_status(err.code())
                    .with_body(err.to_string());
                Match::Some(future::ok(resp).boxed())
            }
            None => Match::None(req),
        }
    }
}

#[derive(Clone)]
struct AppRequest {
    service: String,
    event: String,
    trace: u64,
    method: Method,
    version: u8,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl AppRequest {
    fn new(service: String, event: String, trace: u64, req: Request) -> Self {
        let headers = req.headers()
            .iter()
            .map(|header| (header.name().to_string(), header.value_string()))
            .collect();

        let version = if let HttpVersion::Http11 = req.version() {
            1
        } else {
            0
        };

        Self {
            service: service,
            event: event,
            trace: trace,
            method: req.method().clone(),
            version: version,
            path: req.path().into(),
            headers: headers,
            body: String::new(),
        }
    }
}

/// A future that retries application invocation on receiving "safe" error,
/// meaning that it is safe to retry it again until the specified limit reached.
///
/// In this context "safety" means, that the request is guaranteed not to be
/// delivered to the worker, for example when the queue is full.
struct AppWithSafeRetry {
    attempts: u32,
    limit: u32,
    request: Arc<AppRequest>,
    dispatcher: EventDispatch,
    headers: Vec<hpack::Header>,
    current: Option<BoxFuture<Option<(Response, u64)>, Error>>,
}

impl AppWithSafeRetry {
    fn new(request: AppRequest, dispatcher: EventDispatch, limit: u32) -> Self {
        let mut headers = Vec::with_capacity(4);

        let mut buf = vec![0; 8];
        LittleEndian::write_u64(&mut buf[..], request.trace);
        headers.push(hpack::Header::new(&b"trace_id"[..], buf.clone()));
        // TODO: Well, we should generate separate span for each attempt.
        headers.push(hpack::Header::new(&b"span_id"[..], buf.clone()));
        LittleEndian::write_u64(&mut buf[..], 0);
        headers.push(hpack::Header::new(&b"parent_id"[..], buf));

        let mut res = Self {
            attempts: 1,
            limit: limit,
            request: Arc::new(request),
            dispatcher: dispatcher,
            headers: headers,
            current: None,
        };

        res.current = Some(res.make_future());

        res
    }

    fn make_future(&self) -> BoxFuture<Option<(Response, u64)>, Error> {
        let (tx, rx) = oneshot::channel();

        let request = self.request.clone();
        let mut headers = self.headers.clone();
        let ev = Event::Service {
            name: request.service.clone(),
            func: box move |service: &Service, trace_bit: bool| {
                if trace_bit {
                    headers.push(hpack::Header::new(&b"trace_bit"[..], &b"1"[..]));
                }

                let future = service.call(0, &vec![request.event.clone()], headers, AppReadDispatch {
                    tx: tx,
                    body: None,
                    response: Some(Response::new()),
                }).and_then(move |tx| {
                    let buf = rmps::to_vec(&(
                        request.method.to_string(),
                        &request.path,
                        &request.version,
                        &request.headers[..],
                        // TODO: Proper body.
                        ""
                    )).unwrap();
                    tx.send(0, &[unsafe { ::std::str::from_utf8_unchecked(&buf) }]);
                    tx.send(2, &[0; 0]);
                    Ok(())
                }).then(|_| {
                    Ok(())
                });

                future.boxed()
            }
        };

        self.dispatcher.send(ev);

        let future = rx.map_err(|futures::Canceled| Error::Canceled);

        future.boxed()
    }
}

impl Future for AppWithSafeRetry {
    type Item = (Response, u64);
    type Error = Error;

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

#[derive(Debug)]
enum Error {
    /// Either none or both `X-Cocaine-Service` and `X-Cocaine-Event` headers
    /// must be specified.
    IncompleteHeadersMatch,
    /// Failed to parse special tracing header, by default `X-Request-Id`.
    InvalidRequestIdHeader(Cow<'static, str>),
//    RetryLimitExceeded(u32),
//    Service(cocaine::Error),
    Canceled,
}

impl Error {
    fn code(&self) -> StatusCode {
        match *self {
            Error::IncompleteHeadersMatch |
            Error::InvalidRequestIdHeader(..) => StatusCode::BadRequest,
            Error::Canceled => StatusCode::InternalServerError,
        }
    }
}

impl Display for Error {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::IncompleteHeadersMatch => fmt.write_str(error::Error::description(self)),
            Error::InvalidRequestIdHeader(ref name) => {
                write!(fmt, "Invalid `{}` header value", name)
            }
            Error::Canceled => fmt.write_str("canceled"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IncompleteHeadersMatch => {
                "either none or both `X-Cocaine-Service` and `X-Cocaine-Event` headers must be specified"
            }
            Error::InvalidRequestIdHeader(..) => "invalid tracing header value",
            Error::Canceled => "canceled",
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
            // TODO: Support chunked transfer encoding.
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
            Err(cocaine::Error::Service(ref err)) if err.category() == 0x52ff && err.code() == 1 => {
                drop(self.tx.send(None));
                None
            }
            Err(err) => {
                let body = err.to_string();
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

    fn discard(self: Box<Self>, err: &cocaine::Error) {
        let body = err.to_string();
        let body_len = body.as_bytes().len() as u64;

        let res = Response::new()
            .with_status(StatusCode::InternalServerError)
            .with_body(body);
        drop(self.tx.send(Some((res, body_len))));
    }
}

#[test]
fn test_request_id_header() {
    let header = XRequestId::parse_header(&Raw::from("2a")).unwrap();
    let value: u64 = header.into();
    assert_eq!(42, value);
}

#[test]
fn test_request_id_header_offset() {
    let header = XRequestId::parse_header(&Raw::from("0000002a")).unwrap();
    let value: u64 = header.into();
    assert_eq!(42, value);
}

#[test]
fn test_request_id_header_err() {
    assert!(XRequestId::parse_header(&Raw::from("")).is_err());
    assert!(XRequestId::parse_header(&Raw::from("0x42")).is_err());
    assert!(XRequestId::parse_header(&Raw::from("damn")).is_err());
}
