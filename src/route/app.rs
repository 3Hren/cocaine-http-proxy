use std::borrow::Cow;
use std::error;
use std::fmt::{self, Display, Formatter};
use std::str;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use rand;

use futures::{self, Async, BoxFuture, Future, Poll, Stream, future};
use futures::sync::oneshot;

use hyper::{self, HttpVersion, Method, StatusCode};
use hyper::header::{self, Header, Raw};
use hyper::server::{Request, Response};

use rmps;
use rmpv::ValueRef;

use serde::Serializer;

use cocaine::{self, Dispatch, Service};
use cocaine::hpack;
use cocaine::logging::Log;
use cocaine::protocol::{self, Flatten};

use logging::AccessLogger;
use pool::{Event, EventDispatch, Settings};
use route::{Match, Route};

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }
header! { (XPoweredBy, "X-Powered-By") => [String] }

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
    type Call: Fn(&Service, Settings) -> Box<Future<Item = (), Error = ()> + Send> + Send;
    type Future: Future<Item = Response, Error = Error>;

    /// Selects an appropriate service with its settings from the pool and
    /// passes them to the provided callback.
    fn call_service(&self, name: String, callback: Self::Call) -> Self::Future;
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
    fn extract_parameters(req: &Request) -> Option<Result<(String, String), Error>> {
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
        let mut app_request = AppRequest::new(service, event, trace, &req);
        let dispatcher = self.dispatcher.clone();
        req.body()
            .concat2()
            .map_err(Error::InvalidBodyRead)
            .and_then(move |body| {
                app_request.set_body(body.to_vec());
                AppWithSafeRetry::new(app_request, dispatcher, 3)
            })
            .and_then(move |(mut resp, bytes_sent)| {
                resp.headers_mut().set(XPoweredBy("Cocaine".into()));
                log.commit(trace, resp.status().into(), bytes_sent);
                Ok(resp)
            })
            .boxed()
    }
}

impl<L: Log + Clone + Send + Sync + 'static> Route for AppRoute<L> {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: Request) -> Match<Self::Future> {
        match Self::extract_parameters(&req) {
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

/// A meta frame of HTTP request for cocaine application HTTP protocol.
#[derive(Clone, Serialize)]
struct RequestMeta {
    #[serde(serialize_with = "serialize_method")]
    method: Method,
    uri: String,
    #[serde(serialize_with = "serialize_version")]
    version: HttpVersion,
    headers: Vec<(String, String)>,
    /// HTTP body. May be empty either when there is no body in the request or if it is transmitted
    /// later.
    body: Vec<u8>,
}

#[inline]
fn serialize_method<S>(method: &Method, se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    se.serialize_str(&method.to_string())
}

#[inline]
fn serialize_version<S>(version: &HttpVersion, se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    let v = if let &HttpVersion::Http11 = version {
        "1.1"
    } else {
        "1.0"
    };

    se.serialize_str(v)
}

#[derive(Debug, Deserialize)]
struct ResponseMeta {
    code: u32,
    headers: Vec<(String, String)>
}

#[derive(Clone)]
struct AppRequest {
    service: String,
    event: String,
    trace: u64,
    frame: RequestMeta,
}

impl AppRequest {
    fn new(service: String, event: String, trace: u64, req: &Request) -> Self {
        let headers = req.headers()
            .iter()
            .map(|header| (header.name().to_string(), header.value_string()))
            .collect();

        let frame = RequestMeta {
            method: req.method().clone(),
            version: req.version(),
            // TODO: Test that uri is sent properly (previously only path was sent).
            uri: req.uri().to_string(),
            headers: headers,
            body: Vec::new(),
        };

        Self {
            service: service,
            event: event,
            trace: trace,
            frame: frame,
        }
    }

    fn set_body(&mut self, body: Vec<u8>) {
        self.frame.body = body;
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
            func: box move |service: &Service, settings: Settings| {
                if settings.verbose {
                    headers.push(hpack::Header::new(&b"trace_bit"[..], &b"1"[..]));
                }

                if let Some(timeout) = settings.timeout {
                    headers.push(hpack::Header::new(&b"request_timeout"[..], format!("{}", timeout).into_bytes()));
                }

                let future = service.call(0, &vec![request.event.clone()], headers, AppReadDispatch {
                    tx: tx,
                    body: None,
                    response: Some(Response::new()),
                }).and_then(move |tx| {
                    let buf = rmps::to_vec(&request.frame).unwrap();
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
    InvalidBodyRead(hyper::Error),
    Canceled,
}

impl Error {
    fn code(&self) -> StatusCode {
        match *self {
            Error::IncompleteHeadersMatch |
            Error::InvalidRequestIdHeader(..) => StatusCode::BadRequest,
            Error::InvalidBodyRead(..) |
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
            Error::InvalidBodyRead(ref err) => write!(fmt, "{}", err),
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
            Error::InvalidBodyRead(..) => "failed to read HTTP body",
            Error::Canceled => "canceled",
        }
    }
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
                    let meta: ResponseMeta = rmps::from_slice(data.as_bytes()).unwrap();
                    let mut resp = self.response.take().unwrap();
                    resp.set_status(StatusCode::try_from(meta.code as u16).unwrap_or(StatusCode::InternalServerError));
                    for (name, value) in meta.headers {
                        // TODO: Filter headers - https://tools.ietf.org/html/draft-ietf-httpbis-p1-messaging-14#section-7.1.3
                        resp.headers_mut().set_raw(name, value);
                    }
                    self.response = Some(resp);
                    self.body = Some(Vec::with_capacity(64));
                } else {
                    // TODO: If TE: chunked - feed parser. Consume chunks until None and send.
                    // TODO: Otherwise - just send.
                    self.body.as_mut().unwrap().extend(data.as_bytes());
                }
                Some(self)
            }
            Ok(None) => {
                let body = self.body.take().unwrap();
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

#[cfg(test)]
mod test {
    use hyper::HttpVersion;
    use serde_json::Serializer;

    use super::serialize_version;

    #[test]
    fn test_serialize_version() {
        let mut se = Serializer::new(Vec::new());
        serialize_version(&HttpVersion::Http10, &mut se).unwrap();
        assert_eq!(&b"\"1.0\""[..], &se.into_inner()[..]);

        let mut se = Serializer::new(Vec::new());
        serialize_version(&HttpVersion::Http11, &mut se).unwrap();
        assert_eq!(&b"\"1.1\""[..], &se.into_inner()[..]);
    }
}
