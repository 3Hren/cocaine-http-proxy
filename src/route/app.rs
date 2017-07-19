use std::borrow::Cow;
use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::str;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use rand;

use futures::{self, Async, BoxFuture, Future, Poll, Stream, future};
use futures::sync::oneshot;

use hyper::{self, HttpVersion, Method, StatusCode};
use hyper::header::{self, Header, Raw};
use hyper::server::{Request, Response};

use regex::Regex;

use rmps;
use rmpv::ValueRef;

use serde::Serializer;

use cocaine::{self, Dispatch, Service};
use cocaine::hpack;
use cocaine::logging::{Log, Severity};
use cocaine::protocol::{self, Flatten};

use logging::AccessLogger;
use pool::{Event, EventDispatch, Settings};
use route::{Match, Route};

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }
header! { (XPoweredBy, "X-Powered-By") => [String] }

impl Default for XPoweredBy {
    fn default() -> Self {
        XPoweredBy(format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")))
    }
}

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

/// Helper for safe debug `Request` formatting without panicking on errors.
struct SafeRequestDebug<'a>(&'a Request);

impl<'a> Debug for SafeRequestDebug<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match write!(fmt, "{:?}", self.0) {
            Ok(()) => Ok(()),
            Err(err) => {
                write!(fmt, "failed to format `Request` using Debug trait: {}", err)
            }
        }
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
    regex: Regex,
    log: L,
}

impl<L: Log + Clone + Send + Sync + 'static> AppRoute<L> {
    pub fn new(dispatcher: EventDispatch, log: L) -> Self {
        Self {
            dispatcher: dispatcher,
            tracing_header: XRequestId::header_name().into(),
            regex: Regex::new("/([^/]*)/([^/?]*)(.*)").expect("invalid URI regex in app route"),
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
    fn extract_parameters(&self, req: &Request) -> Option<Result<(String, String, String), Error>> {
        let service = req.headers().get::<XCocaineService>();
        let event = req.headers().get::<XCocaineEvent>();

        match (service, event) {
            (Some(service), Some(event)) => {
                Some(Ok((service.to_string(), event.to_string(), req.uri().to_string())))
            }
            (Some(..), None) | (None, Some(..)) => Some(Err(Error::IncompleteHeadersMatch)),
            (None, None) => {
                self.regex.captures(req.uri().as_ref()).and_then(|cap| {
                    match (cap.get(1), cap.get(2), cap.get(3)) {
                        (Some(service), Some(event), Some(other)) => {
                            let uri = other.as_str();
                            let uri = if uri.starts_with("/") {
                                uri.into()
                            } else {
                                format!("/{}", uri)
                            };

                            Some(Ok((service.as_str().into(), event.as_str().into(), uri)))
                        }
                        (..) => None,
                    }
                })
            }
        }
    }

    fn invoke(&self, service: String, event: String, req: Request, uri: String)
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

        cocaine_log!(self.log, Severity::Debug, "processing HTTP request"; {
            service: service,
            event: event,
            trace_id: trace,
            request_id: format!("{:016x}", trace),
            request: format!("{:?}", SafeRequestDebug(&req)),
        });

        let log = AccessLogger::new(self.log.clone(), &req);
        let mut app_request = AppRequest::new(service, event, trace, &req, uri);
        let dispatcher = self.dispatcher.clone();
        req.body()
            .concat2()
            .map_err(Error::InvalidBodyRead)
            .and_then(move |body| {
                app_request.set_body(body.to_vec());
                AppWithSafeRetry::new(app_request, dispatcher, 3)
            })
            .and_then(move |(mut resp, bytes_sent)| {
                resp.headers_mut().set(XPoweredBy::default());
                log.commit(trace, resp.status().into(), bytes_sent);
                Ok(resp)
            })
            .boxed()
    }
}

impl<L: Log + Clone + Send + Sync + 'static> Route for AppRoute<L> {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: Request) -> Match<Self::Future> {
        match self.extract_parameters(&req) {
            Some(Ok((service, event, uri))) => {
                let future = self.invoke(service, event, req, uri).then(|resp| {
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
    #[serde(serialize_with = "serialize_body")]
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

#[inline]
fn serialize_body<S>(body: &[u8], se: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    se.serialize_str(unsafe { str::from_utf8_unchecked(body) })
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
    fn new(service: String, event: String, trace: u64, req: &Request, uri: String) -> Self {
        let headers = req.headers()
            .iter()
            .map(|header| {
                let value = header.raw().into_iter().fold(Vec::new(), |mut vec, v| {
                    vec.extend(v);
                    vec
                });
                let value = unsafe { String::from_utf8_unchecked(value) };

                (header.name().to_string(), value)
            })
            .collect();

        let frame = RequestMeta {
            method: req.method().clone(),
            version: req.version(),
            // TODO: Test that uri is sent properly (previously only path was sent).
            uri: uri,
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
                    method: request.frame.method.clone(),
                    body: None,
                    trace: request.trace,
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
    method: Method,
    body: Option<Vec<u8>>,
    trace: u64,
    response: Option<Response>,
}

impl Dispatch for AppReadDispatch {
    fn process(mut self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        match protocol::deserialize::<protocol::Streaming<rmps::RawRef>>(ty, data).flatten() {
            // TODO: Support chunked transfer encoding.
            Ok(Some(data)) => {
                if self.body.is_none() {
                    let meta: ResponseMeta = match rmps::from_slice(data.as_bytes()) {
                        Ok(meta) => meta,
                        Err(err) => {
                            let err = err.to_string();
                            let body_size = err.len();
                            let resp = Response::new()
                                .with_status(StatusCode::InternalServerError)
                                .with_header(XRequestId(self.trace))
                                .with_body(err);
                            drop(self.tx.send(Some((resp, body_size as u64))));
                            return None
                        }
                    };

                    let status = StatusCode::try_from(meta.code as u16)
                        .unwrap_or(StatusCode::InternalServerError);

                    let mut resp = self.response.take().unwrap();
                    resp.set_status(status);
                    resp.headers_mut().set(XRequestId(self.trace));
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
                let (resp, size) = match self.body.take() {
                    Some(body) => {
                        use hyper::header::ContentLength;
                        let mut resp = self.response.take().unwrap();

                        // Special handling for responses with no body.
                        // See https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html for more.
                        let size = if self.method == Method::Head {
                            // We shouldn't remove Content-Length header here, because according to
                            // RFC: the Content-Length entity-header field indicates the size of the
                            // entity-body, in decimal number of OCTETs, sent to the recipient or,
                            // in the case of the HEAD method, the size of the entity-body that
                            // would have been sent had the request been a GET.
                            0
                        } else {
                            match resp.status() {
                                StatusCode::NoContent |
                                StatusCode::NotModified => {
                                    0
                                }
                                _ => {
                                    let mut has_body = !body.is_empty();
                                    has_body = has_body & resp.headers().get::<ContentLength>().map(|head| {
                                        match *head {
                                            ContentLength(len) => return len > 0,
                                        }
                                    }).unwrap_or(true);

                                    if has_body {
                                        let size = body.len();
                                        resp.set_body(body);
                                        size
                                    } else {
                                        0
                                    }
                                }
                            }
                        };

                        (resp, size)
                    }
                    None => {
                        let err = "received `close` event without prior meta info";
                        let size = err.len();
                        let resp = Response::new()
                            .with_status(StatusCode::InternalServerError)
                            .with_header(XRequestId(self.trace))
                            .with_body(err);

                        (resp, size)
                    }
                };

                drop(self.tx.send(Some((resp, size as u64))));
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

                let res = Response::new()
                    .with_status(StatusCode::InternalServerError)
                    .with_header(XRequestId(self.trace))
                    .with_body(body);
                drop(self.tx.send(Some((res, body_len))));
                None
            }
        }
    }

    fn discard(self: Box<Self>, err: &cocaine::Error) {
        let body = err.to_string();
        let body_len = body.as_bytes().len() as u64;

        let status = if let cocaine::Error::Service(ref err) = *err {
            if err.category() == 10 && err.code() == 1 {
                StatusCode::ServiceUnavailable
            } else {
                StatusCode::InternalServerError
            }
        } else {
            StatusCode::InternalServerError
        };

        let res = Response::new()
            .with_status(status)
            .with_header(XRequestId(self.trace))
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
fn test_request_id_header_real() {
    let header = XRequestId::parse_header(&Raw::from("fc1d162f7797fba1")).unwrap();
    let value: u64 = header.into();
    assert_eq!(18166700865008171937, value);
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

// TODO: Test HEAD responses with body.
// TODO: Test NoContent responses with body.
// TODO: Test NotModified responses with body.
// TODO: Test invalid UTF8 headers in request/response.
