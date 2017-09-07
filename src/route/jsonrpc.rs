//! [JSON RPC][jsonrpc] implementation for Cocaine services.
//!
//! The "method" parameter must be represented in form <service>.<event>.
//! The "params" parameter may be omitted when a service takes no arguments, an array with
//! positional arguments when a service protocol terminates immediately, named arguments with a
//! fixed format otherwise (see below).
//!
//! `>>> {"jsonrpc": "2.0", "method": "echo.info", "id": 0}`
//! `<<< {"jsonrpc": "2.0", "result": [{"event": "value", "value": [{"load":0}]}]}`
//!
//! For example calling `read` method from `Storage` service:
//!
//! `>>> {"jsonrpc": "2.0", "method": "storage.read", "params": ["collection", "key"], "id": 0}`
//! `<<< {"jsonrpc": "2.0", "result": [{"event": "value", "value": ["Путин - не краб!"]}], "id": 0}`
//!
//! To demonstrate streaming input let's call "ping" event from "echo" application service:
//!
//! `>>> {"jsonrpc": "2.0", "method": "echo.enqueue", "params": {"args": ["ping"], "chunks": [{"write": ["le message"]}, {"close", []}], "id": 0}`
//! `<<< {"jsonrpc": "2.0", "result": [{"event": "write", "value": ["le message"]}, {"event": "close", "value": []}], "id": 0}`
//!
//! Note that it's important to move the protocol graph to its terminate state to prevent channel
//! leak.
//!
//! [jsonrpc]: http://www.jsonrpc.org/specification

// TODO: There are `println's` which help debugging a lot. May be transform then into debug logs?
// TODO: I can perform input sanity check by matching "chunks" with protocol to be sure that user reaches the terminate state.

use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::mem;

use futures::{future, stream, Future, Stream};
use futures::sync::oneshot;

use jsonrpc_core::{Call, Error, ErrorCode, Id, MethodCall, Output, Params, Request, Response, Success, Version};

use hyper::{self, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Response as HttpResponse, Request as HttpRequest};

use rmpv::ValueRef;

use serde_json as json;
use serde_json::Value;

use cocaine::{self, Dispatch, Service};
use cocaine::service::locator::{EventGraph, GraphNode};
use cocaine::logging::{Severity, Log};

use pool::{Event, EventDispatch, Settings};
use route::{Match, Route};

header! { (XJsonRpc, "X-Cocaine-JSON-RPC") => [i64] }

enum ServerErrorCode {
    InvalidMethodFormat,
    /// Service has failed to provide its methods as like as to perform connection.
    ServiceNotConnected,
    ProtocolViolation,
    Cocaine,
}

impl Into<Error> for ServerErrorCode {
    fn into(self) -> Error {
        let (code, message, data) = match self {
            ServerErrorCode::InvalidMethodFormat => (-32000, "Invalid method format", None),
            ServerErrorCode::ServiceNotConnected => (-32001, "Service is not connected", None),
            ServerErrorCode::ProtocolViolation => (-32002, "Protocol violation", None),
            ServerErrorCode::Cocaine => (-32099, "Generic Cocaine error", None),
        };

        Error {
            code: ErrorCode::ServerError(code),
            message: message.into(),
            data: data,
        }
    }
}

pub struct JsonRpc<L> {
    dispatcher: EventDispatch,
    log: L,
}

impl<L: Log> JsonRpc<L> {
    pub fn new(dispatcher: EventDispatch, log: L) -> Self {
        Self {
            dispatcher: dispatcher,
            log: log,
        }
    }
}

fn parse_method(method: &str) -> Result<(String, String), Error> {
    let mut parts = method.splitn(2, '.').collect::<Vec<&str>>();
    if parts.len() == 2 {
        let event = parts.pop().unwrap();
        let service = parts.pop().unwrap();

        Ok((service.to_owned(), event.to_owned()))
    } else {
        Err(ServerErrorCode::InvalidMethodFormat.into())
    }
}

#[derive(Serialize)]
struct Chunk {
    event: String,
    value: Value,
}

struct JsonDispatch {
    /// Notification.
    tx: oneshot::Sender<Output>,
    /// Request id.
    id: Id,
    /// Current graph state.
    graph: HashMap<u64, GraphNode>,
    /// Response chunks.
    chunks: Vec<Chunk>,
}

impl Dispatch for JsonDispatch {
    fn process(mut self: Box<Self>, ty: u64, response: &ValueRef) -> Option<Box<Dispatch>> {
        match self.graph.remove(&ty) {
            Some(graph) => {
                let GraphNode { event, rx } = graph;

                let chunk = Chunk {
                    event: event.clone(),
                    value: json::to_value(response.to_owned()).unwrap(),
                };

                self.chunks.push(chunk);

                match rx {
                    Some(ref graph) if graph.is_empty() => {
                        // We've reached the terminate state.
                        let succ = Success {
                            jsonrpc: Some(Version::V2),
                            result: json::to_value(mem::replace(&mut self.chunks, Vec::new())).unwrap(),
                            id: self.id.clone(),
                        };
                        let out = Output::Success(succ);
                        mem::drop(self.tx.send(out));
                        None
                    }
                    Some(graph) => {
                        self.graph = graph;
                        Some(self)
                    }
                    None => {
                        self.graph.insert(ty, GraphNode { event: event, rx: rx });
                        Some(self)
                    }
                }
            }
            None => {
                let err = ServerErrorCode::ProtocolViolation.into();
                let out = Output::from(Err(err), self.id.clone(), Some(Version::V2));
                mem::drop(self.tx.send(out));
                None
            }
        }
    }

    fn discard(self: Box<Self>, e: &cocaine::Error) {
        let mut err: Error = ServerErrorCode::Cocaine.into();
        err.data = Some(Value::String(e.to_string()));

        let out = Output::from(Err(err), self.id.clone(), Some(Version::V2));
        mem::drop(self.tx.send(out));
    }
}

impl<L: Log + Clone + Send + Sync + 'static> JsonRpc<L> {
    fn call(call: Call, d: EventDispatch)
        -> Box<Future<Item = Option<Output>, Error = hyper::Error> + Send>
    {
        match call {
            Call::MethodCall(call) => {
                let MethodCall { method, params, id, .. } = call;
                match parse_method(&method) {
                    Ok((service, event)) => {
                        let (params, chunks) = match params {
                            Some(Params::Array(args)) => (Some(args), None),
                            Some(Params::Map(mut params)) => {
                                match params.remove("args") {
                                    Some(Value::Array(args)) => {
                                        match params.remove("chunks") {
                                            Some(Value::Array(chunks)) => {
                                                (Some(args), Some(chunks))
                                            }
                                            Some(..) | None => (Some(args), None),
                                        }
                                    }
                                    Some(..) | None => {
                                        let err = Error::invalid_params("'args' parameter is required and must be an array");
                                        let out = Output::from(Err(err), id, Some(Version::V2));
                                        return future::ok(Some(out)).boxed()
                                    }
                                }
                            }
                            Some(Params::None) |
                            None => (None, None),
                        };

                        let (tx, future) = oneshot::channel();

                        let ev = Event::Service {
                            name: service,
                            func: box move |service: &Service, _settings: Settings| {
                                let methods = match service.methods() {
                                    Some(methods) => methods,
                                    None => {
                                        let service = service.clone();
                                        return service.connect().then(move |result| {
                                            match result {
                                                Ok(()) => {
                                                    match service.methods() {
                                                        Some(methods) => {
                                                            return Self::invoke(&service, event, params, chunks, id, methods, tx);
                                                        }
                                                        None => {
                                                            let err = ServerErrorCode::ServiceNotConnected.into();
                                                            let out = Output::from(Err(err), id, Some(Version::V2));
                                                            mem::drop(tx.send(out));
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    let mut err: Error = ServerErrorCode::ServiceNotConnected.into();
                                                    err.data = Some(Value::String(e.to_string()));

                                                    let out = Output::from(Err(err), id, Some(Version::V2));
                                                    mem::drop(tx.send(out));
                                                }
                                            }

                                            future::ok(()).boxed()
                                        }).boxed()
                                    }
                                };

                                Self::invoke(service, event, params, chunks, id, methods, tx)
                            },
                        };

                        d.send(ev);

                        future
                            .and_then(move |resp| Ok(Some(resp)))
                            .map_err(|err| {
                                hyper::Error::Io(io::Error::new(ErrorKind::Other, format!("{}", err)))
                            })
                            .boxed()
                    }
                    Err(err) => {
                        future::ok(Some(Output::from(Err(err), id, Some(Version::V2)))).boxed()
                    }
                }
            }
            Call::Notification(..) => {
                unimplemented!();
            }
            Call::Invalid(id) => {
                future::ok(Some(Output::invalid_request(id, Some(Version::V2)))).boxed()
            }
        }
    }

    fn invoke(service: &Service, event: String, params: Option<Vec<Value>>, chunks: Option<Vec<Value>>, id: Id, methods: HashMap<u64, EventGraph>, tx: oneshot::Sender<Output>)
        -> Box<Future<Item = (), Error = ()> + Send>
    {
        let methods = methods.into_iter()
            .map(|(ty, graph)| (graph.name.to_string(), (ty, graph)))
            .collect::<HashMap<String, (u64, EventGraph)>>();

//        println!("params={:?}, chunks={:?}", params, chunks);

        if let Some(&(ty, ref graph)) = methods.get(&event) {
            let headers = Vec::new();
            let dispatch = JsonDispatch {
                tx: tx,
                id: id,
                graph: graph.rx.clone(),
                chunks: Vec::new(),
            };

//            println!("ty={:?}, graph={:?}", ty, graph);

            if let Some(args) = params {
                let graph = graph.tx.clone();
                service.call(ty, &args, headers, dispatch).then(move |tx| {
//                    println!("After call");
                    let mut graph = &graph;

                    if let Ok(tx) = tx {
                        for chunk in chunks.unwrap_or_default() {
                            match chunk {
                                Value::Object(chunk) => {
                                    let mut iter = chunk.into_iter();
                                    if iter.len() == 1 {
                                        let (event, args) = iter.next().unwrap();
//                                        println!("Event={}, args={:?}", event, args);
                                        if let Some((ty, g)) = graph.iter().find(|&(.., ref g)| g.event == event) {
//                                            println!("Type={}, Graph={:?}", ty, g);
                                            match g.rx {
                                                Some(ref g) if g.is_empty() => {
                                                    tx.send(*ty, &args);
                                                    break;
                                                }
                                                Some(ref g) => {
                                                    graph = g;
                                                    tx.send(*ty, &args);
                                                }
                                                None => {
                                                    tx.send(*ty, &args);
                                                }
                                            }
                                        } else {
//                                            println!("event not found");
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    Ok(())
                }).boxed()
            } else {
                service.call(ty, &vec![0u8; 0], headers, dispatch).then(|tx| {
                    mem::drop(tx);
                    Ok(())
                }).boxed()
            }
        } else {
            let out = Output::from(Err(Error::method_not_found()), id, Some(Version::V2));
            mem::drop(tx.send(out));
            future::ok(()).boxed()
        }
    }

    fn make_response(resp: Vec<Option<Output>>) -> HttpResponse {
        let resp = resp.into_iter().filter_map(|v| v).collect::<Vec<Output>>();

        let body = if resp.is_empty() {
            json::to_vec(&Output::invalid_request(Id::Null, Some(Version::V2)))
        } else if resp.len() == 1 {
            json::to_vec(&resp.first())
        } else {
            json::to_vec(&resp)
        };

        let body = body.unwrap_or_default();
        let resp = HttpResponse::new()
            .with_status(StatusCode::Ok)
            .with_header(ContentType::json())
            .with_header(ContentLength(body.len() as u64))
            .with_body(body);
        resp
    }
}

impl<L: Log + Clone + Send + Sync + 'static> Route for JsonRpc<L> {
    type Future = Box<Future<Item = HttpResponse, Error = hyper::Error>>;

    fn process(&self, req: HttpRequest) -> Match<Self::Future> {
        if req.headers().has::<XJsonRpc>() {
            // TODO: Send 406 back if there are no "application/json or json-rpc Accept.
            let d = self.dispatcher.clone();
            let log = self.log.clone();

            // TODO: There is a bug in `futures 0.1` when `concat` panics on empty stream (#451).
            let future = req.body().concat2().and_then(move |data| {
                let req = match json::from_slice(&data) {
                    Ok(req) => req,
                    Err(..) => {
                        let resp = Response::from(Error::parse_error(), Some(Version::V2));
                        let body = json::to_vec(&resp).unwrap_or_default();
                        let resp = HttpResponse::new()
                            .with_status(StatusCode::BadRequest)
                            .with_header(ContentType::json())
                            .with_header(ContentLength(body.len() as u64))
                            .with_body(body);
                        return future::ok(resp).boxed();
                    }
                };

                cocaine_log!(log, Severity::Debug, "received JSON RPC: {:?}", req);

                let calls = match req {
                    Request::Single(call) => vec![call],
                    Request::Batch(calls) => calls,
                };

                stream::iter_ok(calls.into_iter())
                    .and_then(move |call| Self::call(call, d.clone()))
                    .collect()
                    .and_then(|resp| Ok(Self::make_response(resp)))
                    .boxed()
            });

            Match::Some(future.boxed())
        } else {
            Match::None(req)
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem;
    use std::str::{self, FromStr};

    use futures::{Future, Stream};
    use futures::sync::mpsc;

    use hyper;
    use hyper::{Body, Method, StatusCode, Uri};
    use hyper::server::{Response, Request};

    use cocaine::logging::{Severity, FilterResult, Log};

    use pool::EventDispatch;
    use route::Route;
    use super::{JsonRpc, XJsonRpc};

    #[derive(Clone)]
    struct MockLogger;

    impl Log for MockLogger {
        fn source(&self) -> &str { "" }

        fn filter(&self, _sev: Severity) -> FilterResult {
            FilterResult::Reject
        }
    }

    #[test]
    fn route_none() {
        let req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);

        assert!(service.process(req).is_none());
        mem::drop(rx);
    }

    #[test]
    fn invalid_json() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"{"jsonrpc":"2.0","method":"foobar,"params":"bar","baz]"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::BadRequest, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(r#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#,
            str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn invalid_request() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"{"jsonrpc":"2.0","method":1,"params":"bar"}"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::Ok, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":null}"#,
            str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn batch_invalid_json() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"[
            {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
            {"jsonrpc": "2.0", "method"
        ]"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::BadRequest, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(r#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#,
        str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn empty_array() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"[]"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::Ok, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":null}"#,
        str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn batch_invalid_one() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"[1]"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::Ok, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":null}"#,
        str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn batch_invalid_many() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"[1,2,3]"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::Ok, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(&format!("[{},{},{}]",
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":null}"#,
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":null}"#,
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"},"id":null}"#,
        ),
        str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn call_invalid_method_format() {
        let mut req: Request<Body> = Request::new(Method::Post, Uri::from_str("/").unwrap());
        req.headers_mut().set(XJsonRpc(1));
        req.set_body(r#"{"jsonrpc":"2.0","method":"storage","params":["collection","key"],"id":1}"#);

        let (tx, rx) = mpsc::unbounded();
        let dispatch = EventDispatch::new(vec![tx]);

        let service = JsonRpc::new(dispatch, MockLogger);
        let future: Box<Future<Item = Response, Error = hyper::Error>> = service.process(req).unwrap();
        let res = future.wait().unwrap();

        assert_eq!(StatusCode::Ok, res.status());
        let body = res.body().concat2().wait().unwrap();

        assert_eq!(r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"Invalid method format"},"id":1}"#,
        str::from_utf8(body.as_ref()).unwrap());

        mem::drop(service);
        assert!(rx.wait().collect::<Vec<_>>().is_empty());
    }
}
