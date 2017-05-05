use std::io::{self, ErrorKind};

use rand;

use futures::{future, Future};
use futures::sync::{oneshot, mpsc};

use hyper::{self, StatusCode};
use hyper::server::{Response, Request};

use rmps;
use rmpv::ValueRef;

use cocaine::{Dispatch, Error, Service};
use cocaine::logging::Logger;
use cocaine::protocol::{self, Flatten};

use logging::AccessLogger;
use pool::Event;
use route::Route;

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }

pub struct AppRoute {
    txs: Vec<mpsc::UnboundedSender<Event>>,
    log: Logger,
}

impl AppRoute {
    pub fn new(txs: Vec<mpsc::UnboundedSender<Event>>, log: Logger) -> Self {
        Self {
            txs: txs,
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

                let (tx, rx) = oneshot::channel();

                let ev = Event::Service {
                    name: service,
                    func: box move |service: &Service| {
                        let future = service.call(0, &vec![event], Vec::new(), AppReadDispatch {
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
