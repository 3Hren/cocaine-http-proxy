//! Contain a route that is used primarily for performance measuring.
//!
//! Currently all requests are transformed into a Geobase requests.

use std::io::{self, ErrorKind};

use futures::Future;
use futures::sync::oneshot;

use hyper::{self, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Response, Request};

use rmpv::ValueRef;

use cocaine::{Dispatch, Error, Service};
use cocaine::logging::Logger;

use logging::AccessLogger;
use pool::{Event, EventDispatch, Settings};
use route::{Match, Route};

pub struct PerfRoute {
    dispatcher: EventDispatch,
    log: Logger,
}

impl PerfRoute {
    pub fn new(dispatcher: EventDispatch, log: Logger) -> Self {
        Self {
            dispatcher: dispatcher,
            log: log,
        }
    }
}

impl Route for PerfRoute {
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn process(&self, req: Request) -> Match<Self::Future> {
        let (tx, rx) = oneshot::channel();

        let ev = Event::Service {
            name: "geobase".into(),
            func: box move |service: &Service, _settings: Settings| {
                let future = service.call(0, &vec!["8.8.8.8"], Vec::new(), SingleChunkReadDispatch { tx: tx })
                    .then(|tx| {
                        drop(tx);
                        Ok(())
                    });
                box future as Box<Future<Item = (), Error = ()> + Send>
            },
        };

        self.dispatcher.send(ev);

        let log = AccessLogger::new(self.log.clone(), &req, "geobase".to_owned(), "ip".to_owned(), 0);
        let future = rx.and_then(move |(mut res, bytes_sent)| {
            res.headers_mut().set_raw("X-Powered-By", "Cocaine");
            log.commit(res.status().into(), bytes_sent, None);
            Ok(res)
        }).map_err(|err| hyper::Error::Io(io::Error::new(ErrorKind::Other, format!("{}", err))));

        Match::Some(box future)
    }
}

pub struct SingleChunkReadDispatch {
    tx: oneshot::Sender<(Response, u64)>,
}

impl Dispatch for SingleChunkReadDispatch {
    fn process(self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        let (code, body) = match ty {
            0 => {
                (StatusCode::Ok, format!("{}", data))
            }
            1 => {
                (StatusCode::InternalServerError, format!("{}", data))
            }
            m => {
                (StatusCode::InternalServerError, format!("unknown type: {} {}", m, data))
            }
        };

        let body_len = body.as_bytes().len() as u64;

        let res = Response::new()
            .with_status(code)
            .with_header(ContentLength(body_len))
            .with_body(body);

        drop(self.tx.send((res, body_len)));

        None
    }

    fn discard(self: Box<Self>, err: &Error) {
        let body = format!("{}", err);
        let body_len = body.as_bytes().len() as u64;

        let mut res = Response::new();
        res.set_status(StatusCode::InternalServerError);
        res.set_body(body);

        drop(self.tx.send((res, body_len)));
    }
}
