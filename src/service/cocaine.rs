use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

use futures::{future, Future};
use futures::sync::mpsc;
use tokio_core::reactor::Handle;
use tokio_service::Service;

use hyper::{self, StatusCode};
use hyper::server::{Request, Response};

use cocaine::logging::{Severity, Logger};

use Metrics;
use config::Config;
use service::{ServiceFactory, ServiceFactorySpawn};
use route::Route;
use pool::{Event, PoolTask};
use metrics::{Meter, Count};

pub struct ProxyService {
    log: Logger,
    metrics: Arc<Metrics>,
    addr: Option<SocketAddr>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl Service for ProxyService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        self.metrics.requests.mark(1);

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
        if let Some(addr) = self.addr.take() {
            cocaine_log!(self.log, Severity::Info, "closed connection from {}", addr);
        } else {
            cocaine_log!(self.log, Severity::Info, "closed connection from Unix socket");
        }

        self.metrics.connections.active.add(-1);
    }
}

#[derive(Clone)]
pub struct ProxyServiceFactory {
    log: Logger,
    metrics: Arc<Metrics>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
}

impl ServiceFactory for ProxyServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = ProxyService;
    type Error    = hyper::Error;

    fn create_service(&mut self, addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        self.metrics.connections.active.add(1);
        self.metrics.connections.accepted.add(1);
        if let Some(addr) = addr {
            cocaine_log!(self.log, Severity::Info, "accepted connection from {}", addr);
        } else {
            cocaine_log!(self.log, Severity::Info, "accepted connection from Unix socket");
        }

        let service = ProxyService {
            log: self.log.clone(),
            metrics: self.metrics.clone(),
            addr: addr,
            routes: self.routes.clone(),
        };

        Ok(service)
    }
}

pub struct ProxyServiceFactoryFactory {
    tx: Mutex<IntoIter<mpsc::UnboundedSender<Event>>>,
    rx: Mutex<IntoIter<mpsc::UnboundedReceiver<Event>>>,
    log: Logger,
    metrics: Arc<Metrics>,
    routes: Vec<Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>>,
    cfg: Config,
}

impl ProxyServiceFactoryFactory {
    pub fn new(txs: Vec<mpsc::UnboundedSender<Event>>, rxs: Vec<mpsc::UnboundedReceiver<Event>>, log: Logger, metrics: Arc<Metrics>, routes: Vec<Arc<Route<Future=Box<Future<Item = Response, Error = hyper::Error>>>>>, cfg: Config) -> Self {
        Self {
            tx: Mutex::new(txs.clone().into_iter()),
            rx: Mutex::new(rxs.into_iter()),
            log: log,
            metrics: metrics,
            routes: routes,
            cfg: cfg,
        }
    }
}

impl ServiceFactorySpawn for ProxyServiceFactoryFactory {
    type Factory = ProxyServiceFactory;

    fn create_factory(&self, handle: &Handle) -> Self::Factory {
        let tx = self.tx.lock().unwrap().next()
            .expect("number of event channels must be exactly the same as the number of threads");
        let rx = self.rx.lock().unwrap().next()
            .expect("number of event channels must be exactly the same as the number of threads");

        // This will stop after all associated connections are closed.
        let pool = PoolTask::new(handle.clone(), self.log.clone(), tx, rx, self.cfg.clone());

        handle.spawn(pool);
        ProxyServiceFactory {
            log: self.log.clone(),
            metrics: self.metrics.clone(),
            routes: self.routes.clone(),
        }
    }
}
