use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

use futures::Future;
use futures::sync::mpsc;
use tokio_core::reactor::Handle;
use tokio_service::Service;

use hyper;
use hyper::server::{Request, Response};

use cocaine::logging::{Severity, Logger};

use Metrics;
use config::Config;
use metrics::{Meter, Count};
use pool::{Event, PoolTask};
use route::Router;
use service::{ServiceFactory, ServiceFactorySpawn};

pub struct ProxyService {
    log: Logger,
    metrics: Arc<Metrics>,
    addr: Option<SocketAddr>,
    router: Router,
}

impl Service for ProxyService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        self.metrics.requests.mark(1);
        self.router.process(&req)
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
    router: Router,
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
            router: self.router.clone(),
        };

        Ok(service)
    }
}

pub struct ProxyServiceFactoryFactory {
    tx: Mutex<IntoIter<mpsc::UnboundedSender<Event>>>,
    rx: Mutex<IntoIter<mpsc::UnboundedReceiver<Event>>>,
    log: Logger,
    metrics: Arc<Metrics>,
    router: Router,
    cfg: Config,
}

impl ProxyServiceFactoryFactory {
    pub fn new(txs: Vec<mpsc::UnboundedSender<Event>>, rxs: Vec<mpsc::UnboundedReceiver<Event>>, log: Logger, metrics: Arc<Metrics>, router: Router, cfg: Config) -> Self {
        Self {
            tx: Mutex::new(txs.clone().into_iter()),
            rx: Mutex::new(rxs.into_iter()),
            log: log,
            metrics: metrics,
            router: router,
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
            router: self.router.clone(),
        }
    }
}
