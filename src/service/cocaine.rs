use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::IntoIter;

use futures::{future, Future};
use futures::sync::mpsc;
use tokio_core::reactor::{Handle, Timeout};
use tokio_service::Service;

use hyper::{self, StatusCode};
use hyper::server::{Request, Response};

use cocaine::logging::{Severity, Logger};

use Metrics;
use config::Config;
use metrics::{Meter, Count};
use pool::{Event, PoolTask};
use route::Router;
use service::{ServiceFactory, ServiceFactorySpawn};

pub struct ProxyService {
    addr: Option<SocketAddr>,
    router: Router,
    metrics: Arc<Metrics>,
    log: Logger,
}

impl Service for ProxyService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        self.metrics.requests.mark(1);
        self.router.process(req)
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

pub struct TimedOut;

impl From<TimedOut> for Response {
    fn from(timeout: TimedOut) -> Self {
        match timeout {
            TimedOut => {
                Response::new()
                    .with_status(StatusCode::GatewayTimeout)
                    .with_body("Timed out while waiting for response from the Cocaine")
            }
        }
    }
}

pub struct TimeoutMiddleware<T> {
    upstream: T,
    timeout: Duration,
    handle: Handle,
}

impl<T> TimeoutMiddleware<T> {
    fn new(upstream: T, timeout: Duration, handle: Handle) -> Self {
        Self {
            upstream: upstream,
            timeout: timeout,
            handle: handle,
        }
    }
}

impl<T> Service for TimeoutMiddleware<T>
    where T: Service,
          T::Response: From<TimedOut>,
          T::Error: From<io::Error> + 'static,
          T::Future: 'static
{
    type Request  = T::Request;
    type Response = T::Response;
    type Error    = T::Error;
    type Future   = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let timeout = future::result(Timeout::new(self.timeout, &self.handle))
            .flatten()
            .map(|()| Self::Response::from(TimedOut))
            .map_err(From::from);

        let future = self.upstream.call(req)
            .select(timeout)
            .map(|v| v.0)
            .map_err(|e| e.0);

        Box::new(future)
    }
}

#[derive(Clone)]
pub struct ProxyServiceFactory {
    router: Router,
    timeout: Duration,
    handle: Handle,
    metrics: Arc<Metrics>,
    log: Logger,
}

impl ServiceFactory for ProxyServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = TimeoutMiddleware<ProxyService>;
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
            addr: addr,
            router: self.router.clone(),
            metrics: self.metrics.clone(),
            log: self.log.clone(),
        };

        let wrapped = TimeoutMiddleware::new(service, self.timeout, self.handle.clone());

        Ok(wrapped)
    }
}

pub struct ProxyServiceFactoryFactory {
    tx: Mutex<IntoIter<mpsc::UnboundedSender<Event>>>,
    rx: Mutex<IntoIter<mpsc::UnboundedReceiver<Event>>>,
    cfg: Config,
    router: Router,
    metrics: Arc<Metrics>,
    log: Logger,
}

impl ProxyServiceFactoryFactory {
    pub fn new(txs: Vec<mpsc::UnboundedSender<Event>>,
               rxs: Vec<mpsc::UnboundedReceiver<Event>>,
               cfg: Config,
               router: Router,
               metrics: Arc<Metrics>,
               log: Logger) -> Self
    {
        Self {
            tx: Mutex::new(txs.clone().into_iter()),
            rx: Mutex::new(rxs.into_iter()),
            cfg: cfg,
            router: router,
            metrics: metrics,
            log: log,
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
            router: self.router.clone(),
            timeout: self.cfg.timeout(),
            handle: handle.clone(),
            metrics: self.metrics.clone(),
            log: self.log.clone(),
        }
    }
}
