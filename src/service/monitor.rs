use std::io;
use std::net::SocketAddr;

use futures::future;

use tokio_core::reactor::Handle;
use tokio_service::Service;

use hyper::{self, Method, StatusCode};
use hyper::server::{Request, Response};

use service::{ServiceFactory, ServiceFactorySpawn};

#[derive(Debug)]
pub struct MonitorService;

impl Service for MonitorService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = future::FutureResult<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let res = match (req.method(), req.path()) {
            (&Method::Get, "/ping") => Response::new().with_status(StatusCode::Ok),
            (..) => Response::new().with_status(StatusCode::NotFound),
        };

        future::ok(res)
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactory;

impl ServiceFactory for MonitorServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = MonitorService;
    type Error    = hyper::Error;

    fn create_service(&mut self, _addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        Ok(MonitorService)
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactoryFactory;

impl ServiceFactorySpawn for MonitorServiceFactoryFactory {
    type Factory = MonitorServiceFactory;

    fn create_factory(&self, _handle: &Handle) -> Self::Factory {
        MonitorServiceFactory
    }
}
