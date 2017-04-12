use std::io;
use std::net::SocketAddr;

use futures::{future, Future};

use tokio_core::reactor::Handle;
use tokio_service::Service;

use hyper::{self, Method, StatusCode};
use hyper::server::{Request, Response};

use service::{ServiceFactory, ServiceFactoryFactory};

#[derive(Debug)]
pub struct Handler;

impl Service for Handler {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item=Response, Error=Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let res = match (req.method(), req.path()) {
            (&Method::Get, "/ping") => Response::new().with_status(StatusCode::Ok),
            (..) => Response::new().with_status(StatusCode::NotFound),
        };

        box future::ok(res)
    }
}

#[derive(Debug)]
pub struct MonitoringServiceFactory;

impl ServiceFactory for MonitoringServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = Handler;
    type Error    = hyper::Error;

    fn create_service(&mut self, _addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        Ok(Handler)
    }
}

#[derive(Debug)]
pub struct MonitoringServiceFactoryFactory;

impl ServiceFactoryFactory for MonitoringServiceFactoryFactory {
    type Factory = MonitoringServiceFactory;

    fn create_factory(&self, _handle: &Handle) -> Self::Factory {
        MonitoringServiceFactory
    }
}
