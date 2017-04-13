use std::io;
use std::net::SocketAddr;

use futures::future;

use tokio_core::reactor::Handle;
use tokio_service::Service;

use hyper::{self, Method, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Request, Response};

use serde_json;

use config::Config;
use service::{ServiceFactory, ServiceFactorySpawn};

#[derive(Debug)]
pub struct MonitorService {
    config: Config,
}

impl MonitorService {
    pub fn new(config: Config) -> Self {
        Self {
            config: config,
        }
    }
}

impl Service for MonitorService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = future::FutureResult<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let res = match (req.method(), req.path()) {
            (&Method::Get, "/ping") => Response::new().with_status(StatusCode::Ok),
            (&Method::Get, "/config") => {
                match serde_json::to_string(&self.config) {
                    Ok(body) => {
                        Response::new()
                            .with_status(StatusCode::Ok)
                            .with_header(ContentLength(body.len() as u64))
                            .with_body(body)
                    }
                    Err(err) => {
                        Response::new()
                            .with_status(StatusCode::InternalServerError)
                            .with_body(format!("{}", err))
                    }
                }
            }
            (..) => Response::new().with_status(StatusCode::NotFound),
        };

        future::ok(res)
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactory {
    config: Config,
}

impl ServiceFactory for MonitorServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = MonitorService;
    type Error    = hyper::Error;

    fn create_service(&mut self, _addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        Ok(MonitorService::new(self.config.clone()))
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactoryFactory {
    config: Config,
}

impl MonitorServiceFactoryFactory {
    pub fn new(config: Config) -> Self {
        Self {
            config: config,
        }
    }
}

impl ServiceFactorySpawn for MonitorServiceFactoryFactory {
    type Factory = MonitorServiceFactory;

    fn create_factory(&self, _handle: &Handle) -> Self::Factory {
        MonitorServiceFactory { config: self.config.clone() }
    }
}
