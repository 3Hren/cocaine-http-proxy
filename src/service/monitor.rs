use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future;

use serde::Serialize;

use tokio_core::reactor::Handle;
use tokio_service::Service;

use hyper::{self, Method, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Request, Response};

use serde_json;

use config::Config;
use proxy::Metrics;
use service::{ServiceFactory, ServiceFactorySpawn};

#[derive(Debug)]
pub struct MonitorService {
    config: Config,
    metrics: Arc<Metrics>,
}

impl MonitorService {
    pub fn new(config: Config, metrics: Arc<Metrics>) -> Self {
        Self {
            config: config,
            metrics: metrics,
        }
    }

    fn to_json<T: Serialize>(value: &T) -> Response {
        match serde_json::to_string(value) {
            Ok(body) => {
                Response::new()
                    .with_status(StatusCode::Ok)
                    .with_header(ContentType::json())
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
}

impl Service for MonitorService {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = future::FutureResult<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let res = match (req.method(), req.path()) {
            (&Method::Get, "/ping") => Response::new().with_status(StatusCode::Ok),
            (&Method::Get, "/config") => MonitorService::to_json(&self.config),
            (&Method::Get, "/metrics") => MonitorService::to_json(&self.metrics),
            (..) => Response::new().with_status(StatusCode::NotFound),
        };

        future::ok(res)
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactory {
    config: Config,
    metrics: Arc<Metrics>,
}

impl ServiceFactory for MonitorServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = MonitorService;
    type Error    = hyper::Error;

    fn create_service(&mut self, _addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        Ok(MonitorService::new(self.config.clone(), self.metrics.clone()))
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactoryFactory {
    config: Config,
    metrics: Arc<Metrics>,
}

impl MonitorServiceFactoryFactory {
    pub fn new(config: Config, metrics: Arc<Metrics>) -> Self {
        Self {
            config: config,
            metrics: metrics,
        }
    }
}

impl ServiceFactorySpawn for MonitorServiceFactoryFactory {
    type Factory = MonitorServiceFactory;

    fn create_factory(&self, _handle: &Handle) -> Self::Factory {
        MonitorServiceFactory {
            config: self.config.clone(),
            metrics: self.metrics.clone(),
        }
    }
}
