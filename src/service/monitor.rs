use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use cocaine::logging::Filter;

use futures::future;

use hyper::{self, Method, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Request, Response};

use regex::Regex;

use serde::Serialize;
use serde_json;

use tokio_core::reactor::Handle;
use tokio_service::Service;

use Metrics;
use config::Config;
use logging::Loggers;
use service::{ServiceFactory, ServiceFactorySpawn};

fn response_json<T: Serialize>(value: &T) -> Response {
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

#[derive(Debug)]
pub struct MonitorService {
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    loggers: Arc<Loggers>,
    regex: Regex,
}

impl MonitorService {
    pub fn new(config: Arc<Config>, loggers: Arc<Loggers>, metrics: Arc<Metrics>) -> Self {
        Self {
            config: config,
            metrics: metrics,
            loggers: loggers,
            regex: Regex::new("/v1/severity/(?P<logger>[^/]*)/(?P<severity>\\d)")
                .expect("invalid URI regex in monitoring"),
        }
    }
}

fn match_severity(sev: isize) -> bool {
    0 <= sev && sev <= 3
}

enum Error<'a> {
    LoggerNotFound(&'a str),
    SeverityNotInRange(isize),
    InvalidSeverity,
}

impl<'a> Into<Response> for Error<'a> {
    fn into(self) -> Response {
        let description = match self {
            Error::LoggerNotFound(logger) => format!("Logger `{}` not found", logger),
            Error::SeverityNotInRange(..) => format!("Severity value must be in [0; 3] range"),
            Error::InvalidSeverity => format!("Severity value must be an integer"),
        };

        Response::new()
            .with_status(StatusCode::BadRequest)
            .with_header(ContentType::plaintext())
            .with_header(ContentLength(description.len() as u64))
            .with_body(description)
    }
}

fn extract_filter<'a>(loggers: &'a Loggers, name: &'a str) -> Result<&'a Filter, Error<'a>> {
    match name {
        "common" => Ok(loggers.common_filter()),
        "access" => Ok(loggers.access_filter()),
        name => Err(Error::LoggerNotFound(name).into()),
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
            (&Method::Get, "/config") => response_json(&*self.config),
            (&Method::Get, "/metrics") => response_json(&*self.metrics),
            (&Method::Get, "/v1/severity/common") => {
                response_json(&self.loggers.common_filter().get())
            }
            (&Method::Get, "/v1/severity/access") => {
                response_json(&self.loggers.access_filter().get())
            }
            (&Method::Put, path) if self.regex.is_match(path) => {
                match self.regex.captures(path) {
                    Some(captures) => {
                        match extract_filter(&self.loggers, &captures["logger"]) {
                            Ok(filter) => {
                                match FromStr::from_str(&captures["severity"]) {
                                    Ok(sev) if match_severity(sev) => {
                                        filter.set(sev);
                                        Response::new()
                                            .with_status(StatusCode::Ok)
                                    }
                                    Ok(sev) => Error::SeverityNotInRange(sev).into(),
                                    Err(..) => Error::InvalidSeverity.into(),
                                }
                            }
                            Err(err) => err.into(),
                        }
                    }
                    None => Response::new().with_status(StatusCode::NotFound),
                }
            }
            (..) => Response::new().with_status(StatusCode::NotFound),
        };

        future::ok(res)
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactory {
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    loggers: Arc<Loggers>,
}

impl ServiceFactory for MonitorServiceFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = MonitorService;
    type Error    = hyper::Error;

    fn create_service(&mut self, _addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        Ok(MonitorService::new(self.config.clone(), self.loggers.clone(), self.metrics.clone()))
    }
}

#[derive(Debug)]
pub struct MonitorServiceFactoryFactory {
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    loggers: Arc<Loggers>,
}

impl MonitorServiceFactoryFactory {
    pub fn new(config: Arc<Config>, loggers: Arc<Loggers>, metrics: Arc<Metrics>) -> Self {
        Self {
            config: config,
            metrics: metrics,
            loggers: loggers.clone(),
        }
    }
}

impl ServiceFactorySpawn for MonitorServiceFactoryFactory {
    type Factory = MonitorServiceFactory;

    fn create_factory(&self, _handle: &Handle) -> Self::Factory {
        MonitorServiceFactory {
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            loggers: self.loggers.clone(),
        }
    }
}
