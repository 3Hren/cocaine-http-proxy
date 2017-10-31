use std::error::Error;
use std::fmt::{self, Debug, Formatter};
use std::time::Instant;

use hyper::{Method, StatusCode, Uri};
use hyper::server::Request;

use cocaine::logging::{Filter, Log, Logger, LoggerContext, Severity};

use config::{LoggingBaseConfig, LoggingConfig};

#[derive(Clone, Debug)]
pub struct Entry {
    logger: Logger,
    filter: Filter,
}

impl Entry {
    /// Constructs a new logging entry with its own context using the specified configuration.
    fn new(cfg: &LoggingBaseConfig) -> Self {
        let ctx = LoggerContext::new(cfg.name().to_owned());
        let log = Self {
            logger: ctx.create(cfg.source().to_owned()),
            filter: ctx.filter().clone(),
        };

        log.filter.set(cfg.severity().into());

        log
    }

    /// Returns a logger reference this entry owns.
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    /// Returns a logger filter this entry owns.
    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}

#[derive(Clone, Debug)]
pub struct Loggers {
    common: Entry,
    access: Entry
}

impl Loggers {
    /// Returns a reference to the common logger with its filter that is used for general purpose
    /// logging.
    pub fn common(&self) -> &Entry {
        &self.common
    }

    /// Returns a reference to the access logger with its filter that is used for logging summaries
    /// of HTTP responses.
    pub fn access(&self) -> &Entry {
        &self.access
    }
}

impl<'a> From<&'a LoggingConfig> for Loggers {
    fn from(cfg: &'a LoggingConfig) -> Self {
        Self {
            common: Entry::new(cfg.common()),
            access: Entry::new(cfg.access()),
        }
    }
}

/// Helper for safe debug `Request` formatting without panicking on errors.
struct SafeRequestDebug<'a>(&'a Request);

impl<'a> Debug for SafeRequestDebug<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        match write!(fmt, "{:?}", self.0) {
            Ok(()) => Ok(()),
            Err(err) => {
                write!(fmt, "failed to format `Request` using Debug trait: {}", err)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccessLogger<L> {
    birth: Instant,
    method: Method,
    uri: Uri,
    version: String,
    service: String,
    event: String,
    trace: u64,
    log: L,
}

impl<L: Log> AccessLogger<L> {
    pub fn new(log: L, req: &Request, service: String, event: String, trace: u64) -> Self {
        cocaine_log!(log, Severity::Debug, "processing HTTP request"; {
            service: service,
            event: event,
            trace: trace,
            trace_id: format!("{:016x}", trace),
            request: format!("{:?}", SafeRequestDebug(req)),
        });

        Self {
            birth: Instant::now(),
            method: req.method().clone(),
            uri: req.uri().clone(),
            version: format!("{}", req.version()),
            service: service,
            event: event,
            trace: trace,
            log: log,
        }
    }

    pub fn commit(self, status: StatusCode, bytes_sent: u64, err: Option<&Error>) {
        let status: u16 = status.into();
        let elapsed = self.birth.elapsed();
        let elapsed_ms = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64 / 1e6;

        cocaine_log!(self.log, Severity::Info, "request finished in {:.3} ms", elapsed_ms; {
            trace: self.trace,
            trace_id: format!("{:016x}", self.trace),
            duration: elapsed_ms / 1000.0,
            method: self.method.to_string(),
            uri: self.uri.to_string(),
            version: self.version,
            status: status,
            bytes_sent: bytes_sent,
            service: self.service,
            event: self.event,
            error: err.map(|e| e.description()).unwrap_or("No error"),
        });
    }
}
