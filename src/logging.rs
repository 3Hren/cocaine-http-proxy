use std::time::Instant;

use hyper::server::Request;

use cocaine::logging::{Logger, LoggerContext, Severity};

use config::{LoggingBaseConfig, LoggingConfig};

#[derive(Clone, Debug)]
pub struct Loggers {
    common: Logger,
    access: Logger,
}

impl Loggers {
    pub fn common(&self) -> &Logger {
        &self.common
    }

    pub fn access(&self) -> &Logger {
        &self.access
    }
}

impl<'a> From<&'a LoggingConfig> for Loggers {
    fn from(config: &'a LoggingConfig) -> Self {
        let factory = |cfg: &LoggingBaseConfig| {
            let ctx = LoggerContext::new(cfg.name().to_owned());
            ctx.filter().set(cfg.severity().into());
            ctx.create(cfg.source().to_owned())
        };

        Self {
            common: factory(config.common()),
            access: factory(config.access()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccessLogger {
    birth: Instant,
    method: String,
    path: String,
    version: String,
    log: Logger,
}

impl AccessLogger {
    pub fn new(log: Logger, req: &Request) -> Self {
        Self {
            birth: Instant::now(),
            method: req.method().as_ref().to_owned(),
            path: req.path().to_owned(),
            version: format!("HTTP/1.{}", req.version()),
            log: log,
        }
    }

    pub fn commit(self, trace: usize, status: u32, bytes_sent: u64) {
        let elapsed = self.birth.elapsed();
        let elapsed_ms = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64 / 1e6;

        cocaine_log!(self.log, Severity::Info, "request finished in {:.3} ms", [elapsed_ms], {
            request_id: trace,
            duration: elapsed_ms / 1000.0,
            method: self.method,
            path: self.path,
            version: self.version,
            status: status,
            bytes_sent: bytes_sent,
        });
    }
}
