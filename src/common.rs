use std::fmt;
use std::str::{self, FromStr};

use hyper;
use hyper::header::{self, Header, Raw};

header! { (XCocaineService, "X-Cocaine-Service") => [String] }
header! { (XCocaineEvent, "X-Cocaine-Event") => [String] }
header! { (XPoweredBy, "X-Powered-By") => [String] }

impl Default for XPoweredBy {
    fn default() -> Self {
        XPoweredBy(format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct XRequestId(pub u64);

impl Into<u64> for XRequestId {
    fn into(self) -> u64 {
        match self {
            XRequestId(v) => v,
        }
    }
}

impl Header for XRequestId {
    fn header_name() -> &'static str {
        "X-Request-Id"
    }

    fn parse_header(raw: &Raw) -> Result<XRequestId, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                if let Ok(val) = u64::from_str_radix(line, 16) {
                    return Ok(XRequestId(val));
                }
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&format!("{:x}", self.0))
    }
}

/// Describes how a request should be traced.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TracingPolicy {
    /// Use automatically configured settings from the proxy.
    Auto,
    /// Use tracing chance value provided by the user. Must be in [0.0; 1.0] range.
    Manual(f64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct XTracingPolicy(pub TracingPolicy);

impl Into<TracingPolicy> for XTracingPolicy {
    fn into(self) -> TracingPolicy {
        self.0
    }
}

impl Header for XTracingPolicy {
    fn header_name() -> &'static str {
        "X-Cocaine-Tracing-Policy"
    }

    fn parse_header(raw: &Raw) -> Result<XTracingPolicy, hyper::Error> {
        if let Some(line) = raw.one() {
            if line == b"Auto" {
                Ok(XTracingPolicy(TracingPolicy::Auto))
            } else {
                if let Ok(line) = str::from_utf8(line) {
                    if let Ok(val) = f64::from_str(line) {
                        if 0.0 <= val && val <= 1.0 {
                            return Ok(XTracingPolicy(TracingPolicy::Manual(val)))
                        }
                    }
                }

                Err(hyper::Error::Header)
            }
        } else {
            Err(hyper::Error::Header)
        }
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        match *self {
            XTracingPolicy(TracingPolicy::Auto) => fmt.fmt_line(&"Auto"),
            XTracingPolicy(TracingPolicy::Manual(v)) => fmt.fmt_line(&format!("{:.3}", v)),
        }
    }
}
