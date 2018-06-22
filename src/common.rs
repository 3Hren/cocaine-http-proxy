use std::fmt;
use std::str::{self, FromStr};

use hyper;
use hyper::header::{self, Header, Raw};

#[derive(Clone, Debug, PartialEq)]
pub struct XCocaineService(pub String);

impl Header for XCocaineService {
    fn header_name() -> &'static str {
        "X-Cocaine-Service"
    }

    fn parse_header(raw: &Raw) -> Result<Self, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                return Ok(XCocaineService(line.into()))
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&self.0)
    }
}

impl XCocaineService {
    pub fn to_string(&self) -> String {
        match *self {
            XCocaineService(ref v) => v.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct XCocaineEvent(pub String);

impl Header for XCocaineEvent {
    fn header_name() -> &'static str {
        "X-Cocaine-Event"
    }

    fn parse_header(raw: &Raw) -> Result<Self, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                return Ok(XCocaineEvent(line.into()))
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&self.0)
    }
}

impl XCocaineEvent {
    pub fn to_string(&self) -> String {
        match *self {
            XCocaineEvent(ref v) => v.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct XPoweredBy(pub String);

impl Header for XPoweredBy {
    fn header_name() -> &'static str {
        "X-Powered-By"
    }

    fn parse_header(raw: &Raw) -> Result<Self, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                return Ok(XPoweredBy(line.into()))
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&self.0)
    }
}

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
        fmt.fmt_line(&format!("{:016x}", self.0))
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

// TODO: Almost the same header already exists for service, but it's here for compatibility with tornado.
#[derive(Clone, Debug, PartialEq)]
pub struct XCocaineApp(pub String);

impl Header for XCocaineApp {
    fn header_name() -> &'static str {
        "X-Cocaine-Application"
    }

    fn parse_header(raw: &Raw) -> Result<Self, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                return Ok(XCocaineApp(line.into()))
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct XErrorGeneratedBy(pub String);

impl Header for XErrorGeneratedBy {
    fn header_name() -> &'static str {
        "X-Error-Generated-By"
    }

    fn parse_header(raw: &Raw) -> Result<Self, hyper::Error> {
        if let Some(line) = raw.one() {
            if let Ok(line) = str::from_utf8(line) {
                return Ok(XErrorGeneratedBy(line.into()));
            }
        }

        Err(hyper::Error::Header)
    }

    fn fmt_header(&self, fmt: &mut header::Formatter) -> Result<(), fmt::Error> {
        fmt.fmt_line(&self.0)
    }
}

#[cfg(test)]
mod test {
    use hyper::header::Raw;

    use super::*;

    #[test]
    fn test_request_id_header() {
        let header = XRequestId::parse_header(&Raw::from("2a")).unwrap();
        let value: u64 = header.into();
        assert_eq!(42, value);
    }

    #[test]
    fn test_request_id_header_offset() {
        let header = XRequestId::parse_header(&Raw::from("0000002a")).unwrap();
        let value: u64 = header.into();
        assert_eq!(42, value);
    }

    #[test]
    fn test_request_id_header_real() {
        let header = XRequestId::parse_header(&Raw::from("fc1d162f7797fba1")).unwrap();
        let value: u64 = header.into();
        assert_eq!(18166700865008171937, value);
    }

    #[test]
    fn test_request_id_header_err() {
        assert!(XRequestId::parse_header(&Raw::from("")).is_err());
        assert!(XRequestId::parse_header(&Raw::from("0x42")).is_err());
        assert!(XRequestId::parse_header(&Raw::from("damn")).is_err());
    }

    #[test]
    fn test_tracing_policy_header() {
        let header = XTracingPolicy::parse_header(&Raw::from("Auto")).unwrap();
        let value: TracingPolicy = header.into();
        assert_eq!(TracingPolicy::Auto, value);
    }

    #[test]
    fn test_tracing_policy_header_manual() {
        let header = XTracingPolicy::parse_header(&Raw::from("1.0")).unwrap();
        let value: TracingPolicy = header.into();
        assert_eq!(TracingPolicy::Manual(1.0), value);
    }

    #[test]
    fn test_tracing_policy_header_manual_err() {
        assert!(XTracingPolicy::parse_header(&Raw::from("zero")).is_err());
        assert!(XTracingPolicy::parse_header(&Raw::from("-0.1")).is_err());
        assert!(XTracingPolicy::parse_header(&Raw::from("-1")).is_err());
        assert!(XTracingPolicy::parse_header(&Raw::from("1.01")).is_err());
    }
}
