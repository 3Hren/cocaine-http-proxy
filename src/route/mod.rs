use futures::Future;

use hyper;
use hyper::server::{Response, Request};

pub mod app;
pub mod performance;

pub trait Route: Send + Sync {
    type Future: Future<Item = Response, Error = hyper::Error>;

    /// Tries to process the request, returning a future (doesn't matter success or not) on
    /// conditions match, `None` otherwise.
    ///
    /// A route can process the request fully if all conditions are met, for example if it requires
    /// some headers and all of them are specified.
    /// Also it may decide to fail the request, because of incomplete prerequisites, for example if
    /// it detects all required headers, but fails to match the request method.
    /// At last a route can be neutral to the request, returning `None`.
    fn process(&self, request: &Request) -> Option<Self::Future>;
}
