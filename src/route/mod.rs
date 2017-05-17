use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use futures::{future, Future};

use hyper::{self, StatusCode};
use hyper::server::{Response, Request};

pub use self::app::AppRoute;
pub use self::perf::PerfRoute;

pub mod app;
pub mod perf;

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

pub type HyperRoute = Arc<Route<Future = Box<Future<Item = Response, Error = hyper::Error>>>>;

#[derive(Clone)]
pub struct Router {
    routes: Vec<HyperRoute>,
}

impl Router {
    pub fn new() -> Self {
        Self { routes: Vec::new() }
    }

    /// Adds a route to the `Router`.
    pub fn add(&mut self, route: HyperRoute) {
        self.routes.push(route);
    }

    /// Tries to process the request, returning a future on first route match. If none of them
    /// match, returns a ready future with `NotFound` HTTP status.
    pub fn process(&self, req: &Request) -> Box<Future<Item = Response, Error = hyper::Error>> {
        for route in &self.routes {
            if let Some(future) = route.process(&req) {
                return future;
            }
        }

        future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
    }
}

impl Debug for Router {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Router")
            .field("routes", &"...")
            .finish()
    }
}
