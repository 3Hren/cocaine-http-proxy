use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use futures::{future, Future};

use hyper::{self, StatusCode};
use hyper::server::{Response, Request};

pub use self::app::AppRoute;
pub use self::jsonrpc::JsonRpc;
pub use self::perf::PerfRoute;

mod app;
mod jsonrpc;
mod perf;
mod serialize;

/// Request matching.
///
/// This enum represents a result of HTTP request matching over a number of routes. If a route
/// can handle the request it consumes it, returning `Some` value with a future, otherwise it must
/// return request back using `None` variant.
#[derive(Debug)]
pub enum Match<F> {
    /// Successfully consumed a `Request`, yielding a `Future`.
    Some(F),
    /// The route has not match the request, returning it back.
    None(Request),
}

impl<F> Match<F> {
    /// Moves the value `v` out of the `Match<T>` if it is `Some(v)`.
    ///
    /// # Panics
    ///
    /// Panics if the self value equals [`None`].
    ///
    /// [`None`]: #variant.None
    #[inline]
    pub fn unwrap(self) -> F {
        match self {
            Match::Some(val) => val,
            Match::None(..) => panic!("called `Match::unwrap()` on a `None` value"),
        }
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        match *self {
            Match::Some(..) => false,
            Match::None(..) => true,
        }
    }
}

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
    fn process(&self, request: Request) -> Match<Self::Future>;
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
    pub fn process(&self, mut req: Request) -> Box<Future<Item = Response, Error = hyper::Error>> {
        for route in &self.routes {
            match route.process(req) {
                Match::Some(future) => return future,
                Match::None(back) => req = back,
            }
        }

        box future::ok(Response::new().with_status(StatusCode::NotFound))
    }
}

impl Debug for Router {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("Router")
            .field("routes", &"...")
            .finish()
    }
}
