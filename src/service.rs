use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use tokio_core::reactor::Handle;
use tokio_service::Service;

/// Creates new `Service` values.
pub trait ServiceFactory {
    /// Requests handled by the service.
    type Request;

    /// Responses given by the service.
    type Response;

    /// Errors produced by the service.
    type Error;

    /// The `Service` value created by this factory.
    type Instance: Service<Request = Self::Request, Response = Self::Response, Error = Self::Error>;

    /// Creates a new service that will serve requests from the client that is located at the
    /// provided address.
    ///
    /// In case of serving on Unix sockets `None` will be passed.
    fn create_service(&mut self, addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error>;
}

/// Creates new `ServiceFactory` values.
pub trait ServiceFactoryFactory: Send + Sync {
    type Factory: ServiceFactory;

    /// Creates a new service factory.
    fn create_factory(&self, handle: &Handle) -> Self::Factory;
}

impl<F: ServiceFactoryFactory> ServiceFactoryFactory for Arc<F> {
    type Factory = F::Factory;

    fn create_factory(&self, handle: &Handle) -> Self::Factory {
        self.deref().create_factory(handle)
    }
}
