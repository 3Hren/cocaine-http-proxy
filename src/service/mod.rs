use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio_core::reactor::Handle;
use tokio_service::Service;

pub mod cocaine;
pub mod monitor;

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

    /// Map this `ServiceFactory` to a different `ServiceFactory`, which applies a mapping function
    /// to each `Service` instance produced if `Ok`, returning a new one, leaving an `Err` value
    /// untouched.
    ///
    /// Service's `Request`, `Response` and `Error` types must stay the same.
    fn map<F, U>(self, f: F) -> Map<Self, F>
        where F: FnMut(Self::Instance) -> U,
              U: Service<Request = Self::Request, Response = Self::Response, Error = Self::Error>,
              Self: Sized
    {
        Map {
            factory: self,
            f: f,
        }
    }
}

pub struct Map<T, F>
    where T: ServiceFactory
{
    factory: T,
    f: F,
}

impl<T, F, U> ServiceFactory for Map<T, F>
    where T: ServiceFactory,
          F: FnMut(T::Instance) -> U,
          U: Service<Request = T::Request, Response = T::Response, Error = T::Error>
{
    type Request = T::Request;
    type Response = T::Response;
    type Error = T::Error;
    type Instance = U;

    fn create_service(&mut self, addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        let service = self.factory.create_service(addr)?;
        let wrapped = (self.f)(service);
        Ok(wrapped)
    }
}

/// Creates new `ServiceFactory` values.
pub trait ServiceFactorySpawn: Send + Sync {
    type Factory: ServiceFactory;

    /// Creates a new service factory.
    ///
    /// This is called during server initialization on each thread start. A `Handle` provided can
    /// be used to spawn additional services or to bind some asynchronous context with the event
    /// loop that will be associated with the thread spawned.
    fn create_factory(&self, handle: &Handle) -> Self::Factory;
}

impl<F: ServiceFactorySpawn + ?Sized> ServiceFactorySpawn for Arc<F> {
    type Factory = F::Factory;

    fn create_factory(&self, handle: &Handle) -> Self::Factory {
        (**self).create_factory(handle)
    }
}
