//! HTTP Server

use std::cell::RefCell;
use std::io;
use std::net::{self, SocketAddr};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::os::unix::io::{AsRawFd, FromRawFd};

use cocaine::logging::{Logger, Severity};

use futures::{future, Async, Future, Poll, Stream};
use futures::sync::mpsc;
use futures::task::{self, Task};

use hyper;
use hyper::server::{Http, Request, Response};

use libc;

use net2::TcpBuilder;
use net2::unix::UnixTcpBuilderExt;

use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Handle, Timeout};
use tokio_service::Service;

use net::Incoming;
use service::{ServiceFactory, ServiceFactorySpawn};

const DEFAULT_NUM_THREADS: usize = 1;
const DEFAULT_BACKLOG: i32 = 1024;

struct Info {
    active: usize,
    blocker: Option<Task>,
}

impl Info {
    fn new() -> Self {
        Self {
            active: 0,
            blocker: None,
        }
    }
}

struct NotifyService<S> {
    inner: S,
    info: Weak<RefCell<Info>>,
}

impl<S> NotifyService<S> {
    fn new(inner: S, info: &mut Rc<RefCell<Info>>) -> Self {
        info.borrow_mut().active += 1;

        Self {
            inner: inner,
            info: Rc::downgrade(&info),
        }
    }
}

impl<S> Drop for NotifyService<S> {
    fn drop(&mut self) {
        let info = match self.info.upgrade() {
            Some(info) => info,
            None => return,
        };

        let mut info = info.borrow_mut();
        info.active -= 1;
        if info.active == 0 {
            if let Some(task) = info.blocker.take() {
                task.notify();
            }
        }
    }
}

impl<S: Service> Service for NotifyService<S> {
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, message: Self::Request) -> Self::Future {
        self.inner.call(message)
    }
}

struct WaitUntilZero {
    info: Rc<RefCell<Info>>,
}

impl Future for WaitUntilZero {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        let mut info = self.info.borrow_mut();
        if info.active == 0 {
            Ok(().into())
        } else {
            info.blocker = Some(task::current());
            Ok(Async::NotReady)
        }
    }
}

struct HttpService<T> {
    rx: mpsc::UnboundedReceiver<(net::TcpStream, SocketAddr)>,
    handle: Handle,
    protocol: Http,
    factory: T,
    log: Logger,
}

impl<T> HttpService<T> {
    fn new(rx: mpsc::UnboundedReceiver<(net::TcpStream, SocketAddr)>, handle: Handle, factory: T, log: Logger) -> Self {
        Self {
            rx: rx,
            handle: handle,
            protocol: Http::new(),
            factory: factory,
            log: log,
        }
    }
}

impl<T: ServiceFactory<Request=Request, Response=Response, Error=hyper::Error>> Future for HttpService<T>
    where T::Instance: 'static
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.rx.poll() {
                Ok(Async::Ready(Some((sock, addr)))) => {
                    let sock = match TcpStream::from_stream(sock, &self.handle) {
                        Ok(sock) => sock,
                        Err(err) => {
                            cocaine_log!(self.log, Severity::Error, "failed to create socket: {}", err);
                            break;
                        }
                    };
                    let service = match self.factory.create_service(Some(addr)) {
                        Ok(sock) => sock,
                        Err(err) => {
                            cocaine_log!(self.log, Severity::Error, "failed to create HTTP handler: {}", err);
                            break;
                        }
                    };
                    self.protocol.bind_connection(&self.handle, sock, addr, service);
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Ok(Async::Ready(None)) | Err(..) => {
                    // Listener is gone.
                    return Ok(Async::Ready(()));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

/// A trait that can give a meaningful name for each worker thread created by the server.
pub trait GodFather {
    /// Called when it's time to give a name for a new thread.
    fn name(&self, id: usize) -> String;
}

/// Default Godfather implementation that names all threads with "worker X" prefix where "X" - is
/// an integral index.
pub struct DefaultGodFather;

impl GodFather for DefaultGodFather {
    fn name(&self, id: usize) -> String {
        format!("worker {:02}", id)
    }
}

impl<F> GodFather for F
    where F: Fn(usize) -> String
{
    fn name(&self, id: usize) -> String {
        (*self)(id)
    }
}

/// Chained server configuration.
pub struct ServerConfig<G> {
    addr: SocketAddr,
    backlog: i32,
    godfather: G,
    num_threads: usize,
}

impl ServerConfig<DefaultGodFather> {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr: addr,
            backlog: DEFAULT_BACKLOG,
            godfather: DefaultGodFather,
            num_threads: DEFAULT_NUM_THREADS,
        }
    }
}

impl<G> ServerConfig<G> {
    pub fn backlog(mut self, backlog: i32) -> Self {
        self.backlog = backlog;
        self
    }

    pub fn godfather<T>(self, godfather: T) -> ServerConfig<T> {
        ServerConfig {
            addr: self.addr,
            backlog: self.backlog,
            godfather: godfather,
            num_threads: self.num_threads,
        }
    }

    pub fn threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }
}

fn bind(addr: SocketAddr, backlog: i32, handle: &Handle) -> Result<TcpListener, io::Error> {
    let sock = match addr {
        SocketAddr::V4(..) => TcpBuilder::new_v4(),
        SocketAddr::V6(..) => TcpBuilder::new_v6(),
    };

    let listener = sock?
        .reuse_port(true)?
        .bind(addr)?
        .listen(backlog)?;

    TcpListener::from_listener(listener, &addr, handle)
}

#[derive(Debug)]
pub struct ServerGroup {
    core: Core,
    servers: Vec<(TcpListener, Vec<mpsc::UnboundedSender<(net::TcpStream, SocketAddr)>>)>,
    threads: Vec<JoinHandle<Result<(), io::Error>>>,
    log: Logger,
}

impl ServerGroup {
    pub fn new(log: Logger) -> Result<Self, io::Error> {
        let core = Core::new()?;
        let result = Self {
            core: core,
            servers: Vec::new(),
            threads: Vec::new(),
            log: log,
        };

        Ok(result)
    }

    /// 1. Binds socket, starts listening.
    /// 2. Spawns worker thread(s).
    pub fn expose<F, T, G>(mut self, cfg: ServerConfig<G>, factory: F) -> Result<Self, io::Error>
        where F: ServiceFactorySpawn<Factory = T> + 'static,
              T: ServiceFactory<Request = Request, Response = Response, Error = hyper::Error> + 'static,
              G: GodFather
    {
        let listener = bind(cfg.addr, cfg.backlog, &self.core.handle())?;

        let mut dispatchers = Vec::new();
        let factory = Arc::new(factory);

        for id in 0..cfg.num_threads {
            let (tx, rx) = mpsc::unbounded();
            let factory = factory.clone();
            let log = self.log.clone();
            let thread = thread::Builder::new().name(cfg.godfather.name(id)).spawn(move || {
                let mut core = Core::new()?;
                let handle = core.handle();

                // Mini-future to track the number of active services.
                let info = Rc::new(RefCell::new(Info::new()));

                let factory = {
                    let mut info = info.clone();
                    factory.create_factory(&handle).map(move |service| {
                        NotifyService::new(service, &mut info)
                    })
                };

                // This will stop just after listener is stopped, because it polls the connection
                // receiver.
                core.run(HttpService::new(rx, handle.clone(), factory, log))?;

                let monitor = WaitUntilZero { info: info };
                let timeout = Timeout::new(Duration::new(5, 0), &handle)?;

                // We've stopped accepting new connections at this point, but we want to give
                // existing connections a chance to clear themselves out. Wait at most `timeout`
                // time before we just return clearing everything out.
                //
                // Our custom `WaitUntilZero` will resolve once all services constructed here have
                // been destroyed.
                match core.run(monitor.select(timeout)) {
                    Ok(..) => {}
                    Err((err, ..)) => return Err(err.into()),
                }

                Ok(())
            })?;

            dispatchers.push(tx);
            self.threads.push(thread);
        }

        self.servers.push((listener, dispatchers));

        Ok(self)
    }

    /// Execute this server infinitely.
    ///
    /// This method does not currently return, but it will return an error if one occurs.
    pub fn run(self) -> Result<(), io::Error> {
        self.run_until(future::empty())
    }

    /// Execute this server until the given future resolves.
    pub fn run_until<F>(mut self, cancel: F) -> Result<(), io::Error>
        where F: Future<Item = (), Error = ()>
    {
        let log = self.log.clone();
        let listeners = self.servers.into_iter().map(|(listener, dispatchers)| {
            let log = log.clone();
            let mut iter = dispatchers.into_iter().cycle();
            Incoming::new(listener).for_each(move |accept| {
                match accept {
                    Ok((sock, addr)) => {
                        let fd = unsafe { libc::dup(sock.as_raw_fd()) };
                        if fd >= 0 {
                            let cloned = unsafe { net::TcpStream::from_raw_fd(fd) };
                            if let Err(..) = iter.next().expect("iterator is infinite").unbounded_send((cloned, addr)) {
                                cocaine_log!(log, Severity::Error, "failed to schedule incoming TCP connection from {}", addr);
                            }
                        } else {
                            let err = io::Error::last_os_error();
                            cocaine_log!(log, Severity::Error, "failed to dup file descriptor: {}", err);
                        }
                    }
                    Err(err) => {
                        cocaine_log!(log, Severity::Error, "failed to accept TCP connection: {}", err);
                    }
                }

                Ok(())
            })
        });

        let listen = future::join_all(listeners).and_then(|vec| Ok(drop(vec)));
        let cancel = cancel.then(|result| Ok(drop(result)));

        self.core.run(listen.select(cancel).map_err(|(err, ..)| err))
            .expect("received unreachable error");

        for thread in self.threads {
            thread.join().expect("workers should not panic")?;
        }

        Ok(())
    }
}
