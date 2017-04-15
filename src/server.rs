//! HTTP Server

use std::cell::RefCell;
use std::io::{self};
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use net2::TcpBuilder;
use net2::unix::UnixTcpBuilderExt;
use num_cpus;

use futures::{future, Async, Future, Poll, Stream};
use futures::sync::mpsc;
use futures::task::{self, Task};

use hyper;
use hyper::server::{Http, Request, Response};

use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Handle, Timeout};
use tokio_service::Service;

use service::{ServiceFactory, ServiceFactorySpawn};

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
                task.unpark();
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

struct NotifyServiceFactory<F> {
    wrapped: F,
    info: Rc<RefCell<Info>>,
}

impl<F: ServiceFactory> ServiceFactory for NotifyServiceFactory<F> {
    type Request  = F::Request;
    type Response = F::Response;
    type Instance = NotifyService<F::Instance>;
    type Error    = F::Error;

    fn create_service(&mut self, addr: Option<SocketAddr>) -> Result<Self::Instance, io::Error> {
        let service = self.wrapped.create_service(addr)?;
        let wrapped = NotifyService::new(service, &mut self.info);
        Ok(wrapped)
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
            info.blocker = Some(task::park());
            Ok(Async::NotReady)
        }
    }
}

struct HttpService<T> {
    rx: mpsc::UnboundedReceiver<(TcpStream, SocketAddr)>,
    handle: Handle,
    protocol: Http,
    factory: T,
}

impl<T> HttpService<T> {
    fn new(rx: mpsc::UnboundedReceiver<(TcpStream, SocketAddr)>, handle: Handle, factory: T) -> Self {
        Self {
            rx: rx,
            handle: handle,
            protocol: Http::new(),
            factory: factory,
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
                    let service = self.factory.create_service(Some(addr))?;
                    self.protocol.bind_connection(&self.handle, sock, addr, service);
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Ok(Async::Ready(None)) | Err(..) => {
                    // Listener is gone.
                    println!("Listener is gone.");
                    return Ok(Async::Ready(()));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

pub struct ServerBuilder {
    addr: SocketAddr,
    backlog: i32,
    num_threads: usize,
}

impl ServerBuilder {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr: addr,
            backlog: DEFAULT_BACKLOG,
            num_threads: num_cpus::get(),
        }
    }

    pub fn backlog(mut self, backlog: i32) -> Self {
        self.backlog = backlog;
        self
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
    servers: Vec<(TcpListener, Vec<mpsc::UnboundedSender<(TcpStream, SocketAddr)>>)>,
    threads: Vec<JoinHandle<Result<(), io::Error>>>,
}

impl ServerGroup {
    pub fn new() -> Result<Self, io::Error> {
        let core = Core::new()?;
        let result = Self {
            core: core,
            servers: Vec::new(),
            threads: Vec::new(),
        };

        Ok(result)
    }

    /// 1. Binds socket, starts listening.
    /// 2. Spawns worker thread(s).
    pub fn expose<F, T>(mut self, builder: ServerBuilder, factory: F) -> Result<Self, io::Error>
        where F: ServiceFactorySpawn<Factory = T> + 'static,
              T: ServiceFactory<Request = Request, Response = Response, Error = hyper::Error> + 'static
    {
        let listener = bind(builder.addr, builder.backlog, &self.core.handle())?;

        let mut dispatchers = Vec::new();
        let factory = Arc::new(factory);

        for id in 0..builder.num_threads {
            let (tx, rx) = mpsc::unbounded();
            let factory = factory.clone();
            let name = format!("worker {:02}", id);
            let thread = thread::Builder::new().name(name).spawn(move || {
                let mut core = Core::new()?;
                let handle = core.handle();

                // Mini-future to track the number of active services.
                let info = Rc::new(RefCell::new(Info::new()));

                let factory = NotifyServiceFactory {
                    wrapped: factory.create_factory(&handle),
                    info: info.clone(),
                };

                // This will stop just after listener is stopped, because it polls the connection
                // receiver.
                core.run(HttpService::new(rx, handle.clone(), factory))?;

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
        let listeners = self.servers.into_iter().map(|(listener, dispatchers)| {
            let mut iter = dispatchers.into_iter().cycle();
            listener.incoming().for_each(move |(sock, addr)| {
                sock.set_nodelay(true)?;
                iter.next().expect("iterator is infinite").send((sock, addr)).unwrap();
                Ok(())
            })
        });

        let listen = future::join_all(listeners).and_then(|vec| Ok(drop(vec)));
        let cancel = cancel.then(|result| Ok(drop(result)));

        self.core.run(listen.select(cancel).map_err(|(err, ..)| err))?;

        println!("Before join");

        for thread in self.threads {
            thread.join().unwrap()?;
        }

        Ok(())
    }
}
