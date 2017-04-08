use std::io;
use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

use net2::TcpBuilder;

use futures::{future, Async, Future, Poll, Stream};
use futures::sync::mpsc;

use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Handle};
use tokio_service::Service;

use hyper::{self, Method, StatusCode};
use hyper::server::{Http, Request, Response};

struct Handler;

impl Service for Handler {
    type Request  = Request;
    type Response = Response;
    type Error    = hyper::Error;
    type Future   = Box<Future<Item=Response, Error=Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let res = match (req.method(), req.path()) {
            (&Method::Get, "/ping") => Response::new().with_status(StatusCode::Ok),
            (..) => Response::new().with_status(StatusCode::NotFound),
        };

        box future::ok(res)
    }
}

struct HttpService {
    rx: mpsc::UnboundedReceiver<(TcpStream, SocketAddr)>,
    handle: Handle,
    protocol: Http,
}

impl Future for HttpService {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.rx.poll() {
                Ok(Async::Ready(Some((sock, addr)))) => {
                    self.protocol.bind_connection(&self.handle, sock, addr, Handler);
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Ok(Async::Ready(None)) | Err(..) => {
                    return Ok(Async::Ready(()));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
pub struct MonitoringServer {
    addr: SocketAddr,
    backlog: i32,
    threads: usize,
}

impl MonitoringServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr: addr,
            backlog: 128,
            threads: 1,
        }
    }

    pub fn run(self) -> Result<JoinHandle<Result<(), io::Error>>, io::Error> {
        let listener = TcpBuilder::new_v6()?
            .bind(self.addr)?
            .listen(self.backlog)?;

        let mut threads: Vec<JoinHandle<Result<(), io::Error>>> = Vec::new();
        let mut dispatchers = Vec::new();

        for id in 0..self.threads {
            let (tx, rx) = mpsc::unbounded();

            let thread = thread::Builder::new().name(format!("monitor {:02}", id)).spawn(move || {
                let mut core = Core::new()?;
                let handle = core.handle();

                core.run(HttpService { rx: rx, handle: handle, protocol: Http::new() }).unwrap();

                Ok(())
            })?;

            threads.push(thread);
            dispatchers.push(tx);
        }

        let thread = thread::spawn(move || {
            let mut core = Core::new()?;
            let listener = TcpListener::from_listener(listener, &self.addr, &core.handle())?;

            let mut iter = dispatchers.iter().cycle();
            core.run(listener.incoming().for_each(move |(sock, addr)| {
                iter.next().expect("iterator is infinite").send((sock, addr)).unwrap();
                Ok(())
            }))?;

            Ok(())
        });

        Ok(thread)
    }
}
