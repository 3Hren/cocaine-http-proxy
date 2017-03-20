//! It is unclear what type of HTTP backend will be chosen finally.

// TODO: unix/ping -> health check.
// TODO: unix/config -> dump config (often config file changes, but daemon not restarted).
// TODO: unix/metrics -> dump metrics.
// TODO: show info about service pool. Draw cluster map. Count response times.

use std;
use std::borrow::Borrow;
use std::io::{self, ErrorKind};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use rand;

use futures::{future, Async, Future, Poll, Stream};
use futures::sync::{oneshot, mpsc};
use tokio_core::reactor::{Core, Handle};
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};

use rmpv::ValueRef;
use cocaine::{self, Dispatch, Error};

use config::Config;

trait HashMapExt<K: Hash + Eq, V> {
    fn entry_ref<'a, Q, B: ?Sized>(&'a mut self, key: Q) -> Entry<'a, K, V>
        where Q: Query<K, B>,
              K: Borrow<B>,
              B: Hash + Eq;
}

impl<K: Hash + Eq, V> HashMapExt<K, V> for HashMap<K, V> {
    fn entry_ref<'a, Q, B: ?Sized>(&'a mut self, key: Q) -> Entry<'a, K, V>
        where Q: Query<K, B>,
              K: Borrow<B>,
              B: Hash + Eq
    {
        // Can check for equality using `k.borrow() == key.borrow_as_key()`.
        //
        // Entry would have to gain Q: IntoOwned<K> as a type param with default
        // Q = K and call `into_key` only if necessary.
        self.entry(key.into_key())
    }
}

pub trait Query<T, B: ?Sized>: Sized where T: Borrow<B> {
    fn into_key(self) -> T;
    fn borrow_as_key(&self) -> &B;
}

impl<T> Query<T, T> for T {
    fn into_key(self) -> T { self }
    fn borrow_as_key(&self) -> &Self { self }
}

impl<'a, B: ToOwned + ?Sized> Query<B::Owned, B> for &'a B {
    fn into_key(self) -> B::Owned { self.to_owned() }
    fn borrow_as_key(&self) -> &B { *self }
}

type Event = (String, oneshot::Sender<Response>);

struct Proxy {
    txs: Vec<mpsc::UnboundedSender<Event>>,
}

impl Proxy {
    fn call_jsonrpc(&self, _req: Request) -> <Self as Service>::Future {
        let mut res = Response::new();
        res.status_code(200, "OK");
        box future::ok(res)
    }
}

impl Service for Proxy {
    type Request  = Request;
    type Response = Response;
    type Error    = io::Error;
    type Future   = Box<Future<Item = Response, Error = io::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        if let Some(..) = req.headers().find(|&(name, ..)| name == "X-Cocaine-JSON-RPC") {
            self.call_jsonrpc(req)
        } else {
            let service = "geobase".into();
            let (tx, rx) = oneshot::channel();
            let x = rand::random::<usize>();
            let birth = Instant::now();
            let rolled = x % self.txs.len();
            self.txs[rolled].send((service, tx)).unwrap();

            box rx.map_err(|err| io::Error::new(ErrorKind::Other, format!("{}", err)))
                .and_then(move |mut response|
            {
                response.header("X-Powered-By", "Cocaine");

                // TODO: Write to `proxy/access` logs.
                let elapsed = birth.elapsed();
                info!("request finished [{:#018.18x}] in {:.3} ms", x, (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as f64 / 1e6);

                Ok(response)
            })
        }
    }
}

// impl Drop for Proxy {
//     fn drop(&mut self) {
//         println!("dropped");
//     }
// }

struct SingleChunkReadDispatch {
    tx: oneshot::Sender<Response>,
}

impl Dispatch for SingleChunkReadDispatch {
    fn process(self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        let mut response = Response::new();

        match ty {
            0 => {
                response.status_code(200, "OK");
                response.body(&format!("{}", data));
            }
            1 => {
                let mut response = Response::new();
                response.status_code(500, "Internal Server Error");
                response.body(&format!("{}", data));
            }
            m => {
                let mut response = Response::new();
                response.status_code(500, "Internal Server Error");
                response.body(&format!("unknown type: {} {}", m, data));
            }
        }

        drop(self.tx.send(response));

        None
    }

    fn discard(self: Box<Self>, err: &Error) {
        let mut response = Response::new();
        response.status_code(500, "Internal Server Error");
        response.body(&format!("{}", err));

        drop(self.tx.send(response));
    }
}

struct AppReadDispatch {
    tx: oneshot::Sender<Response>,
    body: Option<String>,
    res: Option<Response>,
}

#[derive(Deserialize)]
struct MetaInfo{
    code: u32,
    headers: Vec<(String, String)>
}

#[derive(Deserialize)]
struct Body {
    body: String,
}

impl Dispatch for AppReadDispatch {
    fn process(mut self: Box<Self>, ty: u64, data: &ValueRef) -> Option<Box<Dispatch>> {
        // match ty {
        //     0 => {
        //         if self.body.is_none() {
        //             let data = match data {
        //                 &ValueRef::Array(mut data) => data.pop().unwrap(),
        //                 _ => unimplemented!(),
        //             };
        //             let meta: MetaInfo = rmps::from_slice(data.as_slice().unwrap()).unwrap();
        //             let mut res = self.res.take().unwrap();
        //             res.status_code(meta.code, "OK");
        //             for &(ref name, ref value) in &meta.headers {
        //                 res.header(&name, &value);
        //             }
        //             self.res = Some(res);
        //             self.body = Some("".into());
        //         } else {
        //             let body: Body = rmpv::ext::from_value(data).unwrap();
        //             self.body.as_mut().unwrap().push_str(&body.body);
        //         }
        //
        //         Some(self)
        //     }
        //     2 => {
        //         let mut res = self.res.take().unwrap();
        //         res.header("X-Powered-By", "Cocaine")
        //             .body(&self.body.take().unwrap());
        //
        //         self.tx.complete(res);
        //         None
        //     }
        //     _ => {
        //         let mut res = self.res.take().unwrap();
        //         res.status_code(500, "Internal Server Error")
        //             .body(&"");
        //
        //         self.tx.complete(res);
        //         None
        //     }
        // }
        unimplemented!();
    }

    fn discard(self: Box<Self>, err: &Error) {
        let mut response = Response::new();
        response.status_code(500, "Internal Server Error");
        response.body(&format!("{}", err));

        drop(self.tx.send(response));
    }
}

struct ServicePool {
    /// Next service.
    counter: usize,
    name: String,
    /// Reconnect threshold.
    threshold: Duration,
    handle: Handle,
    last_traverse: SystemTime,

    services: Vec<(cocaine::Service, SystemTime)>,
}

impl ServicePool {
    fn new(name: String, limit: usize, handle: &Handle) -> Self {
        let now = SystemTime::now();

        Self {
            counter: 0,
            name: name.clone(),
            threshold: Duration::new(60, 0),
            handle: handle.clone(),
            last_traverse: now,
            services: std::iter::repeat(name)
                .take(limit)
                .map(|name| (cocaine::Service::new(name.clone(), handle), now))
                .collect()
        }
    }

    fn next(&mut self) -> &cocaine::Service {
        let now = SystemTime::now();

        // No often than every 5 seconds we're traversing services for reconnection.
        if now.duration_since(self.last_traverse).unwrap() > Duration::new(5, 0) {
            self.last_traverse = now;

            let mut killed = 0;

            for id in 0..self.services.len() {
                let (.., birth) = self.services[id];
                if now.duration_since(birth).unwrap() > self.threshold {
                    killed += 1;
                    self.services[id] = (cocaine::Service::new(self.name.clone(), &self.handle), now);
                }

                if killed > self.services.len() / 2 {
                    break;
                }

                // TODO: Tiny preconnection optimization.
            }
        }

        self.counter = (self.counter + 1) % self.services.len();
        &self.services[self.counter].0
    }
}

struct Infinity {
    handle: Handle,
    rx: mpsc::UnboundedReceiver<Event>,

    pool: HashMap<String, ServicePool>,
}

impl Infinity {
    fn new(handle: Handle, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        Infinity {
            handle: handle,
            rx: rx,
            pool: HashMap::new(),
        }
    }
}

impl Infinity {
    fn select_service(&mut self, name: String, handle: &Handle) -> &cocaine::Service {
        let mut pool = self.pool.entry_ref(&name)
            .or_insert_with(|| ServicePool::new(name, 10, handle));
        pool.next()
    }
}

impl Future for Infinity {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        debug!("woken up; pool size: {}", self.pool.len());

        loop {
            match self.rx.poll() {
                Ok(Async::Ready(Some((name, tx)))) => {
                    debug!("request for `{}` service", name);

                    // Select next service that is not reconnecting right now. No more than N/2
                    // services can be in reconnecting state concurrently.
                    let handle = self.handle.clone();
                    let ref service = self.select_service(name, &handle);

                    // let future = service.call(0, &vec!["http"], AppReadDispatch {
                    //     tx: tx,
                    //     body: None,
                    //     res: Some(Response::new()),
                    // })
                    // .and_then(|tx| {
                    //     let buf = rmps::to_vec(&("GET", "/", 1, &[("Content-Type", "text/plain")], "")).unwrap();
                    //     tx.send(0, &[unsafe { ::std::str::from_utf8_unchecked(&buf) }]);
                    //     tx.send(2, &[0; 0]);
                    //     Ok(())
                    // })
                    // .then(|_| Ok(()));

                    let future = service.call(0, &vec!["8.8.8.8"], SingleChunkReadDispatch { tx: tx })
                        .then(|tx| {
                            drop(tx);
                            Ok(())
                        });


                    handle.spawn(future);
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Ok(Async::Ready(None)) | Err(..) => {
                    unreachable!();
                }
            }
        }

        Ok(Async::NotReady)
    }
}

struct ProxyFactory {
    txs: Vec<mpsc::UnboundedSender<Event>>,
}

impl NewService for ProxyFactory {
    type Request  = Request;
    type Response = Response;
    type Instance = Proxy;
    type Error    = io::Error;

    fn new_service(&self) -> Result<Self::Instance, Self::Error> {
        Ok(Proxy { txs: self.txs.clone() })
    }
}

pub fn run(config: Config) -> Result<(), Box<Error>> {
    // TODO: Init logging system.

    let mut txs = Vec::new();
    let mut threads = Vec::new();

    for tid in 0..config.threads().network() {
        let (tx, rx) = mpsc::unbounded();
        let thread = thread::Builder::new()
            .name(format!("W#{:02}", tid))
            .spawn(move ||
        {
            let mut core = Core::new()
                .expect("failed to initialize event loop");
            let handle = core.handle();

            core.run(Infinity::new(handle, rx)).unwrap();
        });

        txs.push(tx);
        threads.push(thread);
    }

    let ipaddr = config.addr().parse()
        .expect("failed to parse IP address");

    let sockaddr = SocketAddr::new(ipaddr, config.port());
    let mut srv = TcpServer::new(Http, sockaddr);
    srv.threads(config.threads().http());

    let factory = ProxyFactory { txs: txs };
    srv.serve(factory);

    Ok(())
}
