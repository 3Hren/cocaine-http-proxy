use std::boxed::FnBox;
use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::iter;
use std::time::{Duration, SystemTime};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Async, Future, Poll, Stream};
use futures::sync::mpsc;
use tokio_core::reactor::Handle;

use cocaine::Service;

// TODO: Should not be public.
pub enum Event {
    Service {
        name: String,
        func: Box<FnBox(&Service) -> Box<Future<Item=(), Error=()> + Send> + Send>,
    },
    ServiceConnect(Service),
}

struct WatchedService {
    service: Service,
    created_at: SystemTime,
}

impl WatchedService {
    fn new(service: Service, created_at: SystemTime) -> Self {
        Self {
            service: service,
            created_at: created_at,
        }
    }
}

struct ServicePool {
    /// Next service.
    counter: usize,
    name: String,
    /// Maximum service age.
    lifetime: Duration,
    handle: Handle,
    last_traverse: SystemTime,

    connecting: Arc<AtomicUsize>,
    connecting_limit: usize,
    services: VecDeque<WatchedService>,
    tx: mpsc::UnboundedSender<Event>,
}

impl ServicePool {
    fn new(name: String, limit: usize, handle: &Handle, tx: mpsc::UnboundedSender<Event>) -> Self {
        let now = SystemTime::now();

        Self {
            counter: 0,
            name: name.clone(),
            lifetime: Duration::new(5, 0),
            handle: handle.clone(),
            last_traverse: now,
            connecting: Arc::new(AtomicUsize::new(0)),
            connecting_limit: cmp::max(1, limit / 2),
            services: iter::repeat(name)
                .take(limit)
                .map(|name| WatchedService::new(Service::new(name.clone(), handle), now))
                .collect(),
            tx: tx
        }
    }

    fn next(&mut self) -> &Service {
        let now = SystemTime::now();

        // No more than once every 5 seconds we're checking services for reconnection.
        if now.duration_since(self.last_traverse).unwrap() > Duration::new(5, 0) {
            self.last_traverse = now;

            println!("reconnecting at most {}/{} services", self.connecting_limit - self.connecting.load(Ordering::Relaxed), self.services.len());
            while self.connecting.load(Ordering::Relaxed) < self.connecting_limit {
                match self.services.pop_front() {
                    Some(service) => {
                        if now.duration_since(service.created_at).unwrap() > self.lifetime {
                            println!("reconnect");
                            self.connecting.fetch_add(1, Ordering::Relaxed);

                            let service = Service::new(self.name.clone(), &self.handle);
                            let future = service.connect();

                            let tx = self.tx.clone();
                            self.handle.spawn(future.then(move |res| {
                                if let Err(err) = res {
                                    // Okay, we've tried our best. Insert the service anyway,
                                    // because internally it will try to establish connection
                                    // before each invocation attempt.
                                    println!("failed to reconnect `{}` service: {}", service.name(), err);
                                }

                                tx.send(Event::ServiceConnect(service)).unwrap();
                                Ok(())
                            }));
                        } else {
                            self.services.push_front(service);
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        self.counter = (self.counter + 1) % self.services.len();

        &self.services[self.counter].service
    }
}

///
/// # Note
///
/// This task lives until all associated senders live:
/// - HTTP handlers.
/// - Timers.
/// - Unicorn notifiers.
/// - RG notifiers.
pub struct PoolTask {
    handle: Handle,
    tx: mpsc::UnboundedSender<Event>,
    rx: mpsc::UnboundedReceiver<Event>,

    pool: HashMap<String, ServicePool>,
}

impl PoolTask {
    pub fn new(handle: Handle, tx: mpsc::UnboundedSender<Event>, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        Self {
            handle: handle,
            tx: tx,
            rx: rx,
            pool: HashMap::new(),
        }
    }

    fn select_service(&mut self, name: String, handle: &Handle) -> &Service {
        let tx = self.tx.clone();
        let mut pool = {
            let name = name.clone();
            self.pool.entry(name.clone())
                .or_insert_with(|| ServicePool::new(name, 10, handle, tx))
        };

        let now = SystemTime::now();
        while pool.services.len() + pool.connecting.load(Ordering::Relaxed) < 10 {
            pool.services.push_back(WatchedService::new(Service::new(name.clone(), handle), now))
        }

        pool.next()
    }
}

impl Future for PoolTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.rx.poll() {
                Ok(Async::Ready(Some(event))) => {
                    match event {
                        Event::Service { name, func } => {
                            // Select the next service that is not reconnecting right now. No more
                            // than N/2 services can be in reconnecting state concurrently.
                            let handle = self.handle.clone();
                            let ref service = self.select_service(name, &handle);

                            handle.spawn(func.call_box((service,)));
                        }
                        Event::ServiceConnect(service) => {
                            println!("got `ServiceConnect` event for `{}`", service.name());
                            match self.pool.get_mut(service.name()) {
                                Some(pool) => {
                                    pool.connecting.fetch_sub(1, Ordering::Relaxed);
                                    pool.services.push_back(WatchedService::new(service, SystemTime::now()));
                                }
                                None => {
                                    println!("dropping service `{}` to unknown pool", service.name());
                                }
                            }
                        }
                        // TODO: RG updates.
                        // TODO: Unicorn timeout updates.
                        // TODO: Unicorn tracing chance updates.
                    }
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
