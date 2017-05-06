use std::boxed::FnBox;
use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::iter;
use std::time::{Duration, SystemTime};

use futures::{Async, Future, Poll, Stream};
use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_core::reactor::{Handle, Timeout};
use uuid::Uuid;

use cocaine::{Error, Service};
use cocaine::logging::{Logger, Severity};
use cocaine::service::Locator;
use cocaine::service::locator::HashRing;
use cocaine::service::unicorn::{Close, Unicorn, Version};

use config::{PoolConfig, ServicePoolConfig};

// TODO: Should not be public.
pub enum Event {
    Service {
        name: String,
        func: Box<FnBox(&Service) -> Box<Future<Item=(), Error=()> + Send> + Send>,
    },
    OnServiceConnect(Service),
    OnRoutingUpdates(HashMap<String, HashRing>),
    OnTracingUpdates(HashMap<String, f64>),
    OnTimeoutUpdates(HashMap<String, f64>),
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
    log: Logger,

    /// Next service.
    counter: usize,
    name: String,
    limit: usize,
    /// Maximum service age.
    lifespan: Duration,
    handle: Handle,
    last_traverse: SystemTime,

    connecting: usize,
    connecting_limit: usize,
    services: VecDeque<WatchedService>,
    tx: UnboundedSender<Event>,
}

impl ServicePool {
    fn new(name: String, cfg: ServicePoolConfig, handle: &Handle, tx: UnboundedSender<Event>, log: Logger) -> Self {
        let now = SystemTime::now();

        Self {
            log: log,
            counter: 0,
            name: name.clone(),
            limit: cfg.limit(),
            lifespan: Duration::new(cfg.lifespan(), 0),
            handle: handle.clone(),
            last_traverse: now,
            connecting: 0,
            connecting_limit: cmp::max(1, (cfg.limit() as f64 * cfg.reconnection_ratio()).ceil() as usize),
            services: iter::repeat(name)
                .take(cfg.limit())
                .map(|name| WatchedService::new(Service::new(name.clone(), handle), now))
                .collect(),
            tx: tx,
        }
    }

    fn push(&mut self, service: Service) {
        while self.services.len() >= self.limit {
            self.services.pop_front();
        }

        self.services.push_back(WatchedService::new(service, SystemTime::now()));
    }

    fn reconnect(&mut self) {
        cocaine_log!(self.log, Severity::Info, "reconnecting `{}` service", self.name);
        self.connecting += 1;

        let service = Service::new(self.name.clone(), &self.handle);
        let future = service.connect();

        let tx = self.tx.clone();
        let log = self.log.clone();
        self.handle.spawn(future.then(move |res| {
            match res {
                Ok(()) => cocaine_log!(log, Severity::Info, "service `{}` has been reconnected", service.name()),
                Err(err) => {
                    // Okay, we've tried our best. Insert the service anyway, because internally it
                    // will try to establish connection before the next invocation attempt.
                    cocaine_log!(log, Severity::Warn, "failed to reconnect `{}` service: {}", service.name(), err);
                }
            }

            tx.send(Event::OnServiceConnect(service)).unwrap();
            Ok(())
        }));
    }

    fn reconnect_all(&mut self) {
        for _ in 0..self.limit {
            self.reconnect();
        }
    }

    fn next(&mut self) -> &Service {
        let now = SystemTime::now();

        // No more than once every 5 seconds we're checking services for reconnection.
        if now.duration_since(self.last_traverse).unwrap() > Duration::new(5, 0) {
            self.last_traverse = now;

            cocaine_log!(self.log, Severity::Debug, "reconnecting at most {}/{} services",
                self.connecting_limit - self.connecting, self.services.len());

            while self.connecting < self.connecting_limit {
                match self.services.pop_front() {
                    Some(service) => {
                        if now.duration_since(service.created_at).unwrap() > self.lifespan {
                            self.reconnect();
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
    log: Logger,

    tx: UnboundedSender<Event>,
    rx: UnboundedReceiver<Event>,

    cfg: PoolConfig,
    pool: HashMap<String, ServicePool>,

    tracing: HashMap<String, f64>,
    timeouts: HashMap<String, f64>,
}

impl PoolTask {
    pub fn new(handle: Handle, log: Logger, tx: UnboundedSender<Event>, rx: UnboundedReceiver<Event>, cfg: PoolConfig) -> Self {
        Self {
            handle: handle,
            log: log,
            tx: tx,
            rx: rx,
            cfg: cfg,
            pool: HashMap::new(),
            tracing: HashMap::new(),
            timeouts: HashMap::new(),
        }
    }

    fn select_service(&mut self, name: String, handle: &Handle) -> &Service {
        // TODO: Do not clone if not needed.
        let tx = self.tx.clone();
        let log = self.log.clone();
        let cfg = self.cfg.config(&name);

        let mut pool = {
            let name = name.clone();
            self.pool.entry(name.clone())
                .or_insert_with(|| ServicePool::new(name, cfg, handle, tx, log))
        };

        let now = SystemTime::now();
        while pool.services.len() + pool.connecting < 10 {
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
                            // Select the next service that is not reconnecting right now.
                            let handle = self.handle.clone();
                            let ref service = self.select_service(name, &handle);

                            handle.spawn(func.call_box((service,)));
                        }
                        Event::OnServiceConnect(service) => {
                            match self.pool.get_mut(service.name()) {
                                Some(pool) => {
                                    pool.connecting -= 1;
                                    pool.push(service);
                                }
                                None => {
                                    println!("dropping service `{}` to unknown pool", service.name());
                                }
                            }
                        }
                        Event::OnRoutingUpdates(groups) => {
                            for (group, ..) in groups {
                                if let Some(pool) = self.pool.get_mut(&group) {
                                    cocaine_log!(self.log, Severity::Info, "updated `{}` pool", group);
                                    pool.reconnect_all();
                                }
                            }
                        }
                        Event::OnTracingUpdates(tracing) => {
                            self.tracing = tracing;
                        }
                        Event::OnTimeoutUpdates(timeouts) => {
                            self.timeouts = timeouts;
                        }
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

enum RoutingState<T> {
    Fetch((String, T)),
    Retry(Timeout),
}

pub struct RoutingGroupsUpdateTask<T> {
    handle: Handle,
    locator: Locator,
    txs: Vec<UnboundedSender<Event>>,
    log: Logger,
    /// Current state.
    state: Option<RoutingState<T>>,
    /// Number of unsuccessful routing table fetching attempts.
    attempts: u32,
    /// Maximum timeout value. Actual timeout is calculated using `2 ** attempts` formula, but is
    /// truncated to this value if oversaturated.
    timeout_limit: Duration,
}

impl RoutingGroupsUpdateTask<Box<Stream<Item=HashMap<String, HashRing>, Error=Error>>> {
    pub fn new(handle: Handle, locator: Locator, txs: Vec<UnboundedSender<Event>>, log: Logger) -> Self {
        let uuid = Uuid::new_v4().hyphenated().to_string();
        let stream = locator.routing(&uuid);

        Self {
            handle: handle,
            locator: locator,
            txs: txs,
            log: log,
            state: Some(RoutingState::Fetch((uuid, stream.boxed()))),
            attempts: 0,
            timeout_limit: Duration::new(32, 0),
        }
    }

    fn next_timeout(&self) -> Duration {
        // Hope that 2**18 seconds, which is ~3 days, fits everyone needs.
        let exp = cmp::min(self.attempts, 18);
        let duration = Duration::new(2u64.pow(exp), 0);

        cmp::min(duration, self.timeout_limit)
    }

    fn retry_later(&mut self) -> Poll<(), io::Error> {
        let timeout = self.next_timeout();
        cocaine_log!(self.log, Severity::Debug, "next timeout will fire in {}s", timeout.as_secs());

        self.state = Some(RoutingState::Retry(Timeout::new(timeout, &self.handle)?));
        self.attempts += 1;
        return self.poll();
    }
}

impl Future for RoutingGroupsUpdateTask<Box<Stream<Item=HashMap<String, HashRing>, Error=Error>>> {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.take().expect("state must be valid") {
            RoutingState::Fetch((uuid, mut stream)) => {
                loop {
                    match stream.poll() {
                        Ok(Async::Ready(Some(groups))) => {
                            self.attempts = 0;

                            cocaine_log!(self.log, Severity::Info, "received {} RG(s) updates", groups.len());
                            for tx in &self.txs {
                                tx.send(Event::OnRoutingUpdates(groups.clone()))
                                    .expect("channel is bound to itself and lives forever");
                            }
                        }
                        Ok(Async::NotReady) => {
                            break;
                        }
                        Ok(Async::Ready(None)) => {
                            cocaine_log!(self.log, Severity::Info, "locator has closed RG subscription"; {
                                uuid: uuid,
                            });
                            return self.retry_later();
                        }
                        Err(err) => {
                            cocaine_log!(self.log, Severity::Warn, "failed to update RG: {}", err; {
                                uuid: uuid,
                            });
                            return self.retry_later();
                        }
                    }
                }
                self.state = Some(RoutingState::Fetch((uuid, stream)));
            }
            RoutingState::Retry(mut timeout) => {
                match timeout.poll() {
                    Ok(Async::Ready(())) => {
                        cocaine_log!(self.log, Severity::Debug, "timed out, trying to subscribe routing ...");

                        let uuid = Uuid::new_v4().hyphenated().to_string();
                        let stream = self.locator.routing(&uuid);
                        self.state = Some(RoutingState::Fetch((uuid, stream.boxed())));
                        return self.poll();
                    }
                    Ok(Async::NotReady) => {
                        self.state = Some(RoutingState::Retry(timeout));
                    }
                    Err(..) => unreachable!(),
                }
            }
        }

        Ok(Async::NotReady)
    }
}

type SubscribeStream = Box<Stream<Item=(HashMap<String, f64>, Version), Error=Error> + Send>;

enum SubscribeState {
    Start(Box<Future<Item=(Close, SubscribeStream), Error=Error>>),
    Fetch(SubscribeStream),
    Retry(Timeout),
}

pub struct SubscribeTask<F> {
    /// Path subscribed on.
    path: String,
    /// Unicorn service.
    unicorn: Unicorn,
    /// Logger service.
    log: Logger,
    /// I/O loop handle.
    handle: Handle,
    /// Current state.
    state: Option<SubscribeState>,
    /// Number of unsuccessful fetching attempts.
    attempts: u32,
    /// Maximum timeout value. Actual timeout is calculated using `2 ** attempts` formula, but is
    /// truncated to this value if oversaturated.
    timeout_limit: Duration,
    /// Close handle to be able to notify the Unicorn that we no longer needed for updates.
    close: Option<Close>,
    callback: F,
}

impl<F> SubscribeTask<F>
    where F: Fn(HashMap<String, f64>)
{
    pub fn new(path: String, unicorn: Unicorn, log: Logger, handle: Handle, callback: F) -> Self {
        let future = Box::new(unicorn.subscribe(path.clone()));

        Self {
            path: path,
            unicorn: unicorn,
            log: log,
            handle: handle,
            state: Some(SubscribeState::Start(future)),
            attempts: 0,
            timeout_limit: Duration::new(32, 0),
            close: None,
            callback: callback,
        }
    }

    fn next_timeout(&self) -> Duration {
        // Hope that 2**18 seconds, which is ~3 days, fits everyone needs.
        let exp = cmp::min(self.attempts, 18);
        let duration = Duration::new(2u64.pow(exp), 0);

        cmp::min(duration, self.timeout_limit)
    }

    fn retry_later(&mut self) -> Poll<(), io::Error> {
        let timeout = self.next_timeout();
        cocaine_log!(self.log, Severity::Debug, "next timeout will fire in {}s", timeout.as_secs());

        self.state = Some(SubscribeState::Retry(Timeout::new(timeout, &self.handle)?));
        self.attempts += 1;
        return self.poll();
    }
}

impl<F> Future for SubscribeTask<F>
    where F: Fn(HashMap<String, f64>)
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.take().unwrap() {
            SubscribeState::Start(mut future) => {
                match future.poll() {
                    Ok(Async::Ready((close, stream))) => {
                        self.close = Some(close);
                        self.state = Some(SubscribeState::Fetch(Box::new(stream)));

                        cocaine_log!(self.log, Severity::Debug, "ready to subscribe ...");
                        return self.poll();
                    }
                    Ok(Async::NotReady) => {
                        self.state = Some(SubscribeState::Start(future));
                    }
                    Err(err) => {
                        cocaine_log!(self.log, Severity::Warn, "failed to subscribe: {}", err; {
                            path: self.path,
                        });
                        return self.retry_later();
                    }
                }
            }
            SubscribeState::Fetch(mut stream) => {
                loop {
                    match stream.poll() {
                        Ok(Async::Ready(Some((value, version)))) => {
                            self.attempts = 0;
                            cocaine_log!(self.log, Severity::Debug, "received subscription update: {:?}, version {}", value, version);

                            (self.callback)(value);
                        }
                        Ok(Async::NotReady) => {
                            break;
                        }
                        Ok(Async::Ready(None)) => {
                            return self.retry_later();
                        }
                        Err(err) => {
                            cocaine_log!(self.log, Severity::Warn, "failed to fetch subscriptions: {}", err; {
                                path: self.path,
                            });
                            return self.retry_later();
                        }
                    }
                }
                self.state = Some(SubscribeState::Fetch(stream));
            }
            SubscribeState::Retry(mut timeout) => {
                match timeout.poll() {
                    Ok(Async::Ready(())) => {
                        cocaine_log!(self.log, Severity::Debug, "timed out, trying to subscribe ...");

                        let future = self.unicorn.subscribe(self.path.clone()).boxed();
                        self.state = Some(SubscribeState::Start(future));
                        return self.poll();
                    }
                    Ok(Async::NotReady) => {
                        self.state = Some(SubscribeState::Retry(timeout));
                    }
                    Err(..) => return self.retry_later(),
                }
            }
        }

        Ok(Async::NotReady)
    }
}
