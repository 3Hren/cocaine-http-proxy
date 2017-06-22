use std::boxed::FnBox;
use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::iter;
use std::mem;
use std::time::{Duration, SystemTime};

use futures::{Async, Future, Poll, Stream};
use futures::future::Loop;
use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use rand;
use tokio_core::reactor::Handle;
use uuid::Uuid;

use cocaine::{Error, Service};
use cocaine::hpack::Header;
use cocaine::logging::{Logger, Severity};
use cocaine::service::Locator;
use cocaine::service::locator::HashRing;
use cocaine::service::tvm::{Grant, Tvm};
use cocaine::service::unicorn::{Close, Unicorn, Version};

use config::{Config, PoolConfig, ServicePoolConfig};
use retry::Action;

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub verbose: bool,
//    timeout: f64,
}

pub enum Event {
    Service {
        name: String,
        func: Box<FnBox(&Service, Settings) -> Box<Future<Item = (), Error = ()> + Send> + Send>,
    },
    OnServiceConnect(Service),
    OnRoutingUpdates(HashMap<String, HashRing>),
    OnTracingUpdates(HashMap<String, f64>),
    OnTimeoutUpdates(HashMap<String, f64>),
}

#[derive(Clone)]
pub struct EventDispatch {
    senders: Vec<UnboundedSender<Event>>,
}

impl EventDispatch {
    pub fn new(senders: Vec<UnboundedSender<Event>>) -> Self {
        Self { senders: senders }
    }

    pub fn send(&self, event: Event) {
        let rand = rand::random::<usize>();
        let roll = rand % self.senders.len();
        mem::drop(self.senders[roll].send(event));
    }

    pub fn send_all<F>(&self, f: F)
        where F: Fn() -> Event
    {
        for sender in &self.senders {
            mem::drop(sender.send(f()));
        }
    }

    /// Destructs this dispatcher, yielding underlying senders.
    pub fn into_senders(self) -> Vec<UnboundedSender<Event>> {
        self.senders
    }
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

struct Tracing {
    default: f64,
    precise: HashMap<String, f64>,
}

impl Tracing {
    fn new(default: f64) -> Self {
        Self {
            default: default,
            precise: HashMap::new(),
        }
    }

    fn reset(&mut self, precise: HashMap<String, f64>) {
        self.precise = precise;
    }

    fn probability_for(&self, name: &String) -> f64 {
        *self.precise.get(name).unwrap_or(&self.default)
    }

    fn calculate_trace_bit(&mut self, name: &String) -> bool {
        rand::random::<f64>() <= self.probability_for(name)
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

    tracing: Tracing,
    timeouts: HashMap<String, f64>,
}

impl PoolTask {
    pub fn new(handle: Handle, log: Logger, tx: UnboundedSender<Event>, rx: UnboundedReceiver<Event>, cfg: Config) -> Self {
        Self {
            handle: handle,
            log: log,
            tx: tx,
            rx: rx,
            cfg: cfg.pool().clone(),
            pool: HashMap::new(),
            tracing: Tracing::new(cfg.tracing().probability()),
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
                            let settings = Settings {
                                verbose: self.tracing.calculate_trace_bit(&name)
                            };

                            // Select the next service that is not reconnecting right now.
                            let handle = self.handle.clone();
                            let ref service = self.select_service(name, &handle);

                            let future = func.call_box((service, settings));
                            handle.spawn(future);
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
                            self.tracing.reset(tracing);
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

pub struct RoutingGroupsAction {
    locator: Locator,
    dispatcher: EventDispatch,
    log: Logger,
}

impl RoutingGroupsAction {
    pub fn new(locator: Locator, dispatcher: EventDispatch, log: Logger) -> Self {
        Self {
            locator: locator,
            dispatcher: dispatcher,
            log: log,
        }
    }
}

impl Action for RoutingGroupsAction {
    type Future = RoutingGroupsUpdateTask;

    fn run(&mut self) -> Self::Future {
        let uuid = Uuid::new_v4().hyphenated().to_string();
        let stream = self.locator.routing(&uuid);

        RoutingGroupsUpdateTask {
            dispatcher: self.dispatcher.clone(),
            log: self.log.clone(),
            uuid: uuid,
            stream: stream.boxed(),
        }
    }
}

pub struct RoutingGroupsUpdateTask {
    dispatcher: EventDispatch,
    log: Logger,
    uuid: String,
    stream: Box<Stream<Item=HashMap<String, HashRing>, Error=Error>>,
}

impl Future for RoutingGroupsUpdateTask {
    type Item = Loop<(), ()>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.stream.poll() {
                Ok(Async::Ready(Some(groups))) => {
                    cocaine_log!(self.log, Severity::Info, "received {} RG(s) updates", groups.len(); {
                        uuid: self.uuid,
                    });

                    self.dispatcher.send_all(|| Event::OnRoutingUpdates(groups.clone()));
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    cocaine_log!(self.log, Severity::Info, "locator has closed RG subscription"; {
                        uuid: self.uuid,
                    });
                    return Ok(Async::Ready(Loop::Continue(())));
                }
                Err(err) => {
                    cocaine_log!(self.log, Severity::Warn, "failed to update RG: {}", err; {
                        uuid: self.uuid,
                    });
                    return Err(err);
                }
            }
        }
    }
}

type SubscribeStream = Box<Stream<Item=(Option<HashMap<String, f64>>, Version), Error=Error> + Send>;

enum SubscribeState {
    Start(Box<Future<Item=(Close, SubscribeStream), Error=Error>>),
    Fetch(SubscribeStream),
}

pub struct SubscribeTask<F> {
    /// Path subscribed on.
    path: String,
    /// Current state.
    state: Option<SubscribeState>,
    /// Close handle to be able to notify the Unicorn that we no longer needed for updates.
    close: Option<Close>,
    callback: F,
    /// Logger service.
    log: Logger,
}

pub trait Factory {
    type Item;
    type Error;
    type Future: Future<Item = Self::Item, Error = Self::Error>;

    fn create(&mut self) -> Self::Future;
}

#[derive(Clone, Debug)]
pub struct TicketFactory {
    tvm: Tvm,
    client_id: u32,
    client_secret: String,
    grant: Grant,
}

impl TicketFactory {
    pub fn new(tvm: Tvm, client_id: u32, client_secret: String, grant: Grant) -> Self {
        Self {
            tvm: tvm,
            client_id: client_id,
            client_secret: client_secret,
            grant: grant,
        }
    }
}

impl Factory for TicketFactory {
    type Item = String;
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Error> + Send>;

    fn create(&mut self) -> Self::Future {
        self.tvm.ticket(self.client_id, &self.client_secret, self.grant.clone()).boxed()
    }
}

pub struct SubscribeAction<T, F> {
    /// Path subscribed on.
    path: String,
    /// Authorization ticket factory.
    tm: T,
    /// Unicorn service.
    unicorn: Unicorn,
    /// Callback.
    callback: F,
    /// Logger service.
    log: Logger,
}

impl<T, F> SubscribeAction<T, F>
    where F: Fn(HashMap<String, f64>) + Clone
{
    pub fn new(path: String, tm: T, unicorn: Unicorn, callback: F, log: Logger) -> Self {
        Self {
            path: path,
            tm: tm,
            unicorn: unicorn,
            callback: callback,
            log: log,
        }
    }
}

impl<T, F, U> Action for SubscribeAction<T, F>
where
    // TODO: Looks creepy, refactor somehow.
    T: Factory<Item = String, Error = Error, Future = U>,
    F: Fn(HashMap<String, f64>) + Clone,
    U: Future<Item = String, Error = Error> + Send + 'static
{
    type Future = SubscribeTask<F>;

    fn run(&mut self) -> Self::Future {
        let unicorn = self.unicorn.clone();
        let path = self.path.clone();

        let future = self.tm.create().and_then(move |ticket| {
            let auth = Header::new(
                // TODO: Use hardcoded names.
                "authorization".as_bytes(),
                format!("TVM {}", ticket).into_bytes()
            );

            Box::new(unicorn.subscribe(path, Some(vec![auth])))
        });

        SubscribeTask {
            path: self.path.clone(),
            state: Some(SubscribeState::Start(future.boxed())),
            close: None,
            callback: self.callback.clone(),
            log: self.log.clone(),
        }
    }
}

impl<F> Future for SubscribeTask<F>
    where F: Fn(HashMap<String, f64>)
{
    type Item = Loop<(), ()>;
    type Error = Error;

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
                        return Err(err);
                    }
                }
            }
            SubscribeState::Fetch(mut stream) => {
                loop {
                    match stream.poll() {
                        Ok(Async::Ready(Some((value, version)))) => {
                            let value = value.unwrap_or_default();
                            cocaine_log!(self.log, Severity::Debug, "received subscription update: {:?}, version {}", value, version);

                            (self.callback)(value);
                        }
                        Ok(Async::NotReady) => {
                            break;
                        }
                        Ok(Async::Ready(None)) => {
                            return Ok(Async::Ready(Loop::Continue(())));
                        }
                        Err(err) => {
                            cocaine_log!(self.log, Severity::Warn, "failed to fetch subscriptions: {}", err; {
                                path: self.path,
                            });
                            return Err(err);
                        }
                    }
                }
                self.state = Some(SubscribeState::Fetch(stream));
            }
        }

        Ok(Async::NotReady)
    }
}
