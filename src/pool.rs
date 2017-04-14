use std::boxed::FnBox;
use std::collections::HashMap;
use std::iter;
use std::mem;
use std::time::{Duration, SystemTime};

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

struct ServicePool {
    /// Next service.
    counter: usize,
    name: String,
    /// Reconnect threshold.
    threshold: Duration,
    handle: Handle,
    last_traverse: SystemTime,

    services: Vec<(Service, SystemTime)>,
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
            services:iter::repeat(name)
                .take(limit)
                .map(|name| (Service::new(name.clone(), handle), now))
                .collect()
        }
    }

    fn next(&mut self) -> &Service {
        let now = SystemTime::now();

        // No often than every 5 seconds we're traversing services for reconnection.
        if now.duration_since(self.last_traverse).unwrap() > Duration::new(5, 0) {
            self.last_traverse = now;

            let mut killed = 0;

            let kill_limit = self.services.len() / 2;
            for &mut (ref mut service, ref mut birth) in self.services.iter_mut() {
                if now.duration_since(*birth).unwrap() > self.threshold {
                    killed += 1;
                    mem::replace(birth, now);
                    mem::replace(service, Service::new(self.name.clone(), &self.handle));

                    if killed > kill_limit {
                        break;
                    }
                }
                // TODO: Tiny pre-connection optimizations.
            }
        }

        self.counter = (self.counter + 1) % self.services.len();
        &self.services[self.counter].0
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
    rx: mpsc::UnboundedReceiver<Event>,

    pool: HashMap<String, ServicePool>,
}

impl PoolTask {
    pub fn new(handle: Handle, rx: mpsc::UnboundedReceiver<Event>) -> Self {
        Self {
            handle: handle,
            rx: rx,
            pool: HashMap::new(),
        }
    }

    fn select_service(&mut self, name: String, handle: &Handle) -> &Service {
        let mut pool = self.pool.entry(name.clone())
            .or_insert_with(|| ServicePool::new(name, 10, handle));
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
                        Event::ServiceConnect(..) => {
                            unimplemented!();
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
