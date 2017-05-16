use std::cmp;
use std::io;
use std::time::Duration;

use futures::{Async, Future, Poll};

use tokio_core::reactor::{Handle, Timeout};

const MAX_ATTEMPTS: u32 = 18;
const MAX_DURATION_SECS: u32 = 32;

/// A value that must be returned from the future that is wrapped with `ExponentialBackOff`.
///
/// Passing a `Terminate` will terminate the wrapper future, passing result through. The `Repeat`
/// value will not terminate the future, instead a new future will be created from the factory and
/// the computation will go on.
pub enum RepeatResult<T> {
    Repeat,
    Terminate(T),
}

enum State<F> {
    Fetch(F),
    Retry(Timeout),
}

pub trait FutureFactory {
    type Item: Future;

    /// Constructs a new future.
    ///
    /// This is called before each retry attempt.
    fn create(&mut self) -> Self::Item;
}

impl<F, T> FutureFactory for F
    where F: FnMut() -> T,
          T: Future
{
    type Item = T;

    fn create(&mut self) -> Self::Item {
        (*self)()
    }
}

/// Wraps a future into an another infinite future that will repeatedly retry on error using
/// exponential backoff algorithm to calculate timeouts.
pub struct ExponentialBackOff<T, F> {
    factory: T,
    state: Option<State<F>>,
    /// I/O loop handle.
    handle: Handle,
    /// Number of unsuccessful attempts.
    attempts: u32,
    /// Maximum timeout value. Actual timeout is calculated using `2 ** attempts` formula, but is
    /// truncated to this value if oversaturated.
    timeout_limit: Duration,
}

impl<T, F, R> ExponentialBackOff<T, F>
    where T: FutureFactory<Item=F>,
          F: Future<Item=RepeatResult<R>, Error=()>
{
    pub fn new(mut factory: T, handle: Handle) -> Self {
        let future = factory.create();

        Self {
            factory: factory,
            state: Some(State::Fetch(future)),
            handle: handle,
            attempts: 0,
            timeout_limit: Duration::new(MAX_DURATION_SECS, 0),
        }
    }

    fn next_duration(&self) -> Duration {
        // Hope that 2**18 seconds, which is ~3 days, fits everyone needs.
        let exp = cmp::min(self.attempts, MAX_ATTEMPTS);
        let duration = Duration::new(2u64.pow(exp), 0);

        cmp::min(duration, self.timeout_limit)
    }

    fn retry_later(&mut self) -> Poll<R, io::Error> {
        let timeout = Timeout::new(self.next_duration(), &self.handle)?;

        self.attempts += 1;
        self.state = Some(State::Retry(timeout));
        return self.poll();
    }
}

impl<T, F, R> Future for ExponentialBackOff<T, F>
    where T: FutureFactory<Item=F>,
          F: Future<Item=RepeatResult<R>, Error=()>
{
    type Item = R;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.take().unwrap() {
            State::Fetch(mut future) => {
                match future.poll() {
                    Ok(Async::Ready(RepeatResult::Repeat)) => {
                        self.attempts = 0;
                        return self.retry_later();
                    }
                    Ok(Async::Ready(RepeatResult::Terminate(v))) => {
                        return Ok(Async::Ready(v));
                    }
                    Ok(Async::NotReady) => self.state = Some(State::Fetch(future)),
                    Err(()) => return self.retry_later(),
                }
            }
            State::Retry(mut timeout) => {
                match timeout.poll() {
                    Ok(Async::Ready(())) => {
                        self.state = Some(State::Fetch(self.factory.create()));
                        return self.poll();
                    }
                    Ok(Async::NotReady) => self.state = Some(State::Retry(timeout)),
                    Err(..) => return self.retry_later(),
                }
            }
        }

        Ok(Async::NotReady)
    }
}
