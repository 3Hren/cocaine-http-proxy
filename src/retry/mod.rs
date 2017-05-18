//! Contains a `Retry` future wrapper that simplifies failed futures retrying using various
//! policies.

use std::io;
use std::iter::IntoIterator;
use std::time::Duration;

use futures::{Async, Future, IntoFuture, Poll};

use tokio_core::reactor::{Handle, Timeout};

/// Represents the errors possible during the execution of the `Retry`.
#[derive(Debug)]
pub enum Error<T, E> {
    /// Last result returned from the wrapped future.
    ///
    /// This value is returned if the policy has given up retrying, i.e. the iterator has been
    /// drained if it is not infinite. The value will contain an error if the last attempt has
    /// ended up with error. An `Ok` value is returned when the future resolved successfully, but
    /// it decided to continue retrying for some reasons, by returning `RepeatResult::Repeat`.
    Operation(Result<T, E>),
    /// Failed to create or activate the timer due to some I/O error.
    Timer(io::Error),
}

/// A value that must be returned from the future that is wrapped with `Retry`.
///
/// Passing an `Ok` will terminate the `Retry` future, passing result through. The `Repeat` value
/// will not terminate the future, instead a new future will be created from the action as like as
/// the retry policy reset and the computation will go on.
pub enum RepeatResult<T> {
    Ok(T),
    Repeat(T),
}

enum State<F> {
    Run(F),
    Sleep(Timeout),
}

/// An action can be run multiple times and produces a future.
pub trait Action {
    /// The future that this action produces.
    type Future: Future;

    /// Constructs a new future.
    ///
    /// This is called before each retry attempt.
    fn run(&mut self) -> Self::Future;
}

impl<T: IntoFuture, F: FnMut() -> T> Action for F {
    type Future = T::Future;

    fn run(&mut self) -> Self::Future {
        self().into_future()
    }
}

/// A `Future` that drives multiple attempts for futures produces by an action via a retry policy.
pub struct Retry<T, F, U, P> {
    /// Future producer.
    action: T,
    /// Current state.
    state: Option<State<F>>,
    /// Retry policy reset handle.
    policy_reset: U,
    /// Retry policy.
    policy: P,
    /// I/O loop handle.
    handle: Handle,
}

impl<T, F, R, U, P> Retry<T, F, U, P>
    where T: Action<Future = F>,
          F: Future<Item = RepeatResult<R>>,
          U: IntoIterator<Item = Duration, IntoIter = P> + Clone,
          P: Iterator<Item = Duration>
{
    pub fn new(mut action: T, policy: U, handle: Handle) -> Self {
        Self {
            state: Some(State::Run(action.run())),
            action: action,
            policy_reset: policy.clone(),
            policy: policy.into_iter(),
            handle: handle,
        }
    }

    fn retry(&mut self, result: Result<R, F::Error>) -> Poll<R, Error<R, F::Error>> {
        match self.policy.next() {
            Some(duration) => {
                let future = Timeout::new(duration, &self.handle)
                    .map_err(Error::Timer)?;
                self.state = Some(State::Sleep(future));
                return self.poll();
            }
            None => Err(Error::Operation(result)),
        }
    }
}

impl<T, F, R, U, P> Future for Retry<T, F, U, P>
    where T: Action<Future = F>,
          F: Future<Item = RepeatResult<R>>,
          U: IntoIterator<Item = Duration, IntoIter = P> + Clone,
          P: Iterator<Item = Duration>
{
    type Item = R;
    type Error = Error<R, F::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.take().expect("invalid internal state") {
            State::Run(mut future) => {
                match future.poll() {
                    Ok(Async::Ready(RepeatResult::Ok(v))) => {
                        Ok(Async::Ready(v))
                    }
                    Ok(Async::Ready(RepeatResult::Repeat(v))) => {
                        self.policy = self.policy_reset.clone().into_iter();
                        self.retry(Ok(v))
                    }
                    Ok(Async::NotReady) => {
                        self.state = Some(State::Run(future));
                        Ok(Async::NotReady)
                    }
                    Err(err) => self.retry(Err(err)),
                }
            }
            State::Sleep(mut timeout) => {
                match timeout.poll() {
                    Ok(Async::Ready(())) => {
                        self.state = Some(State::Run(self.action.run()));
                        self.poll()
                    }
                    Ok(Async::NotReady) => {
                        self.state = Some(State::Sleep(timeout));
                        Ok(Async::NotReady)
                    }
                    Err(err) => Err(Error::Timer(err)),
                }
            }
        }
    }
}

// Retry::from(|| future)
//     .with_policy(ExponentialBackOff::new(1..).map(jitter).take(3))
//     .build(handle)
//[1, 3, 7, 15].iter().chain(15..)
