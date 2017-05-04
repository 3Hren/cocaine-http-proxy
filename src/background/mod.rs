enum State<F> {
    Fetch(F),
    Retry(Timeout),
}

pub trait FutureFactory {
    type Item: Future;

    fn create(&mut self) -> Self::Item;
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

impl<T, F> ExponentialBackOff<T, F>
    where T: FutureFactory<Item=F>,
          F: Future<Item=(), Error=()>
{
    pub fn new(mut factory: T, handle: Handle) -> Self {
        let future = factory.create();

        Self {
            factory: factory,
            state: Some(State::Fetch(future)),
            handle: handle,
            attempts: 0,
            timeout_limit: Duration::new(32, 0),
        }
    }

    fn next(&self) -> Duration {
        // Hope that 2**18 seconds, which is ~3 days, fits everyone needs.
        let exp = cmp::min(self.attempts, 18);
        let duration = Duration::new(2u64.pow(exp), 0);

        cmp::min(duration, self.timeout_limit)
    }

    fn retry_later(&mut self) -> Poll<(), io::Error> {
        let timeout = Timeout::new(self.next(), &self.handle)?;

        self.attempts += 1;
        self.state = Some(State::Retry(timeout));
        return self.poll();
    }
}

impl<T, F> Future for ExponentialBackOff<T, F>
    where T: FutureFactory<Item=F>,
          F: Future<Item=(), Error=()>
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.take().unwrap() {
            State::Fetch(mut future) => {
                match future.poll() {
                    Ok(Async::Ready(..)) => {
                        self.attempts = 0;
                        return self.retry_later();
                    }
                    Ok(Async::NotReady) => self.state = Some(State::Fetch(future)),
                    Err(..) => return self.retry_later(),
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
