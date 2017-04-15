use std::sync::atomic::{AtomicI64, Ordering};

use time;

use super::ewma::EWMA;

/// A `Meter` measures the rate at which a set of events occur.
///
/// Just like the Unix load averages visible in `top`.
pub trait Meter: Sync {
    /// Returns the number of events which have been marked.
    fn count(&self) -> i64;

    /// Returns the mean rate at which events have occurred since the meter was created.
    fn mean_rate(&self) -> f64;

    /// Returns the one-minute exponentially-weighted moving average rate at which events have
    /// occurred since the meter was created.
    fn m01rate(&self) -> f64;

    /// Returns the five-minute exponentially-weighted moving average rate at which events have
    /// occurred since the meter was created.
    fn m05rate(&self) -> f64;

    /// Returns the fifteen-minute exponentially-weighted moving average rate at which events have
    /// occurred since the meter was created.
    fn m15rate(&self) -> f64;

    /// Mark the occurrence of a given number of events.
    fn mark(&self, value: i64);
}

pub trait Clock: Sync {
    fn now(&self) -> i64;
}

#[derive(Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        time::get_time().sec
    }
}

#[derive(Debug)]
pub struct RateMeter<C: Clock = SystemClock> {
    clock: C,

    birthstamp: i64,
    prev: AtomicI64,

    count: AtomicI64,
    rates: [EWMA; 3],
}

impl<C: Clock> RateMeter<C> {
    fn with(clock: C) -> Self {
        let birthstamp = clock.now();

        Self {
            count: AtomicI64::new(0),
            clock: clock,
            birthstamp: birthstamp,
            prev: AtomicI64::new(birthstamp),
            rates: [EWMA::m01rate(), EWMA::m05rate(), EWMA::m15rate()],
        }
    }

    fn tick_maybe(&self) {
        let now = self.clock.now();
        let old = self.prev.load(Ordering::Acquire);
        let elapsed = now - old;

        if elapsed > 5 {
            // Clock values should monotonically increase, so no ABA problem here is possible.
            if self.prev.compare_and_swap(old, now - elapsed % 5, Ordering::Release) == old {
                let ticks = elapsed / 5;

                for _ in 0..ticks {
                    for rate in &self.rates {
                        rate.tick();
                    }
                }
            }
        }
    }
}

impl RateMeter<SystemClock> {
    pub fn new() -> Self {
        Self::with(SystemClock)
    }
}

impl Default for RateMeter<SystemClock> {
    fn default() -> Self {
        RateMeter::new()
    }
}

impl<C: Clock> Meter for RateMeter<C> {
    fn count(&self) -> i64 {
        self.count.load(Ordering::Acquire)
    }

    fn mean_rate(&self) -> f64 {
        let count = self.count.load(Ordering::Acquire);

        if count == 0 {
            return 0.0;
        }

        let elapsed = self.clock.now() - self.birthstamp;

        count as f64 / elapsed as f64
    }

    fn m01rate(&self) -> f64 {
        self.tick_maybe();
        self.rates[0].rate()
    }

    fn m05rate(&self) -> f64 {
        self.tick_maybe();
        self.rates[1].rate()
    }

    fn m15rate(&self) -> f64 {
        self.tick_maybe();
        self.rates[2].rate()
    }

    fn mark(&self, value: i64) {
        self.tick_maybe();

        self.count.fetch_add(value, Ordering::Release);

        for rate in &self.rates {
            rate.update(value);
        }
    }
}
