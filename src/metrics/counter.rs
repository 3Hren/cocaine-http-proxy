use std::sync::atomic::{AtomicI64, Ordering};

/// An incrementing and decrementing counter metric.
pub trait Count: Sync {
    /// Underlying value.
    type Value;

    /// Loads a value.
    fn get(&self) -> Self::Value;

    /// Adds to the current value, returning the previous value.
    fn add(&self, value: Self::Value) -> Self::Value;
}

/// An atomic counter implementation.
#[derive(Debug, Default)]
pub struct Counter {
    v: AtomicI64,
}

impl Count for Counter {
    type Value = i64;

    fn get(&self) -> Self::Value {
        self.v.load(Ordering::Acquire)
    }

    fn add(&self, value: Self::Value) -> Self::Value {
        self.v.fetch_add(value, Ordering::Release)
    }
}
