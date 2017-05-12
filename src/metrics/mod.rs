//! This module contains implementation of some useful metrics, like `Counter`, `Meter` etc.

pub use self::counter::{Count, Counter};
pub use self::meter::{Meter, RateMeter};

mod counter;
mod ewma;
mod meter;
