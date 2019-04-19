//! This crate provides some helpers for metrics.

use prometrics::metrics::{Histogram, HistogramBuilder};

use {Error, Result};

/// Creates a histogram, which can be used as default.
pub fn make_histogram(builder: &mut HistogramBuilder) -> Result<Histogram> {
    builder
        .bucket(0.0001)
        .bucket(0.0005)
        .bucket(0.001)
        .bucket(0.005)
        .bucket(0.01)
        .bucket(0.05)
        .bucket(0.1)
        .bucket(0.5)
        .bucket(1.0)
        .bucket(5.0)
        .bucket(10.0)
        .default_registry()
        .finish()
        .map_err(|e| track!(Error::from(e)))
}
