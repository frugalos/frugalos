//! Metrics for `frugalos_segment`.

use prometrics::metrics::{Counter, CounterBuilder, Histogram, HistogramBuilder};

use Result;

#[derive(Debug, Clone)]
pub struct MdsClientMetrics {
    pub(crate) request_max_retry_reached_total: Counter,
    pub(crate) request_retries_total: Histogram,
    pub(crate) request_timeout_total: Histogram,
    pub(crate) request_duration_seconds: Histogram,
}
impl MdsClientMetrics {
    pub(crate) fn new() -> Result<Self> {
        let request_max_retry_reached_total = track!(CounterBuilder::new(
            "mds_client_request_max_retry_reached_total"
        )
        .namespace("frugalos")
        .subsystem("segment")
        .help("Number of MDS request retries")
        .default_registry()
        .finish())?;
        let request_retries_total =
            track!(HistogramBuilder::new("mds_client_request_retries_total")
                .namespace("frugalos")
                .subsystem("segment")
                .help("MDS request retries")
                .bucket(0.0)
                .bucket(1.0)
                .bucket(2.0)
                .bucket(3.0)
                .bucket(4.0)
                .bucket(5.0)
                .default_registry()
                .finish())?;
        let request_timeout_total =
            track!(HistogramBuilder::new("mds_client_request_timeout_total")
                .namespace("frugalos")
                .subsystem("segment")
                .help("MDS request timeout")
                .bucket(0.0)
                .bucket(1.0)
                .bucket(2.0)
                .bucket(3.0)
                .bucket(4.0)
                .bucket(5.0)
                .default_registry()
                .finish())?;
        let request_duration_seconds =
            track!(HistogramBuilder::new("mds_client_request_duration_seconds")
                .namespace("frugalos")
                .subsystem("segment")
                .help("MDS request duration")
                .bucket(0.001)
                .bucket(0.005)
                .bucket(0.01)
                .bucket(0.05)
                .bucket(0.1)
                .bucket(0.5)
                .bucket(1.0)
                .bucket(2.0)
                .bucket(3.0)
                .bucket(4.0)
                .bucket(5.0)
                .default_registry()
                .finish())?;
        Ok(Self {
            request_max_retry_reached_total,
            request_retries_total,
            request_timeout_total,
            request_duration_seconds,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PutAllMetrics {
    pub(crate) failures_total: Counter,
    pub(crate) lost_fragments_total: Counter,
}

impl PutAllMetrics {
    pub(crate) fn new(client_name: &'static str) -> Result<Self> {
        let failures_total = track!(CounterBuilder::new("put_all_failures_total")
            .namespace("frugalos")
            .subsystem("segment")
            .help("Number of PutAll failures")
            .label("client", client_name)
            .default_registry()
            .finish())?;
        let lost_fragments_total = track!(CounterBuilder::new("put_all_lost_fragments_total")
            .namespace("frugalos")
            .subsystem("segment")
            .help("Number of lost fragments")
            .label("client", client_name)
            .default_registry()
            .finish())?;
        Ok(PutAllMetrics {
            failures_total,
            lost_fragments_total,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DispersedClientMetrics {
    pub(crate) put_all: PutAllMetrics,
}

impl DispersedClientMetrics {
    pub fn new() -> Result<Self> {
        let put_all = track!(PutAllMetrics::new("dispersed_client"))?;
        Ok(DispersedClientMetrics { put_all })
    }
}

#[derive(Debug, Clone)]
pub struct ReplicatedClientMetrics {
    pub(crate) put_all: PutAllMetrics,
}

impl ReplicatedClientMetrics {
    pub fn new() -> Result<Self> {
        let put_all = track!(PutAllMetrics::new("replicated_client"))?;
        Ok(ReplicatedClientMetrics { put_all })
    }
}
