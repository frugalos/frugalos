//! Metrics for `frugalos_segment`.

use prometrics::metrics::{Counter, CounterBuilder, Histogram, HistogramBuilder};

use Result;

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
pub struct CollectFragmentsMetrics {
    pub(crate) corrupted_fragments_total: Counter,
    pub(crate) get_fragment_failures_total: Counter,
    pub(crate) get_fragment_timeout_total: Counter,
    pub(crate) get_fragment_success_duration_seconds: Histogram,
    pub(crate) missing_fragments_total: Counter,
}
impl CollectFragmentsMetrics {
    fn new() -> Result<Self> {
        let corrupted_fragments_total = track!(CounterBuilder::new(
            "collect_fragments_corrupted_fragments_total"
        )
        .namespace("frugalos")
        .subsystem("segment")
        .help("Number of corrupted fragments")
        .default_registry()
        .finish())?;
        let get_fragment_failures_total = track!(CounterBuilder::new(
            "collect_fragments_get_fragment_failures_total"
        )
        .namespace("frugalos")
        .subsystem("segment")
        .help("Number of failures when getting a fragment")
        .default_registry()
        .finish())?;
        let get_fragment_timeout_total = track!(CounterBuilder::new(
            "collect_fragments_get_fragment_timeout_total"
        )
        .namespace("frugalos")
        .subsystem("segment")
        .help("Number of timeout when getting a fragment")
        .default_registry()
        .finish())?;
        let get_fragment_success_duration_seconds = track!(HistogramBuilder::new(
            "collect_fragments_get_fragment_success_duration_seconds"
        )
        .namespace("frugalos")
        .subsystem("segment")
        .help("Get a fragment duration excluding failed fragments")
        .bucket(0.001)
        .bucket(0.005)
        .bucket(0.01)
        .bucket(0.05)
        .bucket(0.1)
        .bucket(0.5)
        .bucket(1.0)
        .bucket(2.0)
        .bucket(4.0)
        .bucket(8.0)
        .default_registry()
        .finish())?;
        let missing_fragments_total = track!(CounterBuilder::new(
            "collect_fragments_missing_fragments_total"
        )
        .namespace("frugalos")
        .subsystem("segment")
        .help("Number of missing fragments")
        .default_registry()
        .finish())?;
        Ok(CollectFragmentsMetrics {
            corrupted_fragments_total,
            get_fragment_failures_total,
            get_fragment_timeout_total,
            get_fragment_success_duration_seconds,
            missing_fragments_total,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DispersedClientMetrics {
    pub(crate) put_all: PutAllMetrics,
    pub(crate) collect_fragments: CollectFragmentsMetrics,
    pub(crate) dispersed_get_duration_seconds: Histogram,
    pub(crate) dispersed_get_failures_total: Counter,
    pub(crate) dispersed_head_failures_total: Counter,
    pub(crate) dispersed_put_failures_total: Counter,
}

impl DispersedClientMetrics {
    pub fn new() -> Result<Self> {
        let put_all = track!(PutAllMetrics::new("dispersed_client"))?;
        let collect_fragments = track!(CollectFragmentsMetrics::new())?;
        let dispersed_get_duration_seconds =
            track!(HistogramBuilder::new("dispersed_get_duration_seconds")
                .namespace("frugalos")
                .subsystem("segment")
                .help("DispersedGet duration")
                .bucket(0.001)
                .bucket(0.005)
                .bucket(0.01)
                .bucket(0.05)
                .bucket(0.1)
                .bucket(0.5)
                .bucket(1.0)
                .bucket(2.0)
                .bucket(4.0)
                .bucket(8.0)
                .bucket(16.0)
                .bucket(32.0)
                .default_registry()
                .finish())?;
        let dispersed_get_failures_total =
            track!(CounterBuilder::new("dispersed_get_failures_total")
                .namespace("frugalos")
                .subsystem("segment")
                .help("Number of dispersed get failures")
                .default_registry()
                .finish())?;
        let dispersed_head_failures_total =
            track!(CounterBuilder::new("dispersed_head_failures_total")
                .namespace("frugalos")
                .subsystem("segment")
                .help("Number of dispersed head failures")
                .default_registry()
                .finish())?;
        let dispersed_put_failures_total =
            track!(CounterBuilder::new("dispersed_put_failures_total")
                .namespace("frugalos")
                .subsystem("segment")
                .help("Number of dispersed put failures")
                .default_registry()
                .finish())?;
        Ok(DispersedClientMetrics {
            put_all,
            collect_fragments,
            dispersed_get_duration_seconds,
            dispersed_get_failures_total,
            dispersed_head_failures_total,
            dispersed_put_failures_total,
        })
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
