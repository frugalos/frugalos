//! Metrics for `frugalos_segment`.

use prometrics::metrics::{Counter, CounterBuilder};

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
    pub(crate) failures_total: Counter,
    pub(crate) corrupted_total: Counter,
    pub(crate) timeout_total: Counter,
}

impl CollectFragmentsMetrics {
    pub(crate) fn new() -> Result<Self> {
        let failures_total = track!(CounterBuilder::new("collect_fragment_failures_total")
            .namespace("frugalos")
            .subsystem("segment")
            .help("Number of failures of collecting fragments")
            .default_registry()
            .finish())?;
        let corrupted_total = track!(CounterBuilder::new("collect_fragment_corrupted_total")
            .namespace("frugalos")
            .subsystem("segment")
            .help("Number of corrupted fragments")
            .default_registry()
            .finish())?;
        let timeout_total = track!(CounterBuilder::new("collect_fragment_timeout_total")
            .namespace("frugalos")
            .subsystem("segment")
            .help("Number of timeout fragments")
            .default_registry()
            .finish())?;
        Ok(Self {
            failures_total,
            corrupted_total,
            timeout_total,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DispersedClientMetrics {
    pub(crate) put_all: PutAllMetrics,
    pub(crate) collect_fragments: CollectFragmentsMetrics,
}

impl DispersedClientMetrics {
    pub fn new() -> Result<Self> {
        let put_all = track!(PutAllMetrics::new("dispersed_client"))?;
        let collect_fragments = track!(CollectFragmentsMetrics::new())?;
        Ok(DispersedClientMetrics {
            put_all,
            collect_fragments,
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
