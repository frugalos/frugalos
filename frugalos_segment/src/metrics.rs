//! Metrics for `frugalos_segment`.

use prometrics::metrics::{Counter, CounterBuilder};

use Result;

#[derive(Debug, Clone)]
pub struct PutAllMetrics {
    pub(crate) failures_total: Counter,
    pub(crate) lost_fragments_total: Counter,
}

impl PutAllMetrics {
    pub(crate) fn new(client_name: &str) -> Result<Self> {
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
