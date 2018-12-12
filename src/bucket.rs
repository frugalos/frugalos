#![allow(clippy::ptr_arg)]
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use frugalos_segment::config::{ClusterMember, MdsClientConfig};
use frugalos_segment::Client as Segment;
use frugalos_segment::{self, ErasureCoder};
use libfrugalos::entity::bucket::Bucket as BucketConfig;
use libfrugalos::entity::object::ObjectId;
use siphasher;
use slog::Logger;
use std::iter;

#[derive(Clone)]
pub struct Bucket {
    logger: Logger,
    rpc_service: RpcServiceHandle,
    ec: Option<ErasureCoder>,
    storage_config: frugalos_segment::config::Storage,
    mds_client_config: MdsClientConfig,
    segments: Vec<Segment>,
}
impl Bucket {
    pub fn new(
        logger: Logger,
        rpc_service: RpcServiceHandle,
        config: &BucketConfig,
        mds_client_config: MdsClientConfig,
    ) -> Self {
        let ec = match config {
            BucketConfig::Metadata(_) => None,
            BucketConfig::Replicated(_) => None,
            BucketConfig::Dispersed(ref c) => Some(frugalos_segment::build_ec(
                c.data_fragment_count as usize,
                c.tolerable_faults as usize,
            )),
        };

        let storage_config = match config {
            BucketConfig::Metadata(_) => frugalos_segment::config::Storage::Metadata,
            BucketConfig::Replicated(ref b) => {
                let c = frugalos_segment::config::ReplicatedConfig {
                    tolerable_faults: b.tolerable_faults as u8,
                };
                frugalos_segment::config::Storage::Replicated(c)
            }
            BucketConfig::Dispersed(ref b) => {
                let c = frugalos_segment::config::DispersedConfig {
                    tolerable_faults: b.tolerable_faults as u8,
                    fragments: (b.tolerable_faults + b.data_fragment_count) as u8,
                };
                frugalos_segment::config::Storage::Dispersed(c)
            }
        };

        let segment_config = frugalos_segment::config::ClientConfig {
            cluster: frugalos_segment::config::ClusterConfig {
                members: Vec::new(),
            },
            storage: storage_config.clone(),
            mds: mds_client_config.clone(),
        };
        let segment = Segment::new(
            logger.clone(),
            rpc_service.clone(),
            segment_config,
            ec.clone(),
        );
        let segments = iter::repeat(segment)
            .take(config.segment_count() as usize)
            .collect();
        Bucket {
            logger,
            rpc_service,
            ec,
            storage_config,
            segments,
            mds_client_config,
        }
    }
    pub fn update_segment(&mut self, segment_no: u16, members: Vec<ClusterMember>) {
        let segment_config = frugalos_segment::config::ClientConfig {
            cluster: frugalos_segment::config::ClusterConfig { members },
            storage: self.storage_config.clone(),
            mds: self.mds_client_config.clone(),
        };
        let segment = Segment::new(
            self.logger.clone(),
            self.rpc_service.clone(),
            segment_config,
            self.ec.clone(),
        );
        self.segments[segment_no as usize] = segment;
    }
    pub fn get_segment(&self, id: &ObjectId) -> &Segment {
        use std::hash::{Hash, Hasher};
        let mut hasher = siphasher::sip::SipHasher13::new();
        id.hash(&mut hasher);
        let i = hasher.finish() as usize % self.segments.len();
        &self.segments[i]
    }
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
}
