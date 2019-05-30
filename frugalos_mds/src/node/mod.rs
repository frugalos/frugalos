#![allow(clippy::module_inception)]
use fibers::sync::oneshot::Monitored;
use frugalos_raft::NodeId;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, Metadata, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use prometrics::metrics::{Counter, Histogram, MetricBuilder};
use raftlog::log::ProposalId;
use std::time::Instant;
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};

pub use self::handle::NodeHandle;
pub use self::node::Node;

mod handle;
mod metrics;
mod node;
mod snapshot;

type Reply<T> = Monitored<T, Error>;

/// Raftに提案中のコマンド.
#[derive(Debug)]
enum Proposal {
    Put(
        ProposalId,
        Instant,
        ProposalMetrics,
        Reply<(ObjectVersion, Option<ObjectVersion>)>,
    ),
    Delete(
        ProposalId,
        Instant,
        ProposalMetrics,
        Reply<Option<ObjectVersion>>,
    ),
    DeleteByPrefix(
        ProposalId,
        Instant,
        ProposalMetrics,
        ObjectPrefix,
        Reply<DeleteObjectsByPrefixSummary>,
    ),
}
impl Proposal {
    pub fn id(&self) -> ProposalId {
        match *self {
            Proposal::Put(id, ..) => id,
            Proposal::Delete(id, ..) => id,
            Proposal::DeleteByPrefix(id, ..) => id,
        }
    }
    fn started_at(&self) -> Instant {
        match *self {
            Proposal::Put(_, at, ..) => at,
            Proposal::Delete(_, at, ..) => at,
            Proposal::DeleteByPrefix(_, at, ..) => at,
        }
    }
    fn metrics(&self) -> &ProposalMetrics {
        match *self {
            Proposal::Put(_, _, ref metrics, ..) => metrics,
            Proposal::Delete(_, _, ref metrics, ..) => metrics,
            Proposal::DeleteByPrefix(_, _, ref metrics, ..) => metrics,
        }
    }
    pub fn notify_committed(self, old: &[ObjectVersion]) {
        let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at().elapsed());
        self.metrics()
            .committed_proposal_duration_seconds
            .observe(elapsed);
        self.metrics().committed_proposal_total.increment();
        match self {
            Proposal::Put(id, _, _, monitored) => match old {
                [] => monitored.exit(Ok((ObjectVersion(id.index.as_u64()), None))),
                [old] => monitored.exit(Ok((ObjectVersion(id.index.as_u64()), Some(*old)))),
                _ => monitored.exit(Err(ErrorKind::InvalidInput
                    .cause(format!("Expected [] or [ObjectVersion] but got {:?}", old))
                    .into())),
            },
            Proposal::Delete(_, _, _, monitored) => match old {
                [] => monitored.exit(Ok(None)),
                [old] => monitored.exit(Ok(Some(*old))),
                _ => monitored.exit(Err(ErrorKind::InvalidInput
                    .cause(format!("Expected [] or [ObjectVersion] but got {:?}", old))
                    .into())),
            },
            Proposal::DeleteByPrefix(_, _, _, _, monitored) => {
                monitored.exit(Ok(DeleteObjectsByPrefixSummary {
                    total: old.len() as u64,
                }));
            }
        }
    }
    pub fn notify_rejected(self) {
        let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at().elapsed());
        self.metrics()
            .rejected_proposal_duration_seconds
            .observe(elapsed);
        self.metrics().rejected_proposal_total.increment();
        let e = ErrorKind::Other.cause("rejected");
        self.notify_error(e.into())
    }
    pub fn notify_error(self, e: Error) {
        let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at().elapsed());
        self.metrics()
            .failed_proposal_duration_seconds
            .observe(elapsed);
        self.metrics().failed_proposal_total.increment();
        match self {
            Proposal::Put(_, _, _, monitored) => {
                monitored.exit(Err(track!(e)));
            }
            Proposal::Delete(_, _, _, monitored) => {
                monitored.exit(Err(track!(e)));
            }
            Proposal::DeleteByPrefix(_, _, _, _, monitored) => {
                monitored.exit(Err(track!(e)));
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ProposalMetrics {
    committed_proposal_total: Counter,
    rejected_proposal_total: Counter,
    failed_proposal_total: Counter,
    committed_proposal_duration_seconds: Histogram,
    rejected_proposal_duration_seconds: Histogram,
    failed_proposal_duration_seconds: Histogram,
}
impl ProposalMetrics {
    pub fn new() -> Result<Self> {
        let mut builder = MetricBuilder::new();
        // namespace と subsystem の選択については以下のURLを参照
        // See https://github.com/frugalos/frugalos/pull/139#discussion_r272780913
        builder.namespace("frugalos").subsystem("mds");
        let committed_proposal_total = track!(builder
            .counter("committed_proposal_total")
            .default_registry()
            .finish())?;
        let rejected_proposal_total = track!(builder
            .counter("rejected_proposal_total")
            .default_registry()
            .finish())?;
        let failed_proposal_total = track!(builder
            .counter("failed_proposal_total")
            .default_registry()
            .finish())?;
        let committed_proposal_duration_seconds = track!(metrics::make_histogram(
            &mut builder.histogram("committed_proposal_duration_seconds")
        ))?;
        let rejected_proposal_duration_seconds = track!(metrics::make_histogram(
            &mut builder.histogram("rejected_proposal_duration_seconds")
        ))?;
        let failed_proposal_duration_seconds = track!(metrics::make_histogram(
            &mut builder.histogram("failed_proposal_duration_seconds")
        ))?;
        Ok(Self {
            committed_proposal_total,
            rejected_proposal_total,
            failed_proposal_total,
            committed_proposal_duration_seconds,
            rejected_proposal_duration_seconds,
            failed_proposal_duration_seconds,
        })
    }
}

/// `Node`に発行される要求.
#[derive(Debug)]
enum Request {
    StartElection,
    GetLeader(Instant, Reply<NodeId>),
    List(Reply<Vec<ObjectSummary>>),
    LatestVersion(Reply<Option<ObjectSummary>>),
    ObjectCount(Reply<u64>),
    Get(ObjectId, Expect, Instant, Reply<Option<Metadata>>),
    Head(ObjectId, Expect, Reply<Option<ObjectVersion>>),
    Put(
        ObjectId,
        Vec<u8>,
        Expect,
        Seconds,
        Instant,
        Reply<(ObjectVersion, Option<ObjectVersion>)>,
    ),
    Delete(ObjectId, Expect, Instant, Reply<Option<ObjectVersion>>),
    DeleteByVersion(ObjectVersion, Reply<Option<ObjectVersion>>),
    #[allow(dead_code)]
    DeleteByRange(ObjectVersion, ObjectVersion, Reply<Vec<ObjectSummary>>),
    DeleteByPrefix(ObjectPrefix, Reply<DeleteObjectsByPrefixSummary>),
    Stop,
    TakeSnapshot,
}
impl Request {
    pub fn failed(self, e: Error) {
        match self {
            Request::GetLeader(_, tx) => tx.exit(Err(track!(e))),
            Request::List(tx) => tx.exit(Err(track!(e))),
            Request::LatestVersion(tx) => tx.exit(Err(track!(e))),
            Request::ObjectCount(tx) => tx.exit(Err(track!(e))),
            Request::Get(_, _, _, tx) => tx.exit(Err(track!(e))),
            Request::Head(_, _, tx) => tx.exit(Err(track!(e))),
            Request::Put(_, _, _, _, _, tx) => tx.exit(Err(track!(e))),
            Request::Delete(_, _, _, tx) => tx.exit(Err(track!(e))),
            Request::DeleteByVersion(_, tx) => tx.exit(Err(track!(e))),
            Request::DeleteByRange(_, _, tx) => tx.exit(Err(track!(e))),
            Request::DeleteByPrefix(_, tx) => tx.exit(Err(track!(e))),
            Request::Stop | Request::TakeSnapshot | Request::StartElection => {}
        }
    }
}

/// MDSノードが発行するイベント.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum Event {
    /// メタデータオブジェクトが追加された.
    Putted {
        version: ObjectVersion,
        put_content_timeout: Seconds,
    },

    /// メタデータオブジェクトが削除された.
    Deleted { version: ObjectVersion },
}

#[cfg(test)]
mod tests {
    use super::*;
    use error::Error;
    use fibers::sync::oneshot;
    use fibers::sync::oneshot::{Monitor, Monitored};
    use fibers_global;
    use futures;
    use futures::Future;
    use raftlog::election::Term;
    use raftlog::log::LogIndex;
    use trackable::result::TestResult;

    fn make_monitor<T>() -> (Monitored<T, Error>, Monitor<T, Error>) {
        oneshot::monitor()
    }

    #[test]
    fn it_proposes_delete_by_prefix() -> TestResult {
        let (monitored, monitor) = make_monitor();
        let monitor = monitor.map_err(Error::from);
        let metrics = track!(ProposalMetrics::new())?;
        let proposal_id = ProposalId {
            term: Term::new(0),
            index: LogIndex::new(0),
        };

        fibers_global::spawn(futures::lazy(move || {
            let proposal = Proposal::DeleteByPrefix(
                proposal_id,
                Instant::now(),
                metrics,
                ObjectPrefix("abc".to_owned()),
                monitored,
            );
            proposal.notify_committed(&[ObjectVersion(1)]);
            Ok(())
        }));

        let summary = track!(fibers_global::execute(monitor))?;
        assert_eq!(summary.total, 1);
        Ok(())
    }
}
