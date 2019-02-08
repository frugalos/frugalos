#![allow(clippy::module_inception)]
use fibers::sync::oneshot::Monitored;
use frugalos_raft::NodeId;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, Metadata, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use raftlog::log::ProposalId;
use trackable::error::ErrorKindExt;

use {Error, ErrorKind};

pub use self::handle::NodeHandle;
pub use self::node::Node;

mod handle;
mod node;

type Reply<T> = Monitored<T, Error>;

/// Raftに提案中のコマンド.
#[derive(Debug)]
enum Proposal {
    Put(ProposalId, Reply<(ObjectVersion, Option<ObjectVersion>)>),
    Delete(ProposalId, Reply<Option<ObjectVersion>>),
    DeleteByPrefix(
        ProposalId,
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
    pub fn notify_committed(self, old: &[ObjectVersion]) {
        match self {
            Proposal::Put(id, monitored) => match old {
                [] => monitored.exit(Ok((ObjectVersion(id.index.as_u64()), None))),
                [old] => monitored.exit(Ok((ObjectVersion(id.index.as_u64()), Some(*old)))),
                _ => monitored.exit(Err(ErrorKind::InvalidInput
                    .cause(format!("Expected [] or [ObjectVersion] but got {:?}", old))
                    .into())),
            },
            Proposal::Delete(_, monitored) => match old {
                [] => monitored.exit(Ok(None)),
                [old] => monitored.exit(Ok(Some(*old))),
                _ => monitored.exit(Err(ErrorKind::InvalidInput
                    .cause(format!("Expected [] or [ObjectVersion] but got {:?}", old))
                    .into())),
            },
            Proposal::DeleteByPrefix(_, _, monitored) => {
                monitored.exit(Ok(DeleteObjectsByPrefixSummary {
                    total: old.len() as u64,
                }));
            }
        }
    }
    pub fn notify_rejected(self) {
        let e = ErrorKind::Other.cause("rejected");
        self.notify_error(e.into())
    }
    pub fn notify_error(self, e: Error) {
        match self {
            Proposal::Put(_, monitored) => {
                monitored.exit(Err(track!(e)));
            }
            Proposal::Delete(_, monitored) => {
                monitored.exit(Err(track!(e)));
            }
            Proposal::DeleteByPrefix(_, _, monitored) => {
                monitored.exit(Err(track!(e)));
            }
        }
    }
}

/// `Node`に発行される要求.
#[derive(Debug)]
enum Request {
    StartElection,
    GetLeader(Reply<NodeId>),
    List(Reply<Vec<ObjectSummary>>),
    LatestVersion(Reply<Option<ObjectSummary>>),
    ObjectCount(Reply<u64>),
    Get(ObjectId, Expect, Reply<Option<Metadata>>),
    Head(ObjectId, Expect, Reply<Option<ObjectVersion>>),
    MdsHead(ObjectId, Expect, Reply<Option<ObjectVersion>>),
    Put(
        ObjectId,
        Vec<u8>,
        Expect,
        Seconds,
        Reply<(ObjectVersion, Option<ObjectVersion>)>,
    ),
    Delete(ObjectId, Expect, Reply<Option<ObjectVersion>>),
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
            Request::GetLeader(tx) => tx.exit(Err(track!(e))),
            Request::List(tx) => tx.exit(Err(track!(e))),
            Request::LatestVersion(tx) => tx.exit(Err(track!(e))),
            Request::ObjectCount(tx) => tx.exit(Err(track!(e))),
            Request::Get(_, _, tx) => tx.exit(Err(track!(e))),
            Request::Head(_, _, tx) => tx.exit(Err(track!(e))),
            Request::MdsHead(_, _, tx) => tx.exit(Err(track!(e))),
            Request::Put(_, _, _, _, tx) => tx.exit(Err(track!(e))),
            Request::Delete(_, _, tx) => tx.exit(Err(track!(e))),
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
        let proposal_id = ProposalId {
            term: Term::new(0),
            index: LogIndex::new(0),
        };

        fibers_global::spawn(futures::lazy(move || {
            let proposal =
                Proposal::DeleteByPrefix(proposal_id, ObjectPrefix("abc".to_owned()), monitored);
            Ok(proposal.notify_committed(&[ObjectVersion(1)]))
        }));

        let summary = track!(fibers_global::execute(monitor))?;
        Ok(assert_eq!(summary.total, 1))
    }
}
