use cannyls::deadline::Deadline;
use fibers::sync::mpsc;
use futures::{Async, Future, Poll};
use raftlog::log::{Log, LogSuffix};
use raftlog::{Error, ErrorKind};
use slog::Logger;
use std::mem;
use std::time::Instant;
use trackable::error::ErrorKindExt;

use super::log_prefix::{LoadLogPrefix, SaveLogPrefix};
use super::log_suffix::{LoadLogSuffix, SaveLogSuffix};
use super::{into_box_future, BoxFuture, Event, Handle, LocalNodeId, StorageMetrics};

/// Raft用のローカルログを保存するための`Future`実装.
// #[derive(Debug)]
pub struct SaveLog {
    pub(crate) inner: SaveLogInner,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl SaveLog {
    /// Creates a new `SaveLog` instance.
    pub(crate) fn new(inner: SaveLogInner, metrics: StorageMetrics) -> Self {
        Self {
            inner,
            started_at: Instant::now(),
            metrics,
        }
    }
}
impl Future for SaveLog {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(()) = track!(self.inner.poll())? {
            let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
            self.metrics.save_log_duration_seconds.observe(elapsed);
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

// #[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SaveLogInner {
    Suffix(SaveLogSuffix),
    Prefix(SaveLogPrefix),
    Failed(Error),
}
impl Future for SaveLogInner {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            SaveLogInner::Suffix(ref mut f) => track!(f.poll()),
            SaveLogInner::Prefix(ref mut f) => track!(f.poll()),
            SaveLogInner::Failed(ref mut e) => {
                let e = mem::replace(e, ErrorKind::Other.error().into());
                Err(track!(e))
            }
        }
    }
}

/// Raft用のローカルログを読み込むための`Future`実装.
// #[derive(Debug)]
pub struct LoadLog {
    pub(crate) inner: LoadLogInner,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl LoadLog {
    /// Creates a new `LoadLog` instance.
    pub(crate) fn new(inner: LoadLogInner, metrics: StorageMetrics) -> Self {
        Self {
            inner,
            started_at: Instant::now(),
            metrics,
        }
    }
}
impl Future for LoadLog {
    type Item = Log;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(item) = track!(self.inner.poll())? {
            let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
            self.metrics.load_log_duration_seconds.observe(elapsed);
            Ok(Async::Ready(item))
        } else {
            Ok(Async::NotReady)
        }
    }
}

// #[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum LoadLogInner {
    LoadLogPrefix {
        next: Option<LoadLogSuffix>,
        event_tx: Option<mpsc::Sender<Event>>,
        future: LoadLogPrefix,
    },
    LoadLogSuffix(LoadLogSuffix),
    CopyLogSuffix(LogSuffix),
    Failed(Error),
}
impl Future for LoadLogInner {
    type Item = Log;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let next = match *self {
                LoadLogInner::LoadLogSuffix(ref mut f) => {
                    return Ok(track!(f.poll())?.map(Log::Suffix));
                }
                LoadLogInner::CopyLogSuffix(ref mut f) => {
                    let suffix = mem::replace(f, Default::default());
                    return Ok(Async::Ready(Log::Suffix(suffix)));
                }
                LoadLogInner::LoadLogPrefix {
                    ref mut next,
                    ref mut future,
                    ref mut event_tx,
                } => {
                    match track!(future.poll())? {
                        Async::NotReady => return Ok(Async::NotReady),
                        Async::Ready(None) => {
                            // 接頭辞部分が未保存の場合には、代わりに末尾部分の読み込みを試す
                            let next =
                                track_assert_some!(next.take(), ErrorKind::InconsistentState);
                            LoadLogInner::LoadLogSuffix(next)
                        }
                        Async::Ready(Some(p)) => {
                            if let Some(tx) = event_tx.take() {
                                let _ = tx.send(Event::LogPrefixUpdated { new_head: p.tail });
                            }
                            return Ok(Async::Ready(Log::Prefix(p)));
                        }
                    }
                }
                LoadLogInner::Failed(ref mut e) => {
                    let e = mem::replace(e, ErrorKind::Other.error().into());
                    return Err(track!(e));
                }
            };
            *self = next;
        }
    }
}

/// Raft用のローカルログを削除するための`Future`実装.
pub struct DeleteLog {
    logger: Logger,
    event_tx: mpsc::Sender<Event>,
    future: BoxFuture<()>,
}
impl DeleteLog {
    /// Creates a new `DeleteLog` instance.
    pub(crate) fn new(handle: &Handle, event_tx: mpsc::Sender<Event>, node: LocalNodeId) -> Self {
        let logger = handle.logger.clone();
        let future = into_box_future(
            handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .delete_range(node.to_available_lump_id_range())
                .map(|_| ()),
        );
        info!(logger, "[START] DeleteLog");
        Self {
            logger,
            event_tx,
            future,
        }
    }
}
impl Future for DeleteLog {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(()) = track!(self.future.poll())? {
            info!(self.logger, "[FINISH] DeleteLog");
            let _ = self.event_tx.send(Event::LogSuffixDeleted);
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

#[cfg(test)]
mod tests {
    use cannyls::lump::LumpData;
    use raftlog::cluster::ClusterConfig;
    use raftlog::election::Term;
    use raftlog::log::{LogEntry, LogIndex, LogPosition, LogPrefix, LogSuffix};
    use std::collections::btree_set::BTreeSet;
    use trackable::result::TestResult;

    use test_util::{run_test_with_storage, wait_for};
    use LocalNodeId;

    #[test]
    fn delete_log_prefix_works() -> TestResult {
        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);
        run_test_with_storage(node_id, |(mut storage, device)| {
            let term = Term::new(3);
            let log_prefix = LogPrefix {
                tail: LogPosition {
                    prev_term: term,
                    index: LogIndex::new(0),
                },
                config: ClusterConfig::new(BTreeSet::new()),
                snapshot: vec![],
            };

            wait_for(storage.save_log_prefix(log_prefix))?;
            let lump_id = node_id.to_log_prefix_index_lump_id();
            let result = wait_for(device.handle().request().head(lump_id))?;
            assert!(result.is_some());

            wait_for(storage.delete_log())?;
            let lump_id = node_id.to_log_prefix_index_lump_id();
            let result = wait_for(device.handle().request().head(lump_id))?;
            assert!(result.is_none());

            Ok(())
        })
    }

    #[test]
    fn delete_log_works_without_suffix() -> TestResult {
        // 接尾部分がない状態で接尾部分の削除が失敗しないことを確認する。
        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);
        run_test_with_storage(node_id, |(mut storage, _device)| {
            wait_for(storage.delete_log())?;
            Ok(())
        })
    }

    #[test]
    fn delete_log_suffix_works_with_suffix() -> TestResult {
        // 接尾部分を一度保存した後削除して、接尾部分がすべて削除されていることを確認する。
        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);
        let next_node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 67]);
        run_test_with_storage(node_id, |(mut storage, device)| {
            let term = Term::new(0);
            let log_entries = vec![LogEntry::Noop { term }; 3];
            let log_suffix = LogSuffix {
                head: LogPosition {
                    prev_term: term,
                    index: LogIndex::new(0),
                },
                entries: log_entries.clone(),
            };
            let non_deleted_lump_id = next_node_id.to_log_entry_lump_id(LogIndex::new(0));
            let lump_data = track!(LumpData::new(vec![]))?;

            wait_for(storage.save_log_suffix(&log_suffix))?;

            for i in 0..log_entries.len() {
                let lump_id = node_id.to_log_entry_lump_id(LogIndex::new(i as u64));
                let result = wait_for(device.handle().request().head(lump_id))?;
                assert!(result.is_some());
            }

            let _ = wait_for(
                device
                    .handle()
                    .request()
                    .put(non_deleted_lump_id, lump_data),
            );
            wait_for(storage.delete_log())?;

            // バグがないかを確認するために少し先の範囲外まで含めて Lump が存在しないことを確認する
            for i in 0..(log_entries.len() * 2) {
                let lump_id = node_id.to_log_entry_lump_id(LogIndex::new(i as u64));
                let result = wait_for(device.handle().request().head(lump_id))?;
                assert!(result.is_none());
            }

            assert!(wait_for(device.handle().request().get(non_deleted_lump_id))
                .unwrap()
                .is_some());

            Ok(())
        })
    }
}
