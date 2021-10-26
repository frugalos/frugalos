use cannyls::deadline::Deadline;
use fibers::sync::mpsc;
use futures::{Async, Future, Poll};
use raftlog::log::{Log, LogIndex, LogSuffix};
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
                    let suffix = mem::take(f);
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
                .prioritized()
                .wait_for_running()
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

pub struct DeleteSuffixRange {
    future: BoxFuture<()>,
}

impl DeleteSuffixRange {
    pub(crate) fn new(handle: &Handle, node: LocalNodeId, from: LogIndex) -> Self {
        let future = into_box_future(
            handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .prioritized()
                .wait_for_running()
                .delete_range(node.lump_ids_corresponding_to_suffix_from(from))
                .map(|_| ()),
        );
        Self { future }
    }
}

impl Future for DeleteSuffixRange {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(()) = track!(self.future.poll())? {
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

    use crate::test_util::{run_test_with_storage, run_test_with_storages, wait_for};
    use crate::LocalNodeId;

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

    #[test]
    #[should_panic]
    fn do_my_test1() {
        // テスト内容: SHOULD PANIC
        // suffixの範囲外から削除する場合にはpanicになる。

        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);

        run_test_with_storage(node_id, |(mut storage, device)| {
            // 存在しない位置からの削除なのでpanic
            //
            // Design Question:
            // suffixのtailを超えた位置からの削除はエラーとするべきか？
            // 現在はエラーとし、Futureを生成する段階でPANICとしている。
            // ここで良いか？Futureは作らせて実行時にエラーとするべきか？
            wait_for(storage.delete_suffix_from(LogIndex::new(3)))?;
            Ok(())
        });
    }

    #[test]
    fn do_my_test2() -> TestResult {
        // テスト内容: 単一ノードでsuffixを作り範囲削除出来ていることを確認する。

        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);

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

            let is_there = |idx: u64| -> bool {
                let lump_id = node_id.to_log_entry_lump_id(LogIndex::new(idx));
                let result = wait_for(device.handle().request().head(lump_id)).unwrap();
                result.is_some()
            };

            assert!(!is_there(0));
            assert!(!is_there(1));
            assert!(!is_there(2));
            assert!(!is_there(3));

            wait_for(storage.save_log_suffix(&log_suffix))?;

            assert!(is_there(0));
            assert!(is_there(1));
            assert!(is_there(2));
            assert!(!is_there(3)); // <- 長さ3のsuffixなのでここは不在で良い

            wait_for(storage.delete_suffix_from(LogIndex::new(2)))?;

            assert!(is_there(0));
            assert!(is_there(1));
            assert!(!is_there(2)); // <- [2,∞)の削除をしたので消えているべき
            assert!(!is_there(3));

            wait_for(storage.delete_suffix_from(LogIndex::new(0)))?;

            assert!(!is_there(0)); // <- [0,∞)の削除をしたので全て消えているべき
            assert!(!is_there(1));
            assert!(!is_there(2));
            assert!(!is_there(3));

            Ok(())
        })
    }

    #[test]
    fn do_my_test3() -> TestResult {
        // テスト内容: 単一ノードでprefix と suffixを作り
        // suffixだけを範囲削除出来ていることを確認する。

        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);

        run_test_with_storage(node_id, |(mut storage, device)| {
            // <Prefixを書き込むパート>
            let log_prefix = LogPrefix {
                tail: LogPosition {
                    prev_term: Term::new(10), // Suffixの開始Termは10
                    index: LogIndex::new(1),  // Suffixはindex 1から開始
                },
                config: ClusterConfig::new(BTreeSet::new()),
                snapshot: vec![],
            };

            wait_for(storage.save_log_prefix(log_prefix))?;
            // </Prefixを書き込むパート>

            let check_prefix = || {
                let lump_id = node_id.to_log_prefix_index_lump_id();
                let result = wait_for(device.handle().request().head(lump_id)).unwrap();
                assert!(result.is_some());

                let prefix_0 = node_id.to_log_prefix_lump_id(0);
                let result = wait_for(device.handle().request().head(prefix_0)).unwrap();
                assert!(result.is_some());

                let prefix_1 = node_id.to_log_prefix_lump_id(1);
                let result = wait_for(device.handle().request().head(prefix_1)).unwrap();
                assert!(result.is_none()); // <- ここには書き込んでいないので存在しない
            };

            check_prefix();

            // <Suffixを書き込むパート>
            let term = Term::new(10);
            let log_entries = vec![LogEntry::Noop { term }; 3];
            let log_suffix = LogSuffix {
                head: LogPosition {
                    prev_term: term,
                    index: LogIndex::new(1),
                },
                entries: log_entries.clone(),
            };

            let is_there = |idx: u64| -> bool {
                let lump_id = node_id.to_log_entry_lump_id(LogIndex::new(idx));
                let result = wait_for(device.handle().request().head(lump_id)).unwrap();
                result.is_some()
            };

            assert!(!is_there(0));
            assert!(!is_there(1));
            assert!(!is_there(2));
            assert!(!is_there(3));
            assert!(!is_there(4));

            wait_for(storage.save_log_suffix(&log_suffix))?;
            // </Suffixを書き込むパート>

            assert!(!is_there(0)); // index 1からsuffixは開始している
            assert!(is_there(1));
            assert!(is_there(2));
            assert!(is_there(3));
            assert!(!is_there(4)); // index 1から長さ3なので4には不在

            let result = wait_for(device.handle().request().list())?;
            dbg!(&result);

            // Design Question
            // suffixのheadより手前からの削除を実行出来ても良いか？
            // エラーとするべきではないか？
            // tailを超えた位置からの削除はエラーとしている。
            wait_for(storage.delete_suffix_from(LogIndex::new(0)))?;

            assert!(!is_there(0)); // <- [0,∞)の削除をしたので全て消えているべき
            assert!(!is_there(1));
            assert!(!is_there(2));
            assert!(!is_there(3));
            assert!(!is_there(4));

            check_prefix();

            Ok(())
        })
    }

    #[test]
    fn do_my_test4() -> TestResult {
        // テスト内容: 複数ノードでsuffixを作り範囲削除出来ていることを確認する。

        let node0_id = LocalNodeId::new([0, 0, 0, 0, 0, 0, 1]);
        let node1_id = LocalNodeId::new([0, 0, 0, 0, 0, 0, 2]);

        let nodes = vec![node0_id, node1_id];

        run_test_with_storages(nodes, |(mut storages, device)| {
            let term = Term::new(0);
            let log_entries = vec![LogEntry::Noop { term }; 3];
            let log_suffix = LogSuffix {
                head: LogPosition {
                    prev_term: term,
                    index: LogIndex::new(0),
                },
                entries: log_entries.clone(),
            };

            let is_there = |idx: u64| -> bool {
                let lump_id = node0_id.to_log_entry_lump_id(LogIndex::new(idx));
                let result = wait_for(device.handle().request().head(lump_id)).unwrap();
                result.is_some()
            };

            assert!(!is_there(0));
            assert!(!is_there(1));
            assert!(!is_there(2));
            assert!(!is_there(3));

            wait_for(storages[0].save_log_suffix(&log_suffix))?;

            let result = wait_for(device.handle().request().list())?;
            dbg!(&result);

            assert!(is_there(0));
            assert!(is_there(1));
            assert!(is_there(2));
            assert!(!is_there(3));

            wait_for(storages[1].save_log_suffix(&log_suffix))?;

            let result = wait_for(device.handle().request().list())?;
            dbg!(&result);

            wait_for(storages[0].delete_suffix_from(LogIndex::new(0)))?;

            let result = wait_for(device.handle().request().list())?;
            dbg!(&result);

            Ok(())
        })
    }

    fn eq_log_suffix(left: &LogSuffix, right: &LogSuffix) -> bool {
        left.head == right.head && left.entries == right.entries
    }

    #[test]
    fn do_my_test5() -> TestResult {
        let node_id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);

        run_test_with_storage(node_id, |(mut storage, device)| {
            let log_entries = vec![
                LogEntry::Noop { term: Term::new(0) },
                LogEntry::Noop { term: Term::new(1) },
                LogEntry::Noop { term: Term::new(2) },
            ];
            let log_suffix = LogSuffix {
                head: LogPosition {
                    prev_term: Term::new(0),
                    index: LogIndex::new(0),
                },
                entries: log_entries.clone(),
            };

            let is_there = |idx: u64| -> bool {
                let lump_id = node_id.to_log_entry_lump_id(LogIndex::new(idx));
                let result = wait_for(device.handle().request().head(lump_id)).unwrap();
                result.is_some()
            };

            // まだ何も書き込んでいないのでデフォルト値をとっている
            assert!(eq_log_suffix(&storage.log_suffix(), &LogSuffix::default()));

            wait_for(storage.save_log_suffix(&log_suffix))?;

            // 書き込むとcacheも変更される
            assert!(eq_log_suffix(&storage.log_suffix(), &log_suffix));

            // 最終エントリだけ削除してみる
            wait_for(storage.delete_suffix_from(LogIndex::new(2)))?;
            assert!(eq_log_suffix(
                &storage.log_suffix(),
                &LogSuffix {
                    head: LogPosition {
                        prev_term: Term::new(0),
                        index: LogIndex::new(0),
                    },
                    entries: log_entries[0..=1].to_vec()
                }
            ));

            // 全削除する
            wait_for(storage.delete_suffix_from(LogIndex::new(0)))?;
            assert!(eq_log_suffix(&storage.log_suffix(), &LogSuffix::default()));

            Ok(())
        })
    }
}
