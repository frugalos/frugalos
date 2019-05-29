use cannyls;
use cannyls::device::DeviceHandle;
use fibers::sync::mpsc;
use futures::{Async, Future, Stream};
use prometrics::metrics::{Histogram, HistogramBuilder, MetricBuilder};
use raftlog::election::Ballot;
use raftlog::log::{LogIndex, LogPosition, LogPrefix, LogSuffix};
use raftlog::{Error, ErrorKind, Result};
use slog::Logger;
use std::sync::atomic::{self, AtomicUsize};
use trackable::error::ErrorKindExt;

use LocalNodeId;

pub use self::ballot::{LoadBallot, SaveBallot};
pub use self::log::{LoadLog, SaveLog};
pub use self::log_prefix::{LoadLogPrefix, SaveLogPrefix};
pub use self::log_suffix::{LoadLogSuffix, SaveLogSuffix};

mod ballot;
mod log;
mod log_prefix;
mod log_suffix;

// ストレージの初期化処理を直列化するためのグローバル変数.
//
// 初期化時には、スナップ処理や大きなAppendEntriesの処理が入り重いので、
// 並列度を下げるために、これを利用する.
//
// 最終的にはもう少し上手い仕組みを考えたい.
// (個々のRaftノードに独立した仕組みにできるのとベスト)
static INITIALIZATION_LOCK: AtomicUsize = AtomicUsize::new(0);

fn acquire_initialization_lock() -> bool {
    INITIALIZATION_LOCK.compare_and_swap(0, 1, atomic::Ordering::SeqCst) == 0
}

fn release_initialization_lock() {
    INITIALIZATION_LOCK.fetch_sub(1, atomic::Ordering::SeqCst);
}

/// Raft用の永続ストレージ実装.
#[derive(Debug)]
pub struct Storage {
    handle: Handle,

    // スナップショット以降のログ領域を保持するバッファ.
    //
    // これは、読み込み速度向上用に用意されているものであり、
    // Raftノードのロード時を除き、末尾部分のログエントリの読み込みは、
    // 常にこのバッファ上から行われることになる.
    //
    // 反対に書き込みに関しては、常に即座に永続ストレージに即座に
    // エントリが保存される.
    // (同時にバッファにも追記が行われるが、エントリがバッファにしか存在しない期間、
    // というものは発生しない)
    log_suffix: LogSuffix,

    event_rx: mpsc::Receiver<Event>,
    event_tx: mpsc::Sender<Event>,
    phase: Phase,
    metrics: StorageMetrics,
}
impl Storage {
    /// 新しい`Storage`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        node_id: LocalNodeId,
        device: DeviceHandle,
        metrics: StorageMetrics,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel();
        Storage {
            handle: Handle {
                logger,
                node_id,
                device,
            },
            log_suffix: LogSuffix::default(),
            event_rx,
            event_tx,
            phase: Phase::Started,
            metrics,
        }
    }

    pub(crate) fn logger(&self) -> Logger {
        self.handle.logger.clone()
    }
    pub(crate) fn node_id(&self) -> LocalNodeId {
        self.handle.node_id
    }
    #[cfg(test)]
    pub(crate) fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub(crate) fn save_ballot(&mut self, ballot: Ballot) -> ballot::SaveBallot {
        ballot::SaveBallot::new(self, ballot)
    }
    pub(crate) fn load_ballot(&mut self) -> ballot::LoadBallot {
        ballot::LoadBallot::new(self)
    }
    pub(crate) fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> LoadLog {
        if let Err(e) = track!(self.poll_and_handle_event()) {
            return LoadLog::new(log::LoadLogInner::Failed(e), self.metrics.clone());
        }

        // XXX: 全体的に`raftlog`の実装内容に依存しており、あまり良くはない
        let future = if let Some(end) = end {
            // 明示的に終端が指定されている == 初回ロード(ノード起動)時以降のログ読み込み
            if start < self.log_suffix.head.index {
                // バッファ地点以前のエントリが必要 => 存在しないのでスナップショットを返す
                let future = log_prefix::LoadLogPrefix::new(self);
                log::LoadLogInner::LoadLogPrefix {
                    next: None,
                    event_tx: None,
                    future,
                }
            } else {
                // バッファ内から取得
                let copy_from_buffer = || {
                    track_assert!(
                        start <= self.log_suffix.tail().index,
                        ErrorKind::InvalidInput
                    );
                    track_assert!(end <= self.log_suffix.tail().index, ErrorKind::InvalidInput);
                    track!(self.log_suffix.slice(start, end))
                };
                match copy_from_buffer() {
                    Err(e) => log::LoadLogInner::Failed(e),
                    Ok(suffix) => log::LoadLogInner::CopyLogSuffix(suffix),
                }
            }
        } else if start.as_u64() == 0 {
            // 「終端が未指定」かつ「開始地点が0」は、ノード起動時の最初のログ読み込みを示している
            // => まずスナップショットのロードを試みる
            let future = log_prefix::LoadLogPrefix::new(self);
            let next = Some(log_suffix::LoadLogSuffix::new(self));
            log::LoadLogInner::LoadLogPrefix {
                next,
                event_tx: Some(self.event_tx.clone()),
                future,
            }
        } else {
            // 「終端が未指定」かつ「開始地点が0以外」は、
            // ノード起動時かつスナップショットロード以降のログ読み込みを示している
            // => スナップショット以降のログエントリ群を取得する
            assert_eq!(start, self.log_suffix.head.index);
            log::LoadLogInner::LoadLogSuffix(log_suffix::LoadLogSuffix::new(self))
        };
        LoadLog::new(future, self.metrics.clone())
    }
    pub(crate) fn save_log_suffix(&mut self, suffix: &LogSuffix) -> SaveLog {
        if self.phase != Phase::Initialized {
            // ログ書き込みが発生する、ということは初期化フェーズは抜けたことを意味する
            info!(self.handle.logger, "Initialized");
            if self.phase == Phase::Initializing {
                release_initialization_lock();
                info!(self.handle.logger, "Initialization lock is released");
            }
            self.phase = Phase::Initialized;
        }

        if let Err(e) = track!(self.poll_and_handle_event()) {
            return SaveLog::new(log::SaveLogInner::Failed(e), self.metrics.clone());
        }

        // ローカルバッファに追記後に、永続化ストレージに保存する.
        //
        // `raftlog`から、このメソッドが返した`Future`が完了して初めて、
        // エントリ群の追記が完了したものとして認識されるので、
        // 先にバッファに追加してしまっても問題は発生しない.
        let future = if let Err(e) = track!(self.append_to_local_buffer(suffix)) {
            log_suffix::SaveLogSuffix::failed(self, e)
        } else {
            log_suffix::SaveLogSuffix::new(self, suffix)
        };
        SaveLog::new(log::SaveLogInner::Suffix(future), self.metrics.clone())
    }
    pub(crate) fn save_log_prefix(&mut self, prefix: LogPrefix) -> SaveLog {
        if self.phase != Phase::Initialized {
            // ログ書き込みが発生する、ということは初期化フェーズは抜けたことを意味する
            info!(self.handle.logger, "Initialized");
            if self.phase == Phase::Initializing {
                release_initialization_lock();
                info!(self.handle.logger, "Initialization lock is released");
            }
            self.phase = Phase::Initialized;
        }

        let inner = if let Err(e) = track!(self.poll_and_handle_event()) {
            log::SaveLogInner::Failed(e)
        } else {
            log::SaveLogInner::Prefix(log_prefix::SaveLogPrefix::new(self, prefix))
        };
        SaveLog::new(inner, self.metrics.clone())
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn is_busy(&mut self) -> bool {
        if self.phase == Phase::Started {
            if acquire_initialization_lock() {
                info!(self.handle.logger, "Initialization lock is acquired");
                self.phase = Phase::Initializing;
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    fn poll_and_handle_event(&mut self) -> Result<()> {
        while let Async::Ready(event) = self.event_rx.poll().expect("Never fails") {
            let event = event.expect("Never fails");
            match event {
                Event::LogPrefixUpdated { new_head } => {
                    track!(self.handle_log_prefix_updated_event(new_head))?;
                }
                Event::LogSuffixLoaded(suffix) => {
                    track!(self.handle_log_suffix_loaded_event(suffix))?;
                }
            }
        }
        Ok(())
    }
    fn handle_log_prefix_updated_event(&mut self, new_head: LogPosition) -> Result<()> {
        // ログの前半部分が更新されたので、それに合わせてバッファを調整する
        info!(
            self.handle.logger,
            "Event::LogPrefixUpdated: {}",
            dump!(self.log_suffix.head, new_head)
        );
        if self.log_suffix.head.index < new_head.index {
            if self.log_suffix.skip_to(new_head.index).is_err() {
                // バッファがカバーする範囲(i.e., ローカルログの範囲)よりも
                // 先の地点のスナップショットがインストールされた
                // => バッファを空にし、先頭地点を設定し直す
                self.log_suffix.head = new_head;
                self.log_suffix.entries.clear();
            }
            track_assert_eq!(
                new_head.index,
                self.log_suffix.head.index,
                ErrorKind::InconsistentState
            );
            if new_head.prev_term != self.log_suffix.head.prev_term {
                self.log_suffix.head.prev_term = new_head.prev_term;
                self.log_suffix.entries.clear();
            }
        }
        Ok(())
    }
    fn handle_log_suffix_loaded_event(&mut self, suffix: LogSuffix) -> Result<()> {
        // ログの接尾部分がストレージから読み込まれたので、バッファに反映する
        info!(
            self.handle.logger,
            "Event::LogSuffixLoaded: {}",
            dump!(suffix.head, suffix.entries.len())
        );
        self.log_suffix = suffix;
        Ok(())
    }
    fn append_to_local_buffer(&mut self, suffix: &LogSuffix) -> Result<()> {
        // ローカルログと`suffix`の領域に重複部分があるかをチェック
        // (未コミット分がロールバックされる可能性もあるので、
        // 必ずしも`suffix`の先端が、ローカルログの末端と一致する必要はない)
        let entries_offset = if self.log_suffix.head.index <= suffix.head.index {
            0
        } else {
            // スナップショットのインストールタイミング次第で、こちらに入ることがある
            self.log_suffix.head.index - suffix.head.index
        };
        track_assert!(
            suffix.head.index <= self.log_suffix.tail().index,
            ErrorKind::InconsistentState,
            "suffix.start={:?}, self.end={:?}",
            suffix.head.index,
            self.log_suffix.tail().index
        );

        // 整合性(prev_termの一致)チェック
        let offset = suffix.head.index + entries_offset - self.log_suffix.head.index;
        let prev_term = if offset == 0 {
            self.log_suffix.head.prev_term
        } else {
            self.log_suffix.entries[offset - 1].term()
        };
        track_assert_eq!(
            suffix.positions().nth(entries_offset).map(|p| p.prev_term),
            Some(prev_term),
            ErrorKind::InconsistentState,
            "suffix.start={:?}, self.start={:?}",
            suffix.positions().nth(entries_offset),
            self.log_suffix.head
        );

        // 末尾の余剰領域を削除(ロールバック)した上で、追記する
        self.log_suffix.entries.truncate(offset);
        self.log_suffix
            .entries
            .extend(suffix.entries.iter().skip(entries_offset).cloned());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Handle {
    pub logger: Logger,
    pub node_id: LocalNodeId,
    pub device: DeviceHandle,
}

#[derive(Debug)]
pub(crate) enum Event {
    LogPrefixUpdated { new_head: LogPosition },
    LogSuffixLoaded(LogSuffix),
}

type BoxFuture<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

fn into_box_future<F>(future: F) -> BoxFuture<F::Item>
where
    F: Future<Error = cannyls::Error> + Send + 'static,
{
    let future = future.map_err(|e| match *e.kind() {
        cannyls::ErrorKind::DeviceBusy => ErrorKind::Busy.takes_over(e).into(),
        cannyls::ErrorKind::InvalidInput => ErrorKind::InvalidInput.takes_over(e).into(),
        cannyls::ErrorKind::Other
        | cannyls::ErrorKind::InconsistentState
        | cannyls::ErrorKind::DeviceTerminated
        | cannyls::ErrorKind::StorageCorrupted
        | cannyls::ErrorKind::StorageFull => ErrorKind::Other.takes_over(e).into(),
    });
    Box::new(future)
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Started,
    Initializing,
    Initialized,
}

/// Metrics for `storage`.
#[derive(Debug, Clone)]
pub struct StorageMetrics {
    pub(crate) load_log_duration_seconds: Histogram,
    pub(crate) save_log_duration_seconds: Histogram,
    pub(crate) load_log_prefix_duration_seconds: Histogram,
    pub(crate) save_log_prefix_duration_seconds: Histogram,
    pub(crate) load_log_suffix_duration_seconds: Histogram,
    pub(crate) save_log_suffix_duration_seconds: Histogram,
    pub(crate) load_ballot_duration_seconds: Histogram,
    pub(crate) save_ballot_duration_seconds: Histogram,
}
impl StorageMetrics {
    /// Makes a new `StorageMetrics` instance.
    pub fn new() -> Self {
        let mut builder = MetricBuilder::new();
        builder.namespace("frugalos_raft").subsystem("storage");
        let load_log_duration_seconds = make_histogram(
            builder
                .histogram("load_log_duration_seconds")
                .help("Log loading duration"),
        );
        let save_log_duration_seconds = make_histogram(
            builder
                .histogram("save_log_duration_seconds")
                .help("Log saving duration"),
        );
        let load_log_prefix_duration_seconds = make_histogram(
            builder
                .histogram("load_log_prefix_duration_seconds")
                .help("LogPrefix loading duration"),
        );
        let save_log_prefix_duration_seconds = make_histogram(
            builder
                .histogram("save_log_prefix_duration_seconds")
                .help("LogPrefix saving duration"),
        );
        let load_log_suffix_duration_seconds = make_histogram(
            builder
                .histogram("load_log_suffix_duration_seconds")
                .help("LogPrefix loading duration"),
        );
        let save_log_suffix_duration_seconds = make_histogram(
            builder
                .histogram("save_log_suffix_duration_seconds")
                .help("LogPrefix saving duration"),
        );
        let load_ballot_duration_seconds = make_histogram(
            builder
                .histogram("load_ballot_duration_seconds")
                .help("Ballot loading duration"),
        );
        let save_ballot_duration_seconds = make_histogram(
            builder
                .histogram("save_ballot_duration_seconds")
                .help("Ballot saving duration"),
        );
        Self {
            load_log_duration_seconds,
            save_log_duration_seconds,
            load_log_prefix_duration_seconds,
            save_log_prefix_duration_seconds,
            load_log_suffix_duration_seconds,
            save_log_suffix_duration_seconds,
            load_ballot_duration_seconds,
            save_ballot_duration_seconds,
        }
    }
}
impl Default for StorageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn make_histogram(builder: &mut HistogramBuilder) -> Histogram {
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
        .bucket(50.0)
        .finish()
        .expect("Never fails")
}
