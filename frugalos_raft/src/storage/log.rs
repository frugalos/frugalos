use fibers::sync::mpsc;
use futures::{Async, Future, Poll};
use raftlog::log::{Log, LogSuffix};
use raftlog::{Error, ErrorKind};
use std::mem;
use std::time::Instant;
use trackable::error::ErrorKindExt;

use super::log_prefix::{LoadLogPrefix, SaveLogPrefix};
use super::log_suffix::{LoadLogSuffix, SaveLogSuffix};
use super::Event;
use super::StorageMetrics;

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
        if let Ok(Async::Ready(())) = self.inner.poll() {
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
        if let Ok(Async::Ready(item)) = self.inner.poll() {
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
