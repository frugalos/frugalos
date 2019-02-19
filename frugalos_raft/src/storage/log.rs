use fibers::sync::mpsc;
use futures::{Async, Future, Poll};
use raftlog::log::{Log, LogSuffix};
use raftlog::{Error, ErrorKind};
use std::mem;
use trackable::error::ErrorKindExt;

use super::log_prefix::{LoadLogPrefix, SaveLogPrefix};
use super::log_suffix::{LoadLogSuffix, SaveLogSuffix};
use super::Event;

/// Raft用のローカルログを保存するための`Future`実装.
// #[derive(Debug)]
pub struct SaveLog(pub(crate) SaveLogInner);
impl Future for SaveLog {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
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
pub struct LoadLog(pub(crate) LoadLogInner);
impl Future for LoadLog {
    type Item = Log;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
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
