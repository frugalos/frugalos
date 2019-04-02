use cannyls::deadline::Deadline;
use cannyls::lump::LumpData;
use fibers::sync::mpsc;
use futures::future::{Done, Either};
use futures::{self, Async, Future, Poll};
use raftlog::log::{LogEntry, LogIndex, LogSuffix};
use raftlog::Error;
use std::mem;
use std::time::Instant;

use super::{into_box_future, BoxFuture, Event, Handle, Storage};
use protobuf;
use StorageMetrics;

// #[derive(Debug)]
pub struct LoadLogSuffix {
    handle: Handle,
    current: LogIndex,
    suffix: LogSuffix,
    future: LoadLogEntry,
    event_tx: mpsc::Sender<Event>,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl LoadLogSuffix {
    pub fn new(storage: &Storage) -> Self {
        let head = storage.log_suffix.head;
        let handle = storage.handle.clone();
        info!(handle.logger, "[START] LoadLogSuffix: {}", dump!(head));

        let mut suffix = LogSuffix::default();
        suffix.head = head;
        LoadLogSuffix {
            handle: handle.clone(),
            suffix,
            current: head.index,
            future: LoadLogEntry::new(handle, head.index),
            event_tx: storage.event_tx.clone(),
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
}
impl Future for LoadLogSuffix {
    type Item = LogSuffix;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(entry) = track!(self.future.poll())? {
            if let Some(entry) = entry {
                self.suffix.entries.push(entry);
                self.current += 1;
                self.future = LoadLogEntry::new(self.handle.clone(), self.current)
            } else {
                info!(
                    self.handle.logger,
                    "[FINISH] LoadLogSuffix: {}",
                    dump!(self.current)
                );
                let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                self.metrics
                    .load_log_suffix_duration_seconds
                    .observe(elapsed);
                let suffix = mem::replace(&mut self.suffix, LogSuffix::default());
                let _ = self.event_tx.send(Event::LogSuffixLoaded(suffix.clone()));
                return Ok(Async::Ready(suffix));
            }
        }
        Ok(Async::NotReady)
    }
}

// #[derive(Debug)]
struct LoadLogEntry {
    handle: Handle,
    future: BoxFuture<Option<LumpData>>,
}
impl LoadLogEntry {
    pub fn new(handle: Handle, index: LogIndex) -> Self {
        let lump_id = handle.node_id.to_log_entry_lump_id(index);
        debug!(
            handle.logger,
            "[START] LoadLogEntry: {}",
            dump!(index, lump_id)
        );
        let future = into_box_future(
            handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .get(lump_id),
        );
        LoadLogEntry { handle, future }
    }
}
impl Future for LoadLogEntry {
    type Item = Option<LogEntry>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(match track!(self.future.poll())? {
            Async::Ready(Some(data)) => {
                let entry = track!(protobuf::decode_log_entry(data.as_bytes()))?;
                debug!(
                    self.handle.logger,
                    "[FINISH] LoadLogEntry: {}",
                    dump!(entry)
                );
                Async::Ready(Some(entry))
            }
            Async::Ready(None) => {
                debug!(self.handle.logger, "[FINISH] LoadLogEntry: None");
                Async::Ready(None)
            }
            Async::NotReady => Async::NotReady,
        })
    }
}

// #[derive(Debug)]
pub struct SaveLogSuffix {
    handle: Handle,
    next_index: LogIndex,
    entries: Vec<LogEntry>,
    future: Either<Done<bool, Error>, BoxFuture<bool>>,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl SaveLogSuffix {
    pub fn new(storage: &Storage, suffix: &LogSuffix) -> Self {
        let handle = storage.handle.clone();
        let future = Either::A(futures::done(Ok(true)));
        let mut entries = suffix.entries.clone();
        entries.reverse();
        debug!(
            handle.logger,
            "[START] SaveLogSuffix: {}",
            dump!(suffix.head, entries.len())
        );
        SaveLogSuffix {
            handle,
            next_index: suffix.head.index,
            entries,
            future,
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
    pub fn failed(storage: &Storage, e: Error) -> Self {
        SaveLogSuffix {
            handle: storage.handle.clone(),
            next_index: LogIndex::new(0),
            entries: Vec::new(),
            future: Either::A(futures::done(Err(e))),
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
}
impl Future for SaveLogSuffix {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(_) = track!(self.future.poll())? {
            if let Some(e) = self.entries.pop() {
                let index = self.next_index;
                let lump_id = self.handle.node_id.to_log_entry_lump_id(index);
                self.next_index += 1;
                debug!(
                    self.handle.logger,
                    "[PROGRESS] SaveLogSuffix: {}",
                    dump!(index, lump_id, e)
                );
                let bytes = track!(protobuf::encode_log_entry(e))?;
                let data = LumpData::new_embedded(bytes).expect("Never fails");
                let future = self
                    .handle
                    .device
                    .request()
                    .deadline(Deadline::Immediate)
                    .put(lump_id, data);
                self.future = Either::B(into_box_future(future));
            } else {
                debug!(self.handle.logger, "[FINISH] SaveLogSuffix");
                let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                self.metrics
                    .save_log_suffix_duration_seconds
                    .observe(elapsed);
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}
