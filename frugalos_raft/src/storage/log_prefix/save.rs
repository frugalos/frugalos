use cannyls::deadline::Deadline;
use cannyls::lump::LumpData;
use fibers::sync::mpsc;
use futures::{Async, Future, Poll};
use raftlog::log::{LogIndex, LogPosition, LogPrefix};
use raftlog::{Error, Result};
use std::cmp;
use std::env;
use std::ops::Range;
use std::time::Instant;

use super::super::{into_box_future, BoxFuture, Event, Handle, Storage};
use super::delete::{DeleteOldLogEntries, DeleteOldLogPrefixBytes};
use super::load::LoadLogPrefixIndex;
use protobuf;
use util::Phase5;
use StorageMetrics;

fn max_lump_data_size() -> usize {
    env::var("RAFT_IO_MAX_LUMP_DATA_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(LumpData::MAX_SIZE)
}

// #[derive(Debug)]
pub struct SaveLogPrefix {
    handle: Handle,
    phase: Phase5<
        LoadLogPrefixIndex,
        SaveLogPrefixBytes,
        SaveLogPrefixIndex,
        DeleteOldLogPrefixBytes,
        DeleteOldLogEntries,
    >,
    prefix: Option<LogPrefix>,
    old_prefix_index: Range<u64>,
    old_entries: Range<LogIndex>,
    new_head: LogPosition,
    event_tx: mpsc::Sender<Event>,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl SaveLogPrefix {
    pub(crate) fn new(storage: &mut Storage, prefix: LogPrefix) -> Self {
        let handle = storage.handle.clone();
        let old_entries = Range {
            start: storage.log_suffix.head.index,
            end: prefix.tail.index,
        };
        info!(
            handle.logger,
            "[START] SaveLogPrefix: {}",
            dump!(prefix.tail, prefix.config, prefix.snapshot.len())
        );
        let phase = Phase5::A(LoadLogPrefixIndex::new(handle.clone()));
        SaveLogPrefix {
            handle,
            phase,
            new_head: prefix.tail,
            prefix: Some(prefix),
            old_prefix_index: Range { start: 0, end: 0 },
            old_entries,
            event_tx: storage.event_tx.clone(),
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
}
impl Future for SaveLogPrefix {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = self.phase.poll()? {
            let next = match phase {
                Phase5::A(index) => {
                    let index = index.unwrap_or(Range { start: 0, end: 0 });
                    self.old_prefix_index = index.clone();
                    let prefix = self.prefix.take().expect("Never fails");
                    let future =
                        track!(SaveLogPrefixBytes::new(self.handle.clone(), index, prefix))?;
                    Phase5::B(future)
                }
                Phase5::B(prefix_index) => {
                    let future =
                        track!(SaveLogPrefixIndex::new(self.handle.clone(), prefix_index))?;
                    Phase5::C(future)
                }
                Phase5::C(()) => {
                    let future = DeleteOldLogPrefixBytes::new(
                        self.handle.clone(),
                        self.old_prefix_index.clone(),
                    );
                    Phase5::D(future)
                }
                Phase5::D(()) => {
                    let future = track!(DeleteOldLogEntries::new(
                        self.handle.clone(),
                        self.old_entries.clone()
                    ))?;
                    Phase5::E(future)
                }
                Phase5::E(()) => {
                    info!(self.handle.logger, "[FINISH] SaveLogPrefix");
                    let event = Event::LogPrefixUpdated {
                        new_head: self.new_head,
                    };
                    let _ = self.event_tx.send(event);
                    let elapsed =
                        prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                    self.metrics
                        .save_log_prefix_duration_seconds
                        .observe(elapsed);
                    return Ok(Async::Ready(()));
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

// #[derive(Debug)]
struct SaveLogPrefixBytes {
    handle: Handle,
    current_index: u64,
    prefix_index: Range<u64>,
    prefix_bytes: Vec<u8>,
    future: Option<BoxFuture<bool>>,
}
impl SaveLogPrefixBytes {
    pub fn new(handle: Handle, old_index: Range<u64>, prefix: LogPrefix) -> Result<Self> {
        let prefix_bytes = track!(protobuf::encode_log_prefix(prefix))?;
        let lump_count = (prefix_bytes.len() + max_lump_data_size() - 1) / max_lump_data_size();
        let prefix_index = Range {
            start: old_index.end,
            end: old_index.end + lump_count as u64,
        };
        info!(
            handle.logger,
            "[START] SaveLogPrefixBytes: {}",
            dump!(prefix_index, prefix_bytes.len())
        );
        Ok(SaveLogPrefixBytes {
            handle,
            current_index: prefix_index.start,
            prefix_index,
            prefix_bytes,
            future: None,
        })
    }
}
impl Future for SaveLogPrefixBytes {
    type Item = Range<u64>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(_) = track!(self.future.poll())? {
            if self.current_index == self.prefix_index.end {
                info!(self.handle.logger, "[FINISH] SaveLogPrefixBytes");
                return Ok(Async::Ready(self.prefix_index.clone()));
            }

            let end = cmp::min(self.prefix_bytes.len(), max_lump_data_size());
            let bytes = self.prefix_bytes.drain(0..end).collect::<Vec<_>>();

            let index = self.current_index;
            let lump_id = self.handle.node_id.to_log_prefix_lump_id(index);
            info!(
                self.handle.logger,
                "[PROGRESS] SaveLogPrefixBytes: {}",
                dump!(index, lump_id, bytes.len())
            );

            let data = self
                .handle
                .device
                .allocate_lump_data_with_bytes(&bytes)
                .expect("Never fails");
            let future = self
                .handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .put(lump_id, data);
            self.current_index += 1;
            self.future = Some(into_box_future(future));
        }
        Ok(Async::NotReady)
    }
}

// #[derive(Debug)]
struct SaveLogPrefixIndex {
    handle: Handle,
    future: BoxFuture<bool>,
}
impl SaveLogPrefixIndex {
    pub fn new(handle: Handle, index: Range<u64>) -> Result<Self> {
        let lump_id = handle.node_id.to_log_prefix_index_lump_id();
        let bytes = track!(protobuf::encode_log_prefix_index(index.clone()))?;
        info!(
            handle.logger,
            "[START] SaveLogPrefixIndex: {}",
            dump!(index, bytes.len(), lump_id)
        );
        let data = LumpData::new_embedded(bytes).expect("Never fails");
        let future = into_box_future(
            handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .put(lump_id, data),
        );
        Ok(SaveLogPrefixIndex { handle, future })
    }
}
impl Future for SaveLogPrefixIndex {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let polled = track!(self.future.poll())?;
        if polled.is_ready() {
            info!(self.handle.logger, "[FINISH] SaveLogPrefixIndex");
        }
        Ok(polled.map(|_| ()))
    }
}
