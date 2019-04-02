use cannyls::deadline::Deadline;
use cannyls::lump::LumpData;
use futures::{Async, Future, Poll};
use raftlog::log::LogPrefix;
use raftlog::Error;
use std::mem;
use std::ops::Range;
use std::time::Instant;

use super::super::{into_box_future, BoxFuture, Handle, Storage};
use protobuf;
use util::Phase;
use StorageMetrics;

// #[derive(Debug)]
pub struct LoadLogPrefix {
    handle: Handle,
    phase: Phase<LoadLogPrefixIndex, LoadLogPrefixBytes>,
    prefix_index: Range<u64>,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl LoadLogPrefix {
    pub fn new(storage: &Storage) -> Self {
        let handle = storage.handle.clone();
        let phase = Phase::A(LoadLogPrefixIndex::new(handle.clone()));
        info!(handle.logger, "[START] LoadLogPrefix");
        LoadLogPrefix {
            handle,
            phase,
            prefix_index: Range { start: 0, end: 0 },
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
}
impl Future for LoadLogPrefix {
    type Item = Option<LogPrefix>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = self.phase.poll()? {
            let next = match phase {
                Phase::A(None) => {
                    info!(
                        self.handle.logger,
                        "[FINISH] LoadLogPrefix: Index Not Found"
                    );
                    let elapsed =
                        prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                    self.metrics
                        .load_log_prefix_duration_seconds
                        .observe(elapsed);
                    return Ok(Async::Ready(None));
                }
                Phase::A(Some(index)) => {
                    if index == self.prefix_index {
                        // リトライ前後でインデックスが変わらない場合には、
                        // 保存処理と競合した訳ではなく、そもそもデータが存在しないことを意味する.
                        info!(self.handle.logger, "[FINISH] LoadLogPrefix: Data Not Found");
                        let elapsed =
                            prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                        self.metrics
                            .load_log_prefix_duration_seconds
                            .observe(elapsed);
                        return Ok(Async::Ready(None));
                    } else {
                        self.prefix_index = index.clone();
                        Phase::B(LoadLogPrefixBytes::new(self.handle.clone(), index))
                    }
                }
                Phase::B(bytes) => {
                    if let Some(bytes) = bytes {
                        let prefix = track!(protobuf::decode_log_prefix(&bytes))?;
                        info!(
                            self.handle.logger,
                            "[FINISH] LoadLogPrefix: {}",
                            dump!(prefix.tail, prefix.config, bytes.len())
                        );
                        let elapsed =
                            prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                        self.metrics
                            .load_log_prefix_duration_seconds
                            .observe(elapsed);
                        return Ok(Async::Ready(Some(prefix)));
                    } else {
                        // 対応するlumpが見つからなかった.
                        // => ロード中に新しい`LogPrefix`がインストールされた可能性が高いので、
                        //    リトライを行う.
                        info!(self.handle.logger, "[RETRY] LoadLogPrefix");
                        Phase::A(LoadLogPrefixIndex::new(self.handle.clone()))
                    }
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

// #[derive(Debug)]
pub(crate) struct LoadLogPrefixIndex {
    handle: Handle,
    future: BoxFuture<Option<LumpData>>,
}
impl LoadLogPrefixIndex {
    pub(crate) fn new(handle: Handle) -> Self {
        let lump_id = handle.node_id.to_log_prefix_index_lump_id();
        info!(
            handle.logger,
            "[START] LoadLogPrefixIndex: {}",
            dump!(lump_id)
        );
        let future = into_box_future(
            handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .get(lump_id),
        );
        LoadLogPrefixIndex { handle, future }
    }
}
impl Future for LoadLogPrefixIndex {
    type Item = Option<Range<u64>>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(data) = track!(self.future.poll())? {
            let index = if let Some(data) = data {
                let index = track!(protobuf::decode_log_prefix_index(data.as_bytes()))?;
                Some(index)
            } else {
                None
            };
            info!(
                self.handle.logger,
                "[FINISH] LoadLogPrefixIndex: {}",
                dump!(index)
            );
            Ok(Async::Ready(index))
        } else {
            Ok(Async::NotReady)
        }
    }
}

// #[derive(Debug)]
struct LoadLogPrefixBytes {
    handle: Handle,
    prefix_index: Range<u64>,
    bytes: Vec<u8>,
    future: Option<BoxFuture<Option<LumpData>>>,
}
impl LoadLogPrefixBytes {
    pub fn new(handle: Handle, prefix_index: Range<u64>) -> Self {
        assert!(prefix_index.start <= prefix_index.end);
        info!(
            handle.logger,
            "[START] LoadLogPrefixBytes: {}",
            dump!(prefix_index)
        );
        LoadLogPrefixBytes {
            handle,
            prefix_index,
            bytes: Vec::new(),
            future: None,
        }
    }
}
impl Future for LoadLogPrefixBytes {
    type Item = Option<Vec<u8>>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(data) = track!(self.future.poll())? {
            match data {
                Some(Some(data)) => {
                    self.bytes.extend_from_slice(data.as_bytes());
                    self.prefix_index.start += 1;
                    self.future = None;
                }
                Some(None) => {
                    info!(self.handle.logger, "[FINISH] LoadLogPrefixBytes: None");
                    return Ok(Async::Ready(None));
                }
                None if self.prefix_index.start == self.prefix_index.end => {
                    let bytes = mem::replace(&mut self.bytes, Vec::new());
                    info!(
                        self.handle.logger,
                        "[FINISH] LoadLogPrefixBytes: {}",
                        dump!(bytes.len())
                    );
                    return Ok(Async::Ready(Some(bytes)));
                }
                None => {
                    let index = self.prefix_index.start;
                    let lump_id = self.handle.node_id.to_log_prefix_lump_id(index);
                    info!(
                        self.handle.logger,
                        "[PROGRESS] LoadLogPrefixBytes: {}",
                        dump!(index, lump_id)
                    );
                    let future = self
                        .handle
                        .device
                        .request()
                        .deadline(Deadline::Infinity)
                        .get(lump_id);
                    self.future = Some(into_box_future(future));
                }
            }
        }
        Ok(Async::NotReady)
    }
}
