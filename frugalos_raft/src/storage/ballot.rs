use cannyls::deadline::Deadline;
use cannyls::lump::LumpData;
use futures::future::{Either, Failed};
use futures::{self, Async, Future, Poll};
use raftlog::election::Ballot;
use raftlog::Error;
use std::time::Instant;

use super::{into_box_future, BoxFuture, Handle, Storage};
use protobuf;
use StorageMetrics;

/// `Ballot`を永続ストレージから読み込むための`Future`実装.
// #[derive(Debug)]
pub struct LoadBallot {
    handle: Handle,
    future: BoxFuture<Option<LumpData>>,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl LoadBallot {
    pub(crate) fn new(storage: &Storage) -> Self {
        let handle = storage.handle.clone();
        let lump_id = handle.node_id.to_ballot_lump_id();
        info!(handle.logger, "[START] LoadBallot: {}", dump!(lump_id));
        let future = handle
            .device
            .request()
            .wait_for_running() // 一番最初の呼び出し時には、まだデバイス起動中の可能性がある
            .deadline(Deadline::Immediate)
            .get(lump_id);
        let future = into_box_future(future);
        LoadBallot {
            handle,
            future,
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
}
impl Future for LoadBallot {
    type Item = Option<Ballot>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(data) = track!(self.future.poll())? {
            let ballot = if let Some(data) = data {
                Some(track!(protobuf::decode_ballot(data.as_bytes()))?)
            } else {
                None
            };
            info!(self.handle.logger, "[FINISH] LoadBallot: {}", dump!(ballot));
            let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
            self.metrics.load_ballot_duration_seconds.observe(elapsed);
            Ok(Async::Ready(ballot))
        } else {
            Ok(Async::NotReady)
        }
    }
}

/// `Ballot`を永続ストレージに保存するための`Future`実装.
// #[derive(Debug)]
pub struct SaveBallot {
    handle: Handle,
    future: Either<BoxFuture<bool>, Failed<bool, Error>>,
    started_at: Instant,
    metrics: StorageMetrics,
}
impl SaveBallot {
    pub(crate) fn new(storage: &Storage, ballot: Ballot) -> Self {
        let handle = storage.handle.clone();
        let lump_id = handle.node_id.to_ballot_lump_id();
        info!(
            handle.logger,
            "[START] SaveBallot: {}",
            dump!(lump_id, ballot)
        );
        let future = match track!(protobuf::encode_ballot(ballot)) {
            Ok(bytes) => {
                let data = LumpData::new_embedded(bytes).expect("Never fails");
                Either::A(into_box_future(
                    handle
                        .device
                        .request()
                        .deadline(Deadline::Immediate)
                        .put(lump_id, data),
                ))
            }
            Err(e) => Either::B(futures::failed(e)),
        };
        SaveBallot {
            handle,
            future,
            started_at: Instant::now(),
            metrics: storage.metrics.clone(),
        }
    }
}
impl Future for SaveBallot {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let polled = track!(self.future.poll())?;
        if polled.is_ready() {
            info!(self.handle.logger, "[FINISH] SaveBallot");
            let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
            self.metrics.save_ballot_duration_seconds.observe(elapsed);
        }
        Ok(polled.map(|_| ()))
    }
}
