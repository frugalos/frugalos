use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpHeader;
use client::storage::{GetFragment, MaybeFragment, StorageClient};
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::{Counter, Histogram};
use slog::Logger;
use std::time::Instant;

use synchronizer::Synchronizer;
use util::{into_box_future, BoxFuture, Phase3};
use {config, Error};

// NOTE
// ====
//
// 以下の処理を行う:
// 1. `version`に対応するオブジェクトの中身が存在するかチェック
// 2. 存在しないなら復元を試みる (以下は一番複雑なdispersedの場合のみの処理を記述)
// 2-1. 一つ前のクラスタ構成での対応するノードからセグメントを移動する (クラスタ構成変更時; 未実装)
//      => 移動が完了したら、古いノードのセグメントは削除する
// 2-2. それが無理なら、クラスタ内の他のノードからセグメント群を集めて復元する
//
// なおバケツの種別が`metadata`の場合には、復元は不要.
// `replicated`の場合には、クラスタ内の任意の一つのノードからコピーすれば良い
// (クラスタ構成変更によって、完全にノード構成が変わった場合にだけ注意が必要).
pub(crate) struct RepairContent {
    logger: Logger,
    node_id: NodeId,
    version: ObjectVersion,
    client: StorageClient,
    device: DeviceHandle,
    started_at: Instant,
    repairs_success_total: Counter,
    repairs_failure_total: Counter,
    repairs_unnecessary_total: Counter,
    repairs_durations_seconds: Histogram,
    phase: Phase3<BoxFuture<Option<LumpHeader>>, GetFragment, BoxFuture<bool>>,
}
impl RepairContent {
    pub fn new(synchronizer: &Synchronizer, version: ObjectVersion) -> Self {
        let logger = synchronizer.logger.clone();
        let device = synchronizer.device.clone();
        let node_id = synchronizer.node_id;
        let lump_id = config::make_lump_id(&node_id, version);
        let started_at = Instant::now();
        let repairs_success_total = synchronizer.repairs_success_total.clone();
        let repairs_failure_total = synchronizer.repairs_failure_total.clone();
        let repairs_unnecessary_total = synchronizer.repairs_unnecessary_total.clone();
        let repairs_durations_seconds = synchronizer.repairs_durations_seconds.clone();
        debug!(
            logger,
            "Starts checking content: version={:?}, lump_id={:?}", version, lump_id
        );
        let phase = Phase3::A(into_box_future(
            device.request().deadline(Deadline::Infinity).head(lump_id),
        ));
        RepairContent {
            logger,
            node_id,
            version,
            client: synchronizer.client.clone(),
            device,
            started_at,
            repairs_success_total,
            repairs_failure_total,
            repairs_unnecessary_total,
            repairs_durations_seconds,
            phase,
        }
    }
}
impl Future for RepairContent {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(|e| {
            self.repairs_failure_total.increment();
            e
        }))? {
            let next = match phase {
                Phase3::A(Some(_)) => {
                    debug!(self.logger, "The object {:?} already exists", self.version);
                    self.repairs_unnecessary_total.increment();
                    return Ok(Async::Ready(()));
                }
                Phase3::A(None) => {
                    debug!(
                        self.logger,
                        "The object {:?} does not exist (try repairing)", self.version
                    );

                    let future = self.client.clone().get_fragment(self.node_id, self.version);
                    Phase3::B(future)
                }
                Phase3::B(MaybeFragment::NotParticipant) => {
                    debug!(
                        self.logger,
                        "The object {:?} should not be stored on this node: node_id={:?}",
                        self.version,
                        self.node_id
                    );
                    self.repairs_failure_total.increment();
                    return Ok(Async::Ready(()));
                }
                Phase3::B(MaybeFragment::Fragment(mut content)) => {
                    ::client::storage::append_checksum(&mut content); // TODO

                    let lump_id = config::make_lump_id(&self.node_id, self.version);
                    debug!(
                        self.logger,
                        "Puts repaired content: version={:?}, lump_id={:?}, content_size={}",
                        self.version,
                        lump_id,
                        content.len()
                    );

                    let data = track!(self.device.allocate_lump_data_with_bytes(&content))
                        .expect("TODO: error handling");
                    let future = self
                        .device
                        .request()
                        .deadline(Deadline::Infinity)
                        .put(lump_id, data);
                    Phase3::C(into_box_future(future))
                }
                Phase3::C(_) => {
                    debug!(
                        self.logger,
                        "Completed repairing content: {:?}", self.version
                    );
                    self.repairs_success_total.increment();
                    let elapsed =
                        prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                    self.repairs_durations_seconds.observe(elapsed);
                    return Ok(Async::Ready(()));
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}
