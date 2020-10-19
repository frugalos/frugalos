use crate::client::storage::{GetFragment, MaybeFragment, StorageClient};
use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpHeader;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::{Counter, Histogram, MetricBuilder};
use slog::Logger;
use std::time::Instant;

use crate::util::{into_box_future, BoxFuture, Phase3};
use crate::{config, Error};

#[derive(Clone)]
pub(crate) struct RepairMetrics {
    pub(crate) repairs_success_total: Counter,
    pub(crate) repairs_failure_total: Counter,
    pub(crate) repairs_unnecessary_total: Counter,
    pub(crate) repairs_durations_seconds_step_1: Histogram,
    pub(crate) repairs_durations_seconds_step_2: Histogram,
    pub(crate) repairs_durations_seconds: Histogram,
}

impl RepairMetrics {
    pub(crate) fn new(metric_builder: &MetricBuilder) -> Self {
        RepairMetrics {
            repairs_success_total: metric_builder
                .counter("repairs_success_total")
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            repairs_failure_total: metric_builder
                .counter("repairs_failure_total")
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            repairs_unnecessary_total: metric_builder
                .counter("repairs_unnecessary_total")
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            repairs_durations_seconds_step_1: metric_builder
                .histogram("repairs_durations_seconds_step_1")
                .bucket(0.001)
                .bucket(0.005)
                .bucket(0.01)
                .bucket(0.05)
                .bucket(0.1)
                .bucket(0.5)
                .bucket(1.0)
                .bucket(5.0)
                .bucket(10.0)
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            repairs_durations_seconds_step_2: metric_builder
                .histogram("repairs_durations_seconds_step_2")
                .bucket(0.001)
                .bucket(0.005)
                .bucket(0.01)
                .bucket(0.05)
                .bucket(0.1)
                .bucket(0.5)
                .bucket(1.0)
                .bucket(5.0)
                .bucket(10.0)
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            repairs_durations_seconds: metric_builder
                .histogram("repairs_durations_seconds")
                .bucket(0.001)
                .bucket(0.005)
                .bucket(0.01)
                .bucket(0.05)
                .bucket(0.1)
                .bucket(0.5)
                .bucket(1.0)
                .bucket(5.0)
                .bucket(10.0)
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
        }
    }
}

// 以下の処理を行う:
// 1. `version`に対応するオブジェクトの中身が存在するかチェック
// 存在すれば None (リペアの必要なし) を、存在しなければ Some(version) (リペアの必要ありで、 ObjectVersion は version) を返す。
pub(crate) struct RepairPrepContent {
    future: BoxFuture<Option<ObjectVersion>>,
}
impl RepairPrepContent {
    pub fn new(
        logger: &Logger,
        device: &DeviceHandle,
        node_id: NodeId,
        version: ObjectVersion,
    ) -> Self {
        let logger = logger.clone();
        let device = device.clone();
        let lump_id = config::make_lump_id(&node_id, version);
        debug!(
            logger,
            "Starts checking content: version={:?}, lump_id={:?}", version, lump_id
        );
        let future = into_box_future(
            device
                .request()
                .deadline(Deadline::Infinity)
                .head(lump_id)
                .map(move |result| result.map_or(Some(version), |_| None)),
        );
        RepairPrepContent { future }
    }
}
impl Future for RepairPrepContent {
    type Item = Option<ObjectVersion>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.future.poll()
    }
}

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
    repair_metrics: RepairMetrics,
    phase: Phase3<BoxFuture<Option<LumpHeader>>, GetFragment, BoxFuture<bool>>,
}
impl RepairContent {
    pub fn new(
        logger: &Logger,
        device: &DeviceHandle,
        node_id: NodeId,
        client: &StorageClient,
        repair_metrics: &RepairMetrics,
        version: ObjectVersion,
    ) -> Self {
        let logger = logger.clone();
        let device = device.clone();
        let lump_id = config::make_lump_id(&node_id, version);
        let started_at = Instant::now();
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
            client: client.clone(),
            device,
            started_at,
            repair_metrics: repair_metrics.clone(),
            phase,
        }
    }
}
impl Future for RepairContent {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(|e| {
            self.repair_metrics.repairs_failure_total.increment();
            e
        }))? {
            let next = match phase {
                Phase3::A(Some(_)) => {
                    debug!(self.logger, "The object {:?} already exists", self.version);
                    self.repair_metrics.repairs_unnecessary_total.increment();
                    return Ok(Async::Ready(()));
                }
                Phase3::A(None) => {
                    debug!(
                        self.logger,
                        "The object {:?} does not exist (try repairing)", self.version
                    );

                    let elapsed =
                        prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                    self.repair_metrics
                        .repairs_durations_seconds_step_1
                        .observe(elapsed);

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
                    self.repair_metrics.repairs_failure_total.increment();
                    return Ok(Async::Ready(()));
                }
                Phase3::B(MaybeFragment::Fragment(mut content)) => {
                    crate::client::storage::append_checksum(&mut content); // TODO

                    let lump_id = config::make_lump_id(&self.node_id, self.version);
                    debug!(
                        self.logger,
                        "Puts repaired content: version={:?}, lump_id={:?}, content_size={}",
                        self.version,
                        lump_id,
                        content.len()
                    );
                    let elapsed =
                        prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                    self.repair_metrics
                        .repairs_durations_seconds_step_2
                        .observe(elapsed);

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
                    self.repair_metrics.repairs_success_total.increment();
                    let elapsed =
                        prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                    self.repair_metrics
                        .repairs_durations_seconds
                        .observe(elapsed);
                    return Ok(Async::Ready(()));
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}
