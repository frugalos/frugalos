use cannyls::device::DeviceHandle;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use libfrugalos::repair::RepairIdleness;
use prometrics::metrics::{Counter, MetricBuilder};
use slog::Logger;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::time::Instant;

use client::storage::StorageClient;
use repair::{RepairContent, RepairMetrics};
use service::{RepairLock, ServiceHandle};
use Error;

#[allow(clippy::large_enum_variant)]
enum Task {
    Idle,
    Repair(RepairContent, RepairLock),
}
impl Task {
    fn is_sleeping(&self) -> bool {
        match self {
            Task::Idle => true,
            Task::Repair(_, _) => false,
        }
    }
}
impl Future for Task {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            Task::Idle => Ok(Async::Ready(())),
            Task::Repair(ref mut f, _) => track!(f.poll().map_err(Error::from)),
        }
    }
}

/// 若い番号のオブジェクトから順番にリペアするためのキュー。
pub(crate) struct RepairQueueExecutor {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    client: StorageClient,
    service_handle: ServiceHandle,
    task: Task,
    queue: BTreeSet<ObjectVersion>,
    // The idleness threshold for repair functionality.
    repair_idleness_threshold: RepairIdleness,
    last_not_idle: Instant,
    repair_metrics: RepairMetrics,
    enqueued_repair: Counter,
    dequeued_repair: Counter,
}
impl RepairQueueExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        client: &StorageClient,
        service_handle: &ServiceHandle,
        metric_builder: &MetricBuilder,
        enqueued_repair: &Counter,
        dequeued_repair: &Counter,
    ) -> Self {
        RepairQueueExecutor {
            logger: logger.clone(),
            node_id,
            device: device.clone(),
            client: client.clone(),
            service_handle: service_handle.clone(),
            task: Task::Idle,
            queue: BTreeSet::new(),
            repair_idleness_threshold: RepairIdleness::Disabled,
            last_not_idle: Instant::now(),
            repair_metrics: RepairMetrics::new(metric_builder),
            enqueued_repair: enqueued_repair.clone(),
            dequeued_repair: dequeued_repair.clone(),
        }
    }
    /// Pushes an element into this queue.
    pub(crate) fn push(&mut self, version: ObjectVersion) {
        // Insert version. Also, increment enqueued_repair if version was absent before insertion.
        if self.queue.insert(version) {
            self.enqueued_repair.increment();
        }
    }
    /// Deletes an element from this queue.
    /// This is typically called for deleted objects.
    pub(crate) fn delete(&mut self, version: ObjectVersion) {
        if self.queue.remove(&version) {
            self.dequeued_repair.increment();
        }
    }
    fn pop(&mut self) -> Option<ObjectVersion> {
        // Pick the minimum element, if queue is not empty.
        let result = self.queue.iter().next().copied();
        if let Some(version) = result {
            self.queue.remove(&version);
            self.dequeued_repair.increment();
        }
        result
    }
    pub(crate) fn set_repair_idleness_threshold(
        &mut self,
        repair_idleness_threshold: RepairIdleness,
    ) {
        self.repair_idleness_threshold = repair_idleness_threshold;
    }
}
impl Future for RepairQueueExecutor {
    type Item = Infallible; // This executor will never finish normally.
    type Error = Infallible;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if !self.task.is_sleeping() {
            self.last_not_idle = Instant::now();
            debug!(self.logger, "last_not_idle = {:?}", self.last_not_idle);
        }

        while let Async::Ready(()) = self.task.poll().unwrap_or_else(|e| {
            // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
            warn!(self.logger, "Task failure in RepairQueueExecutor: {}", e);
            Async::Ready(())
        }) {
            self.task = Task::Idle;
            if let RepairIdleness::Threshold(repair_idleness_threshold_duration) =
                self.repair_idleness_threshold
            {
                if let Some(version) = self.pop() {
                    let elapsed = self.last_not_idle.elapsed();
                    if elapsed < repair_idleness_threshold_duration {
                        self.push(version);
                        break;
                    } else {
                        let repair_lock = self.service_handle.acquire_repair_lock();
                        if let Some(repair_lock) = repair_lock {
                            self.task = Task::Repair(
                                RepairContent::new(
                                    &self.logger,
                                    &self.device,
                                    self.node_id,
                                    &self.client,
                                    &self.repair_metrics,
                                    version,
                                ),
                                repair_lock,
                            );
                            self.last_not_idle = Instant::now();
                        } else {
                            self.push(version);
                            break;
                        }
                    }
                }
            }
            if let Task::Idle = self.task {
                break;
            }
        }
        Ok(Async::NotReady)
    }
}
