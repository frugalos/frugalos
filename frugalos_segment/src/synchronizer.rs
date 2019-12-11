use cannyls::device::DeviceHandle;
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::object::ObjectVersion;
use libfrugalos::repair::RepairIdleness;
use prometrics::metrics::MetricBuilder;
use slog::Logger;

use client::storage::StorageClient;
use queue_executor::general_queue_executor::GeneralQueueExecutor;
use queue_executor::repair_queue_executor::RepairQueueExecutor;
use segment_gc::{SegmentGc, SegmentGcMetrics};
use service::ServiceHandle;
use Error;

// TODO: 起動直後の確認は`device.list()`の結果を使った方が効率的
pub struct Synchronizer {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    client: StorageClient,
    segment_gc_metrics: SegmentGcMetrics,
    segment_gc: Option<SegmentGc>,
    segment_gc_step: u64,

    // general-purpose queue.
    general_queue: GeneralQueueExecutor,
    // repair-only queue.
    repair_queue: RepairQueueExecutor,
}
impl Synchronizer {
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        device: DeviceHandle,
        service_handle: ServiceHandle,
        client: StorageClient,
        segment_gc_step: u64,
    ) -> Self {
        let metric_builder = MetricBuilder::new()
            .namespace("frugalos")
            .subsystem("synchronizer")
            .clone();
        // Metrics related to queue length
        let enqueued_repair = metric_builder
            .counter("enqueued_items")
            .label("type", "repair")
            .finish()
            .expect("metric should be well-formed");
        let enqueued_repair_prep = metric_builder
            .counter("enqueued_items")
            .label("type", "repair_prep")
            .finish()
            .expect("metric should be well-formed");
        let enqueued_delete = metric_builder
            .counter("enqueued_items")
            .label("type", "delete")
            .finish()
            .expect("metric should be well-formed");
        let dequeued_repair = metric_builder
            .counter("dequeued_items")
            .label("type", "repair")
            .finish()
            .expect("metric should be well-formed");
        let dequeued_repair_prep = metric_builder
            .counter("dequeued_items")
            .label("type", "repair_prep")
            .finish()
            .expect("metric should be well-formed");
        let dequeued_delete = metric_builder
            .counter("dequeued_items")
            .label("type", "delete")
            .finish()
            .expect("metric should be well-formed");

        let general_queue = GeneralQueueExecutor::new(
            &logger,
            node_id,
            &device,
            &enqueued_repair_prep,
            &enqueued_delete,
            &dequeued_repair_prep,
            &dequeued_delete,
        );
        let repair_queue = RepairQueueExecutor::new(
            &logger,
            node_id,
            &device,
            &client,
            &service_handle,
            &metric_builder,
            &enqueued_repair,
            &dequeued_repair,
        );
        Synchronizer {
            logger,
            node_id,
            device,
            client,
            segment_gc_metrics: SegmentGcMetrics::new(&metric_builder),
            segment_gc: None,
            segment_gc_step,

            general_queue,
            repair_queue,
        }
    }
    pub fn handle_event(&mut self, event: Event) {
        debug!(
            self.logger,
            "New event: {:?} (metadata={})",
            event,
            self.client.is_metadata(),
        );
        // metadata かどうかによって場合分けする。
        if self.client.is_metadata() {
            // metadata の場合、StartSegmentGc と StopSegmentGc に含まれている tx は処理しないといけないので処理する。
            match event {
                Event::StartSegmentGc { tx, .. } => {
                    info!(self.logger, "StartSegmentGc suppressed");
                    tx.exit(Ok(()))
                }
                Event::StopSegmentGc { tx } => {
                    info!(self.logger, "StopSegmentGc suppressed");
                    tx.exit(Ok(()))
                }
                _ => (),
            }
        } else {
            match event {
                Event::Putted { .. } => {
                    self.general_queue.push(&event);
                }
                Event::Deleted { version, .. } => {
                    self.general_queue.push(&event);
                    self.repair_queue.delete(version);
                }
                // Because pushing StartSegmentGc into the task queue causes difficulty in implementation,
                // we decided not to push this task to the task priority queue and handle it manually.
                Event::StartSegmentGc {
                    object_versions,
                    next_commit,
                    tx,
                } => {
                    // If SegmentGc is not being processed now, this event lets the synchronizer to handle one.
                    if self.segment_gc.is_none() {
                        self.segment_gc = Some(SegmentGc::new(
                            &self.logger,
                            self.node_id,
                            &self.device,
                            object_versions.clone(),
                            ObjectVersion(next_commit.as_u64()),
                            self.segment_gc_metrics.clone(),
                            self.segment_gc_step,
                            tx,
                        ));
                    }
                }
                Event::StopSegmentGc { tx } => {
                    // If segment gc is running, stop it without notifying its caller of stopping.
                    self.segment_gc = None;
                    // Notify stop's caller of success.
                    tx.exit(Ok(()));
                }
            }
        }
    }
    pub(crate) fn set_repair_idleness_threshold(
        &mut self,
        repair_idleness_threshold: RepairIdleness,
    ) {
        self.repair_queue
            .set_repair_idleness_threshold(repair_idleness_threshold);
    }
}
impl Future for Synchronizer {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(Some(())) = self.segment_gc.poll().unwrap_or_else(|e| {
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(Some(()))
        }) {
            // Segment GC is done. Clearing the segment_gc field.
            self.segment_gc = None;
            self.segment_gc_metrics.reset();
        }

        if let Async::Ready(Some(versions)) = self.general_queue.poll().unwrap_or_else(|e| {
            warn!(self.logger, "Task failure in general_queue: {}", e);
            Async::Ready(None)
        }) {
            for version in versions {
                self.repair_queue.push(version);
            }
        }

        // Never stops, never fails.
        self.repair_queue.poll().unwrap_or_else(Into::into);
        Ok(Async::NotReady)
    }
}
