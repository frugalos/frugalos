use cannyls::device::DeviceHandle;
use fibers::time::timer::{self, Timeout};
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use libfrugalos::repair::RepairIdleness;
use prometrics::metrics::{Counter, MetricBuilder};
use slog::Logger;
use std::cmp::{self, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
use std::env;
use std::time::{Duration, Instant, SystemTime};

use client::storage::StorageClient;
use delete::DeleteContent;
use full_sync::{FullSync, FullSyncMetrics};
use repair::{RepairContent, RepairMetrics};
use service::{RepairLock, ServiceHandle};
use Error;

const MAX_TIMEOUT_SECONDS: u64 = 60;
const DELETE_CONCURRENCY: usize = 16;

// TODO: 起動直後の確認は`device.list()`の結果を使った方が効率的
pub struct Synchronizer {
    pub(crate) logger: Logger,
    pub(crate) node_id: NodeId,
    pub(crate) device: DeviceHandle,
    pub(crate) client: StorageClient,
    service_handle: ServiceHandle,
    task: Task,
    // TODO: define specific types for two kinds of items and specialize the procedure for each todo queue
    todo_delete: BinaryHeap<Reverse<TodoItem>>, // To-do queue for delete. Can hold `TodoItem::DeleteContent`s only.
    todo_repair: BinaryHeap<Reverse<TodoItem>>, // To-do queue for repair. Can hold `TodoItem::RepairContent`s only.
    repair_candidates: BTreeSet<ObjectVersion>,
    enqueued_repair: Counter,
    enqueued_delete: Counter,
    dequeued_repair: Counter,
    dequeued_delete: Counter,
    pub(crate) repair_metrics: RepairMetrics,
    full_sync_metrics: FullSyncMetrics,
    full_sync: Option<FullSync>,
    full_sync_step: u64,
    // The idleness threshold for repair functionality.
    repair_idleness_threshold: RepairIdleness,
    last_not_idle: Instant,
    // Ad-hoc fix for repairing.
    repair_enabled: bool,
}
impl Synchronizer {
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        device: DeviceHandle,
        service_handle: ServiceHandle,
        client: StorageClient,
        full_sync_step: u64,
    ) -> Self {
        let metric_builder = MetricBuilder::new()
            .namespace("frugalos")
            .subsystem("synchronizer")
            .label("node", &node_id.to_string())
            .clone();
        let repair_enabled = env::var("FRUGALOS_REPAIR_ENABLED")
            .ok()
            .map_or(false, |v| v == "1");
        Synchronizer {
            logger,
            node_id,
            device,
            service_handle,
            client,
            task: Task::Idle,
            todo_delete: BinaryHeap::new(),
            todo_repair: BinaryHeap::new(),
            repair_candidates: BTreeSet::new(),
            enqueued_repair: metric_builder
                .counter("enqueued_items")
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            enqueued_delete: metric_builder
                .counter("enqueued_items")
                .label("type", "delete")
                .finish()
                .expect("metric should be well-formed"),
            dequeued_repair: metric_builder
                .counter("dequeued_items")
                .label("type", "repair")
                .finish()
                .expect("metric should be well-formed"),
            dequeued_delete: metric_builder
                .counter("dequeued_items")
                .label("type", "delete")
                .finish()
                .expect("metric should be well-formed"),
            repair_metrics: RepairMetrics::new(&metric_builder),
            full_sync_metrics: FullSyncMetrics::new(&metric_builder),
            full_sync: None,
            full_sync_step,
            repair_idleness_threshold: RepairIdleness::Disabled, // No repairing happens
            last_not_idle: Instant::now(),
            repair_enabled,
        }
    }
    pub fn handle_event(&mut self, event: &Event) {
        debug!(
            self.logger,
            "New event: {:?} (metadata={}, todo.len={}, todo_repair.len = {}, todo_delete.len = {})",
            event,
            self.client.is_metadata(),
            self.todo_repair.len() + self.todo_delete.len(),
            self.todo_repair.len(),
            self.todo_delete.len(),
        );
        if !self.client.is_metadata() {
            match *event {
                Event::Putted { version, .. } => {
                    // TODO: this is an ad-hoc fix. Needs rewriting completely.
                    if self.repair_enabled {
                        self.enqueued_repair.increment();
                        self.repair_candidates.insert(version);
                    }
                }
                Event::Deleted { version } => {
                    self.repair_candidates.remove(&version);
                    if let Some(mut head) = self.todo_delete.peek_mut() {
                        if let TodoItem::DeleteContent { ref mut versions } = head.0 {
                            if versions.len() < DELETE_CONCURRENCY {
                                versions.push(version);
                                return;
                            }
                        }
                    }
                    self.enqueued_delete.increment();
                }
                // Because pushing FullSync into the task queue causes difficulty in implementation,
                // we decided not to push this task to the task priority queue and handle it manually.
                Event::FullSync {
                    ref machine,
                    next_commit,
                } => {
                    // If FullSync is not being processed now, this event lets the synchronizer to handle one.
                    if self.full_sync.is_none() {
                        self.full_sync = Some(FullSync::new(
                            &self.logger,
                            self.node_id,
                            &self.device,
                            machine.clone(),
                            ObjectVersion(next_commit.as_u64()),
                            self.full_sync_metrics.clone(),
                            self.full_sync_step,
                        ));
                    }
                }
            }
            if let Event::FullSync { .. } = &event {
            } else if let Event::Putted { .. } = &event {
                // TODO: this is an ad-hoc fix. Needs rewriting completely.
                if self.repair_enabled {
                    self.todo_repair.push(Reverse(TodoItem::new(&event)));
                }
            } else {
                self.todo_delete.push(Reverse(TodoItem::new(&event)));
            }
        }
    }
    fn next_todo_item(&mut self) -> Option<TodoItem> {
        let item = loop {
            // Repair has priority higher than deletion. If repair is enabled, todo_repair should be examined first.
            let maybe_item = if self.is_repair_enabled() {
                if let Some(item) = self.todo_repair.pop() {
                    Some(item)
                } else {
                    self.todo_delete.pop()
                }
            } else {
                self.todo_delete.pop()
            };
            if let Some(item) = maybe_item {
                if let TodoItem::RepairContent { version, .. } = item.0 {
                    if !self.repair_candidates.contains(&version) {
                        // 既に削除済み
                        self.dequeued_repair.increment();
                        continue;
                    }
                }
                break item.0;
            } else {
                return None;
            }
        };
        if let Some(duration) = item.wait_time() {
            // NOTE: `assert_eq!(self.task, Task::Idel)`

            let duration = cmp::min(duration, Duration::from_secs(MAX_TIMEOUT_SECONDS));
            self.task = Task::Wait(timer::timeout(duration));
            self.todo_repair.push(Reverse(item));

            // NOTE:
            // 同期処理が少し遅れても全体としては大きな影響はないので、
            // 一度Wait状態に入った後に、開始時間がより近いアイテムが入って来たとしても、
            // 古いTimeoutをキャンセルしたりはしない.
            //
            // 仮に`put_content_timeout`が極端に長いイベントが発生したとしても、
            // `MAX_TIMEOUT_SECONDS`以上に後続のTODOの処理が(Waitによって)遅延することはない.
            None
        } else {
            if self.todo_delete.capacity() > 32
                && self.todo_delete.len() < self.todo_delete.capacity() / 2
            {
                self.todo_delete.shrink_to_fit();
            }
            if self.todo_repair.capacity() > 32
                && self.todo_repair.len() < self.todo_repair.capacity() / 2
            {
                self.todo_repair.shrink_to_fit();
            }
            if let TodoItem::RepairContent { version, .. } = item {
                self.repair_candidates.remove(&version);
            }
            Some(item)
        }
    }
    pub(crate) fn set_repair_idleness_threshold(
        &mut self,
        repair_idleness_threshold: RepairIdleness,
    ) {
        info!(
            self.logger,
            "repair_idleness_threshold set to {:?}", repair_idleness_threshold,
        );
        self.repair_idleness_threshold = repair_idleness_threshold;
    }
    fn is_repair_enabled(&self) -> bool {
        match self.repair_idleness_threshold {
            RepairIdleness::Threshold(_) => true,
            RepairIdleness::Disabled => false,
        }
    }
}
impl Future for Synchronizer {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(Some(())) = self.full_sync.poll().unwrap_or_else(|e| {
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(Some(()))
        }) {
            // Full sync is done. Clearing the full_sync field.
            self.full_sync = None;
            self.full_sync_metrics.reset();
        }

        if !self.task.is_sleeping() {
            self.last_not_idle = Instant::now();
            debug!(self.logger, "last_not_idle = {:?}", self.last_not_idle);
        }

        while let Async::Ready(()) = self.task.poll().unwrap_or_else(|e| {
            // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(())
        }) {
            self.task = Task::Idle;
            if let Some(item) = self.next_todo_item() {
                match item {
                    TodoItem::DeleteContent { versions } => {
                        self.dequeued_delete.increment();
                        self.task = Task::Delete(DeleteContent::new(self, versions));
                        self.last_not_idle = Instant::now();
                    }
                    TodoItem::RepairContent { version, .. } => {
                        if let RepairIdleness::Threshold(repair_idleness_threshold_duration) =
                            self.repair_idleness_threshold
                        {
                            let elapsed = self.last_not_idle.elapsed();
                            if elapsed < repair_idleness_threshold_duration {
                                self.repair_candidates.insert(version);
                                self.todo_repair.push(Reverse(item));
                                break;
                            } else {
                                let repair_lock = self.service_handle.acquire_repair_lock();
                                if let Some(repair_lock) = repair_lock {
                                    self.dequeued_repair.increment();
                                    self.task = Task::Repair(
                                        RepairContent::new(self, version),
                                        repair_lock,
                                    );
                                    self.last_not_idle = Instant::now();
                                } else {
                                    self.repair_candidates.insert(version);
                                    self.todo_repair.push(Reverse(item));
                                    break;
                                }
                            }
                        }
                    }
                }
            } else if let Task::Idle = self.task {
                break;
            }
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
enum TodoItem {
    RepairContent {
        start_time: SystemTime,
        version: ObjectVersion,
    },
    DeleteContent {
        versions: Vec<ObjectVersion>,
    },
}
impl TodoItem {
    pub fn new(event: &Event) -> Self {
        match *event {
            Event::Deleted { version } => TodoItem::DeleteContent {
                versions: vec![version],
            },
            Event::Putted {
                version,
                put_content_timeout,
            } => {
                let start_time = SystemTime::now() + Duration::from_secs(put_content_timeout.0);
                TodoItem::RepairContent {
                    start_time,
                    version,
                }
            }
            Event::FullSync { .. } => unreachable!(),
        }
    }
    pub fn wait_time(&self) -> Option<Duration> {
        match *self {
            TodoItem::DeleteContent { .. } => None,
            TodoItem::RepairContent { start_time, .. } => {
                start_time.duration_since(SystemTime::now()).ok()
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Task {
    Idle,
    Wait(Timeout),
    Delete(DeleteContent),
    Repair(RepairContent, RepairLock),
}
impl Task {
    fn is_sleeping(&self) -> bool {
        match self {
            Task::Idle => true,
            Task::Wait(_) => true,
            _ => false,
        }
    }
}
impl Future for Task {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            Task::Idle => Ok(Async::Ready(())),
            Task::Wait(ref mut f) => track!(f.poll().map_err(Error::from)),
            Task::Delete(ref mut f) => track!(f.poll()),
            Task::Repair(ref mut f, _) => track!(f.poll()),
        }
    }
}
