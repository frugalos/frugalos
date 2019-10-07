use cannyls::device::DeviceHandle;
use fibers::time::timer::{self, Timeout};
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::object::ObjectVersion;
use libfrugalos::repair::RepairIdleness;
use prometrics::metrics::{Counter, MetricBuilder};
use slog::Logger;
use std::cmp::{self, Reverse};
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::env;
use std::time::{Duration, Instant, SystemTime};

use client::storage::StorageClient;
use delete::DeleteContent;
use repair::{RepairContent, RepairMetrics, RepairPrepContent};
use segment_gc::{SegmentGc, SegmentGcMetrics};
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
    segment_gc_metrics: SegmentGcMetrics,
    segment_gc: Option<SegmentGc>,
    segment_gc_step: u64,
    // The idleness threshold for repair functionality.
    repair_idleness_threshold: RepairIdleness,
    last_not_idle: Instant,
    // Ad-hoc fix for repairing.
    repair_enabled: bool,

    // general-purpose queue.
    general_queue: GeneralQueue,
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
            .label("node", &node_id.to_string())
            .clone();
        let repair_enabled = env::var("FRUGALOS_REPAIR_ENABLED")
            .ok()
            .map_or(false, |v| v == "1");
        // Metrics related to queue length
        let enqueued_repair = metric_builder
            .counter("enqueued_items")
            .label("type", "repair")
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
        let dequeued_delete = metric_builder
            .counter("dequeued_items")
            .label("type", "delete")
            .finish()
            .expect("metric should be well-formed");

        let general_queue = GeneralQueue::new(
            &logger,
            node_id,
            &device,
            &enqueued_repair,
            &enqueued_delete,
            &dequeued_repair,
            &dequeued_delete,
        );
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
            enqueued_repair,
            enqueued_delete,
            dequeued_repair,
            dequeued_delete,
            repair_metrics: RepairMetrics::new(&metric_builder),
            segment_gc_metrics: SegmentGcMetrics::new(&metric_builder),
            segment_gc: None,
            segment_gc_step,
            repair_idleness_threshold: RepairIdleness::Disabled, // No repairing happens
            last_not_idle: Instant::now(),
            repair_enabled,

            general_queue,
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
                    if self.segment_gc.is_none() {
                        self.segment_gc = Some(SegmentGc::new(
                            &self.logger,
                            self.node_id,
                            &self.device,
                            machine.clone(),
                            ObjectVersion(next_commit.as_u64()),
                            self.segment_gc_metrics.clone(),
                            self.segment_gc_step,
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
        while let Async::Ready(Some(())) = self.segment_gc.poll().unwrap_or_else(|e| {
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(Some(()))
        }) {
            // Full sync is done. Clearing the segment_gc field.
            self.segment_gc = None;
            self.segment_gc_metrics.reset();
        }

        if !self.task.is_sleeping() {
            self.last_not_idle = Instant::now();
            debug!(self.logger, "last_not_idle = {:?}", self.last_not_idle);
        }

        while let Async::Ready(()) = self
            .task
            .poll()
            .map(|async| async.map(|_| ()))
            .unwrap_or_else(|e| {
                // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
                warn!(self.logger, "Task failure: {}", e);
                Async::Ready(())
            })
        {
            self.task = Task::Idle;
            if let Some(item) = self.next_todo_item() {
                match item {
                    TodoItem::DeleteContent { versions } => {
                        self.dequeued_delete.increment();
                        self.task = Task::Delete(DeleteContent::new(
                            &self.logger,
                            &self.device,
                            self.node_id,
                            versions,
                        ));
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
                // Wait for put_content_timeout.0 seconds, to avoid race condition with storage.put.
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
    RepairPrep(RepairPrepContent),
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
    type Item = Option<ObjectVersion>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            Task::Idle => Ok(Async::Ready(None)),
            Task::Wait(ref mut f) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|async| async.map(|()| None))),
            Task::Delete(ref mut f) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|async| async.map(|()| None))),
            Task::Repair(ref mut f, _) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|async| async.map(|()| None))),
            Task::RepairPrep(ref mut f) => track!(f.poll()),
        }
    }
}

/// RepairPrep, Delete タスクの管理と、その処理を行う。
struct GeneralQueue {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    repair_prep_queue: RepairPrepQueue,
    delete_queue: DeleteQueue,
    task: Task,
    repair_candidates: BTreeSet<ObjectVersion>,
}

impl GeneralQueue {
    fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        enqueued_repair: &Counter,
        enqueued_delete: &Counter,
        dequeued_repair: &Counter,
        dequeued_delete: &Counter,
    ) -> Self {
        Self {
            logger: logger.clone(),
            node_id,
            device: device.clone(),
            repair_prep_queue: RepairPrepQueue::new(enqueued_repair, dequeued_repair),
            delete_queue: DeleteQueue::new(enqueued_delete, dequeued_delete),
            task: Task::Idle,
            repair_candidates: BTreeSet::new(),
        }
    }
    fn push(&mut self, event: &Event) {
        match *event {
            Event::Putted { version, .. } => {
                self.repair_prep_queue.push(TodoItem::new(event));
                self.repair_candidates.insert(version);
            }
            Event::Deleted { version } => {
                self.repair_candidates.remove(&version);
                self.delete_queue.push(version);
            }
            Event::FullSync { .. } => {
                unreachable!();
            }
        }
    }
    fn pop(&mut self) -> Option<TodoItem> {
        // assert!(self.task == Task::Idle);
        if let Task::Idle = self.task {
        } else {
            unreachable!("self.task != Task::Idle");
        }
        let item = loop {
            // Repair has priority higher than deletion. repair_prep_queue should be examined first.
            let maybe_item = if let Some(item) = self.repair_prep_queue.pop() {
                Some(item)
            } else {
                self.delete_queue.pop()
            };
            if let Some(item) = maybe_item {
                if let TodoItem::RepairContent { version, .. } = item {
                    if !self.repair_candidates.contains(&version) {
                        // 既に削除済み
                        continue;
                    }
                }
                break item;
            } else {
                return None;
            }
        };
        if let Some(duration) = item.wait_time() {
            // NOTE: `assert_eq!(self.task, Task::Idel)`

            let duration = cmp::min(duration, Duration::from_secs(MAX_TIMEOUT_SECONDS));
            self.task = Task::Wait(timer::timeout(duration));
            self.repair_prep_queue.push(item);

            // NOTE:
            // 同期処理が少し遅れても全体としては大きな影響はないので、
            // 一度Wait状態に入った後に、開始時間がより近いアイテムが入って来たとしても、
            // 古いTimeoutをキャンセルしたりはしない.
            //
            // 仮に`put_content_timeout`が極端に長いイベントが発生したとしても、
            // `MAX_TIMEOUT_SECONDS`以上に後続のTODOの処理が(Waitによって)遅延することはない.
            None
        } else {
            Some(item)
        }
    }
}

impl Stream for GeneralQueue {
    type Item = ObjectVersion;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Async::Ready(result) = self.task.poll().unwrap_or_else(|e| {
            // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(None)
        }) {
            self.task = Task::Idle;
            if let Some(version) = result {
                return Ok(Async::Ready(Some(version)));
            }
            if let Some(item) = self.pop() {
                match item {
                    TodoItem::DeleteContent { versions } => {
                        self.task = Task::Delete(DeleteContent::new(
                            &self.logger,
                            &self.device,
                            self.node_id,
                            versions,
                        ));
                    }
                    TodoItem::RepairContent { version, .. } => {
                        self.task = Task::RepairPrep(RepairPrepContent::new(
                            &self.logger,
                            &self.device,
                            self.node_id,
                            version,
                        ));
                    }
                }
            } else if let Task::Idle = self.task {
                break;
            }
        }
        Ok(Async::NotReady)
    }
}

/// Trait for queue.
trait Queue<Pushed, Popped> {
    fn push(&mut self, element: Pushed);
    fn pop(&mut self) -> Option<Popped>;
}

struct RepairPrepQueue {
    queue: BinaryHeap<Reverse<TodoItem>>,
    enqueued: Counter,
    dequeued: Counter,
}
impl RepairPrepQueue {
    fn new(enqueued_repair: &Counter, dequeued_repair: &Counter) -> Self {
        Self {
            queue: BinaryHeap::new(),
            enqueued: enqueued_repair.clone(),
            dequeued: dequeued_repair.clone(),
        }
    }
}
impl Queue<TodoItem, TodoItem> for RepairPrepQueue {
    fn push(&mut self, element: TodoItem) {
        self.queue.push(Reverse(element));
        self.enqueued.increment();
    }
    fn pop(&mut self) -> Option<TodoItem> {
        let result = self.queue.pop();
        if let Some(_) = result {
            self.dequeued.increment();
        }
        // Shrink if necessary
        if self.queue.capacity() > 32 && self.queue.len() < self.queue.capacity() / 2 {
            self.queue.shrink_to_fit();
        }
        result.map(|element| element.0)
    }
}

struct DeleteQueue {
    deque: VecDeque<ObjectVersion>,
    enqueued: Counter,
    dequeued: Counter,
}
impl DeleteQueue {
    fn new(enqueued_delete: &Counter, dequeued_delete: &Counter) -> Self {
        Self {
            deque: VecDeque::new(),
            enqueued: enqueued_delete.clone(),
            dequeued: dequeued_delete.clone(),
        }
    }
}
impl Queue<ObjectVersion, TodoItem> for DeleteQueue {
    fn push(&mut self, element: ObjectVersion) {
        self.deque.push_back(element);
        self.enqueued.increment();
    }
    fn pop(&mut self) -> Option<TodoItem> {
        let result = self.deque.pop_front();
        if let Some(_) = result {
            self.dequeued.increment();
        }
        if self.deque.capacity() > 32 && self.deque.len() < self.deque.capacity() / 2 {
            self.deque.shrink_to_fit();
        }
        result.map(|version| TodoItem::DeleteContent {
            versions: vec![version],
        })
    }
}
