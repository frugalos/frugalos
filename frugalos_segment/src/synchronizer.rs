use cannyls;
use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpHeader;
use fibers::time::timer::{self, Timeout};
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::{Counter, Gauge, Histogram, MetricBuilder};
use slog::Logger;
use std::cmp::{self, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
use std::time::{Duration, Instant, SystemTime};

use client::storage::{GetFragment, MaybeFragment, StorageClient};
use config;
use full_sync::FullSync;
use util::Phase3;
use Error;

const MAX_TIMEOUT_SECONDS: u64 = 60;
const DELETE_CONCURRENCY: usize = 16;

type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send + 'static>;

// TODO: 起動直後の確認は`device.list()`の結果を使った方が効率的
pub struct Synchronizer {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    client: StorageClient,
    task: Task,
    todo: BinaryHeap<Reverse<TodoItem>>,
    repair_candidates: BTreeSet<ObjectVersion>,
    repair_enabled: bool,
    enqueued_repair: Counter,
    enqueued_delete: Counter,
    dequeued_repair: Counter,
    dequeued_delete: Counter,
    repairs_success_total: Counter,
    repairs_failure_total: Counter,
    repairs_durations_seconds: Histogram,
    full_sync_count: Counter,
    full_sync_deleted_objects: Counter,
    // How many objects have to be swept before full_sync is completed (including non-existent ones)
    full_sync_remaining: Gauge,
    full_sync: Option<FullSync>,
    full_sync_step: u64,
    // The idleness threshold for repair functionality.
    // Set it to a negative integer if you don't want repairing at all.
    repair_idleness_threshold: i64,
}
impl Synchronizer {
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        device: DeviceHandle,
        client: StorageClient,
        repair_enabled: bool,
        full_sync_step: u64,
    ) -> Self {
        let metric_builder = MetricBuilder::new()
            .namespace("frugalos")
            .subsystem("synchronizer")
            .label("node", &node_id.to_string())
            .clone();
        Synchronizer {
            logger,
            node_id,
            device,
            client,
            task: Task::Idle,
            todo: BinaryHeap::new(),
            repair_candidates: BTreeSet::new(),
            repair_enabled,
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
            full_sync_count: metric_builder
                .counter("full_sync_count")
                .finish()
                .expect("metric should be well-formed"),
            full_sync_deleted_objects: metric_builder
                .counter("full_sync_deleted_objects")
                .finish()
                .expect("metric should be well-formed"),
            full_sync_remaining: metric_builder
                .gauge("full_sync_remaining")
                .finish()
                .expect("metric should be well-formed"),
            full_sync: None,
            full_sync_step,
            repair_idleness_threshold: -1, // No repairing happens
        }
    }
    pub fn handle_event(&mut self, event: &Event) {
        debug!(
            self.logger,
            "New event: {:?} (metadata={}, todo.len={})",
            event,
            self.client.is_metadata(),
            self.todo.len()
        );
        if !self.client.is_metadata() {
            match *event {
                Event::Putted { version, .. } => {
                    self.enqueued_repair.increment();
                    if self.repair_enabled {
                        self.repair_candidates.insert(version);
                    }
                }
                Event::Deleted { version } => {
                    self.repair_candidates.remove(&version);
                    if let Some(mut head) = self.todo.peek_mut() {
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
                    if self.full_sync.is_none() && self.repair_enabled {
                        self.full_sync = Some(FullSync::new(
                            &self.logger,
                            self.node_id,
                            &self.device,
                            machine.clone(),
                            ObjectVersion(next_commit.as_u64()),
                            self.full_sync_count.clone(),
                            self.full_sync_deleted_objects.clone(),
                            self.full_sync_remaining.clone(),
                            self.full_sync_step,
                        ));
                    }
                }
            }
            if let Event::FullSync { .. } = &event {
            } else {
                self.todo.push(Reverse(TodoItem::new(&event)));
            }
        }
    }
    fn next_todo_item(&mut self) -> Option<TodoItem> {
        let item = loop {
            if let Some(item) = self.todo.pop() {
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
            self.todo.push(Reverse(item));

            // NOTE:
            // 同期処理が少し遅れても全体としては大きな影響はないので、
            // 一度Wait状態に入った後に、開始時間がより近いアイテムが入って来たとしても、
            // 古いTimeoutをキャンセルしたりはしない.
            //
            // 仮に`put_content_timeout`が極端に長いイベントが発生したとしても、
            // `MAX_TIMEOUT_SECONDS`以上に後続のTODOの処理が(Waitによって)遅延することはない.
            None
        } else {
            if self.todo.capacity() > 32 && self.todo.len() < self.todo.capacity() / 2 {
                self.todo.shrink_to_fit();
            }
            if let TodoItem::RepairContent { version, .. } = item {
                self.repair_candidates.remove(&version);
            }
            Some(item)
        }
    }
    pub(crate) fn set_repair_idleness_threshold(&mut self, repair_idleness_threshold: i64) {
        if self.repair_idleness_threshold != repair_idleness_threshold {
            info!(
                self.logger,
                "repair_idleness_threshold set to {}", repair_idleness_threshold,
            );
        }
        self.repair_idleness_threshold = repair_idleness_threshold;
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
            self.full_sync_remaining.set(0.0);
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
                    }
                    TodoItem::RepairContent { version, .. } => {
                        self.dequeued_repair.increment();
                        self.task = Task::Repair(RepairContent::new(self, version));
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
    Repair(RepairContent),
}
impl Future for Task {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            Task::Idle => Ok(Async::Ready(())),
            Task::Wait(ref mut f) => track!(f.poll().map_err(Error::from)),
            Task::Delete(ref mut f) => track!(f.poll()),
            Task::Repair(ref mut f) => track!(f.poll()),
        }
    }
}

// #[derive(Debug)]
struct DeleteContent {
    futures: Vec<BoxFuture<bool>>,
}
impl DeleteContent {
    pub fn new(synchronizer: &Synchronizer, versions: Vec<ObjectVersion>) -> Self {
        debug!(
            synchronizer.logger,
            "Starts deleting contents: versions={:?}", versions
        );

        let futures = versions
            .into_iter()
            .map(move |v| {
                let lump_id = config::make_lump_id(&synchronizer.node_id, v);
                let future = synchronizer
                    .device
                    .request()
                    .deadline(Deadline::Infinity)
                    .delete(lump_id);
                into_box_future(future)
            })
            .collect();
        DeleteContent { futures }
    }
}
impl Future for DeleteContent {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut i = 0;
        while i < self.futures.len() {
            // NOTE: 一つ失敗しても全てを失敗扱いにする必要はない
            if let Async::Ready(_) = track!(self.futures[i].poll().map_err(Error::from))? {
                self.futures.swap_remove(i);
            } else {
                i += 1;
            }
        }
        if self.futures.is_empty() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

fn into_box_future<F>(future: F) -> BoxFuture<F::Item>
where
    F: Future<Error = cannyls::Error> + Send + 'static,
{
    Box::new(future.map_err(Error::from))
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
struct RepairContent {
    logger: Logger,
    node_id: NodeId,
    version: ObjectVersion,
    client: StorageClient,
    device: DeviceHandle,
    started_at: Instant,
    repairs_success_total: Counter,
    repairs_failure_total: Counter,
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
                    self.repairs_failure_total.increment();
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
