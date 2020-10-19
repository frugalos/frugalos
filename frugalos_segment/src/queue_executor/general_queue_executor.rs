use cannyls::device::DeviceHandle;
use fibers::time::timer::{self, Timeout};
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::Counter;
use slog::Logger;
use std::cmp::{self, min, Reverse};
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::convert::Infallible;
use std::env;
use std::time::{Duration, SystemTime};

use crate::delete::DeleteContent;
use crate::repair::RepairPrepContent;
use crate::Error;

const MAX_TIMEOUT_SECONDS: u64 = 60;
const DELETE_CONCURRENCY: usize = 16;
const REPAIR_PREP_CONCURRENCY: usize = 100;

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
            Event::StartSegmentGc { .. } | Event::StopSegmentGc { .. } => unreachable!(),
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
    RepairPrep(RepairPrepContent),
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
                .map(|r#async| r#async.map(|()| None))),
            Task::Delete(ref mut f) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|r#async| r#async.map(|()| None))),
            Task::RepairPrep(ref mut f) => track!(f.poll()),
        }
    }
}

/// RepairPrep, Delete タスクの管理と、その処理を行う。
pub(crate) struct GeneralQueueExecutor {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    repair_prep_queue: RepairPrepQueue,
    delete_queue: DeleteQueue,
    task: Task,
    repair_candidates: BTreeSet<ObjectVersion>,
    repair_prep_concurrency: usize,
}

impl GeneralQueueExecutor {
    pub(crate) fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        enqueued_repair_prep: &Counter,
        enqueued_delete: &Counter,
        dequeued_repair_prep: &Counter,
        dequeued_delete: &Counter,
    ) -> Self {
        // TODO: 正式に conf ファイルで指定できるようにする
        let repair_prep_concurrency = env::var("FRUGALOS_REPAIR_PREP_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(REPAIR_PREP_CONCURRENCY);

        Self {
            logger: logger.clone(),
            node_id,
            device: device.clone(),
            repair_prep_queue: RepairPrepQueue::new(enqueued_repair_prep, dequeued_repair_prep),
            delete_queue: DeleteQueue::new(enqueued_delete, dequeued_delete),
            task: Task::Idle,
            repair_candidates: BTreeSet::new(),
            repair_prep_concurrency,
        }
    }
    pub(crate) fn push(&mut self, event: &Event) {
        match *event {
            Event::Putted { version, .. } => {
                self.repair_prep_queue.push(TodoItem::new(event));
                self.repair_candidates.insert(version);
            }
            Event::Deleted { version } => {
                self.repair_candidates.remove(&version);
                self.delete_queue.push(version);
            }
            Event::StartSegmentGc { .. } | Event::StopSegmentGc { .. } => {
                unreachable!();
            }
        }
    }
    /// pop を呼ぶ際には、self.Task は Task::Idle でなければならない。
    /// この関数を呼び出した場合、以下の条件に応じて挙動が変わる。
    /// 1. 待たなければいけない場合: 戻り値は None であり、self.task には Task::Wait がセットされる。
    /// 2. 待つ必要がない場合: 戻り値に次やるべきタスクか、タスクがなければ None が入る。
    /// self.task の中身は、戻り値が Some の場合は未規定で、戻り値が None の場合は Task::Idle のままである。
    /// 戻り値が Some の場合は、呼び出し側で適切な task を作り、self.task にセットする必要がある。
    /// 1. と 2. の区別は、戻り値が Some かどうかで行うこと。
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
            // NOTE: `assert_eq!(self.task, Task::Idle)`

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
            // リペアのタスクをキューに戻した場合、何もしないよりは Delete を処理した方がいいので、Delete があれば処理する。
            self.delete_queue.pop()
        } else {
            Some(item)
        }
    }
}

impl Stream for GeneralQueueExecutor {
    type Item = Vec<ObjectVersion>;
    type Error = Infallible;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut popped_versions = vec![];
        while let Async::Ready(result) = self.task.poll().unwrap_or_else(|e| {
            // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(None)
        }) {
            self.task = Task::Idle;
            if let Some(version) = result {
                popped_versions.push(version);
            }
            // REPAIR_PREP_CONCURRENCY 個ごとに repair キューに送信する
            if popped_versions.len() >= self.repair_prep_concurrency {
                return Ok(Async::Ready(Some(popped_versions)));
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
        // REPAIR_PREP_CONCURRENCY 個以下であっても、制御を返す際には popped_version を返す
        if !popped_versions.is_empty() {
            Ok(Async::Ready(Some(popped_versions)))
        } else {
            Ok(Async::NotReady)
        }
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
    fn new(enqueued_repair_prep: &Counter, dequeued_repair_prep: &Counter) -> Self {
        Self {
            queue: BinaryHeap::new(),
            enqueued: enqueued_repair_prep.clone(),
            dequeued: dequeued_repair_prep.clone(),
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
        if result.is_some() {
            self.dequeued.increment();
        }
        // Shrink if necessary
        if self.queue.capacity() > 32 && self.queue.len() < self.queue.capacity() / 2 {
            self.queue.shrink_to_fit();
        }
        result.map(|element| element.0)
    }
}

/// Delete 用のキュー。FIFO キューであり、効率のため、最大 DELETE_CONCURRENCY 個単位でまとめて pop できる。
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
    /// Delete すべきオブジェクトがない場合は None を、ある場合は数個まとめた TodoItem を返す。
    /// 返される順番は push した順番と同一である。
    fn pop(&mut self) -> Option<TodoItem> {
        // How many elements do we pick this time?
        let length = min(self.deque.len(), DELETE_CONCURRENCY);
        if length == 0 {
            return None;
        }

        let versions: Vec<ObjectVersion> = self.deque.drain(..length).collect();
        self.dequeued.add_u64(length as u64);
        if self.deque.capacity() > 32 && self.deque.len() < self.deque.capacity() / 2 {
            self.deque.shrink_to_fit();
        }
        Some(TodoItem::DeleteContent { versions })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libfrugalos::entity::object::ObjectVersion;
    use prometrics::metrics::MetricBuilder;

    #[test]
    fn delete_queue_works() {
        // 乱雑な順番のリスト
        let versions: Vec<ObjectVersion> = (0..30).rev().chain(30..65).map(ObjectVersion).collect();
        let metric_builder = MetricBuilder::new();
        let enqueued = metric_builder.counter("enqueued").finish().unwrap();
        let dequeued = metric_builder.counter("dequeued").finish().unwrap();
        let mut queue = DeleteQueue::new(&enqueued, &dequeued);
        for &version in &versions {
            queue.push(version);
        }
        let mut popped = vec![];
        while let Some(TodoItem::DeleteContent { mut versions }) = queue.pop() {
            popped.append(&mut versions);
        }
        // 突っ込んだ順番に処理される
        assert_eq!(popped, versions);
        // キューに突っ込んだ個数とキューから出した個数が等しい
        assert_eq!(enqueued.value() as usize, versions.len());
        assert_eq!(dequeued.value() as usize, versions.len());
    }
}
