use cannyls;
use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::{LumpHeader, LumpId};
use fibers::time::timer::{self, Timeout};
use frugalos_mds::machine::Machine;
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::{Counter, MetricBuilder};
use slog::Logger;
use std::cmp::{self, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
use std::time::{Duration, SystemTime};

use client::storage::{GetFragment, MaybeFragment, StorageClient};
use config;
use util::Phase3;
use Error;

const MAX_TIMEOUT_SECONDS: u64 = 60;
const DELETE_CONCURRENCY: usize = 16;

type BoxFuture<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

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
    full_sync: Option<FullSync>,
}
impl Synchronizer {
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        device: DeviceHandle,
        client: StorageClient,
        repair_enabled: bool,
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
                .unwrap(),
            enqueued_delete: metric_builder
                .counter("enqueued_items")
                .label("type", "delete")
                .finish()
                .unwrap(),
            dequeued_repair: metric_builder
                .counter("dequeued_items")
                .label("type", "repair")
                .finish()
                .unwrap(),
            dequeued_delete: metric_builder
                .counter("dequeued_items")
                .label("type", "delete")
                .finish()
                .unwrap(),
            full_sync: None,
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
                Event::FullSync { ref machine } => {
                    // If FullSync is not being processed now, this event lets the synchronizer to handle one.
                    if self.full_sync.is_none() {
                        self.full_sync = Some(FullSync::new(self, machine.clone()));
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
        Self::new_with_arguments(
            &synchronizer.logger,
            synchronizer.node_id,
            &synchronizer.device,
            versions,
        )
    }
    pub fn new_with_arguments(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        versions: Vec<ObjectVersion>,
    ) -> Self {
        debug!(logger, "Starts deleting contents: versions={:?}", versions);

        let futures = versions
            .into_iter()
            .map(move |v| {
                let lump_id = config::make_lump_id(&node_id, v);
                let future = device
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
    phase: Phase3<BoxFuture<Option<LumpHeader>>, GetFragment, BoxFuture<bool>>,
}
impl RepairContent {
    pub fn new(synchronizer: &Synchronizer, version: ObjectVersion) -> Self {
        let logger = synchronizer.logger.clone();
        let device = synchronizer.device.clone();
        let node_id = synchronizer.node_id;
        let lump_id = config::make_lump_id(&node_id, version);
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
            phase,
        }
    }
}
impl Future for RepairContent {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(Error::from))? {
            let next = match phase {
                Phase3::A(Some(_)) => {
                    debug!(self.logger, "The object {:?} already exists", self.version);
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
                    return Ok(Async::Ready(()));
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

struct FullSync {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    machine: Option<Machine>,
    phase: Phase3<ListContent, CreateObjectTable, DeleteContent>,
    lump_ids: Option<Vec<LumpId>>,
}

impl FullSync {
    pub fn new(synchronizer: &Synchronizer, machine: Machine) -> Self {
        let logger = synchronizer.logger.clone();
        info!(logger, "Starts full sync");
        let node_id = synchronizer.node_id;
        let device = synchronizer.device.clone();
        let object_version_limit = machine
            .latest_version()
            .map(|x| ObjectVersion(x.version.0 + 1))
            .unwrap_or(ObjectVersion(0));
        let phase = Phase3::A(ListContent::new(synchronizer, object_version_limit));
        FullSync {
            logger,
            node_id,
            device,
            machine: Some(machine),
            phase,
            lump_ids: None,
        }
    }
    /// Returns the `ObjectVersion`s of objects that should be deleted.
    fn compute_deleted_versions(lump_ids: Vec<LumpId>, object_table: &[u64]) -> Vec<ObjectVersion> {
        let mut deleted_versions = Vec::new();
        // lump_ids are iterated over in the reversed order, i.e., in a newest-first manner.
        for lump_id in lump_ids.into_iter().rev() {
            let object_version = config::get_object_version_from_lump_id(lump_id);
            let id = object_version.0;
            let id_high = (id / 64) as usize;
            let id_low = id % 64;
            if id_high >= object_table.len() || (object_table[id_high] & 1 << id_low) == 0 {
                deleted_versions.push(object_version);
            }
        }
        deleted_versions
    }
}

impl Future for FullSync {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(Error::from))? {
            let next = match phase {
                Phase3::A(lump_ids) => {
                    self.lump_ids = Some(lump_ids);
                    let future = CreateObjectTable::new(
                        self.logger.clone(),
                        self.machine.take().expect("always Some"),
                    );
                    Phase3::B(future)
                }
                Phase3::B(object_table) => {
                    // Determine which objects need deleting
                    let deleted_versions = Self::compute_deleted_versions(
                        self.lump_ids.take().expect("always Some"),
                        &object_table,
                    );

                    debug!(self.logger, "deleted_versions = {:?}", deleted_versions);
                    let future = DeleteContent::new_with_arguments(
                        &self.logger,
                        self.node_id,
                        &self.device,
                        deleted_versions,
                    );
                    Phase3::C(future)
                }
                Phase3::C(()) => {
                    debug!(self.logger, "FullSync objects done");
                    return Ok(Async::Ready(()));
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

struct ListContent {
    logger: Logger,
    phase: BoxFuture<Vec<LumpId>>,
}

impl ListContent {
    // [0..object_version_limit) will be checked.
    pub fn new(synchronizer: &Synchronizer, object_version_limit: ObjectVersion) -> Self {
        let logger = synchronizer.logger.clone();
        let device = synchronizer.device.clone();
        let node_id = synchronizer.node_id;
        debug!(logger, "Starts listing content");
        let start_lump_id = config::make_lump_id(&node_id, ObjectVersion(0));
        let end_lump_id = config::make_lump_id(&node_id, object_version_limit);
        let phase = into_box_future(
            device
                .request()
                .deadline(Deadline::Infinity)
                .list_range(start_lump_id..end_lump_id),
        );
        ListContent { logger, phase }
    }
}

impl Future for ListContent {
    type Item = Vec<LumpId>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match track!(self.phase.poll().map_err(Error::from))? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(result) => {
                debug!(self.logger, "ListObject result = {:?}", result);
                Ok(Async::Ready(result))
            }
        }
    }
}

struct CreateObjectTable {
    logger: Logger,
    machine: Machine,
}

impl CreateObjectTable {
    pub fn new(logger: Logger, machine: Machine) -> Self {
        info!(logger, "Starts full sync");
        CreateObjectTable { logger, machine }
    }
}

impl Future for CreateObjectTable {
    type Item = Vec<u64>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = self.machine.enumerate_object_versions();
        let mut objects_count = 0;
        for &entry in &result {
            objects_count += u64::from(entry.count_ones());
        }
        debug!(
            self.logger,
            "FullSync machine_table_size = {}, machine_objects_count = {}",
            64 * result.len(),
            objects_count
        );
        Ok(Async::Ready(result))
    }
}

#[cfg(test)]
mod tests {
    use cannyls::deadline::Deadline;
    use cannyls::device::DeviceHandle;
    use cannyls::lump::LumpId;
    use client::storage::StorageClient;
    use config::make_lump_id;
    use frugalos_mds::machine::Machine;
    use libfrugalos::entity::object::{Metadata, ObjectVersion};
    use libfrugalos::expect::Expect;
    use slog::{Discard, Logger};
    use synchronizer::{CreateObjectTable, FullSync, ListContent, Synchronizer};
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    #[test]
    fn list_content_works_correctly() -> TestResult {
        let logger = Logger::root(Discard, o!());
        let mut system = System::new(2, 1)?;
        let (nodes, _client) = setup_system(&mut system, 3)?;
        let (node0, _device_id0, handle0): (_, _, DeviceHandle) = nodes[0].clone();
        let synchronizer = Synchronizer::new(
            logger,
            node0,
            handle0.clone(),
            StorageClient::Metadata,
            true,
        );
        // Puts 10 objects to device0
        for i in 0..10 {
            let lump_id = make_lump_id(&node0, ObjectVersion(i));
            let lump_data = handle0.allocate_lump_data_with_bytes(&[0; 10])?;
            handle0
                .request()
                .deadline(Deadline::Infinity)
                .put(lump_id, lump_data);
        }

        let x = wait(ListContent::new(&synchronizer, ObjectVersion(10)))?;
        assert_eq!(x.len(), 10);
        Ok(())
    }

    #[test]
    fn create_object_table_lists_objects_correctly() -> TestResult {
        let logger = Logger::root(Discard, o!());
        let mut machine = Machine::new();
        for i in 0..10 {
            let metadata = Metadata {
                version: ObjectVersion(i),
                data: vec![],
            };

            machine.put(format!("test-object-{}", i), metadata, &Expect::None)?;
        }

        let create_object_table = CreateObjectTable::new(logger, machine);
        let result = wait(create_object_table)?;
        assert_eq!(result, vec![1023]);

        Ok(())
    }

    #[test]
    fn compute_deleted_versions_works_correctly() -> TestResult {
        let lump_ids = vec![
            LumpId::new(1),
            LumpId::new(5),
            LumpId::new(8),
            LumpId::new(25),
            LumpId::new(100),
        ];
        let object_table = vec![1 << 1 | 1 << 8 | 1 << 25];
        let deleted_objects = FullSync::compute_deleted_versions(lump_ids, &object_table);
        // deleted_objects are listed in a newest-first manner.
        assert_eq!(deleted_objects, vec![ObjectVersion(100), ObjectVersion(5)]);

        Ok(())
    }
}
