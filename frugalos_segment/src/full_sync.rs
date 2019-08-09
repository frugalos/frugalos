use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpId;
use fibers_tasque::{DefaultCpuTaskQueue, TaskQueueExt};
use frugalos_mds::machine::Machine;
use frugalos_raft::NodeId;
use futures::future::{join_all, loop_fn, ok, Either};
use futures::future::{Future, Loop};
use futures::Poll;
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::{Counter, Gauge, MetricBuilder};
use slog::Logger;

use config;
use Error;

#[derive(Clone)]
pub(crate) struct FullSyncMetrics {
    full_sync_count: Counter,
    full_sync_deleted_objects: Counter,
    // How many objects have to be swept before full_sync is completed (including non-existent ones)
    full_sync_remaining: Gauge,
}

impl FullSyncMetrics {
    pub(crate) fn new(metric_builder: &MetricBuilder) -> Self {
        FullSyncMetrics {
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
        }
    }
    pub(crate) fn reset(&self) {
        self.full_sync_remaining.set(0.0);
    }
}

pub(crate) struct FullSync {
    future: Box<dyn Future<Item = (), Error = Error> + Send + 'static>,
}

impl FullSync {
    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    pub fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        machine: Machine,
        object_version_limit: ObjectVersion,
        full_sync_metrics: FullSyncMetrics,
        full_sync_step: u64,
    ) -> Self {
        let logger = logger.clone();
        info!(logger, "Starts full sync");
        full_sync_metrics.full_sync_count.increment();
        let create_object_table = make_create_object_table(logger.clone(), machine);

        let logger = logger.clone();
        let logger2 = logger.clone();
        let device = device.clone();

        let combined_future = create_object_table
            .and_then(move |object_table| {
                make_list_and_delete_content(
                    &logger,
                    &device,
                    node_id,
                    object_version_limit,
                    full_sync_step,
                    object_table,
                    full_sync_metrics.full_sync_deleted_objects,
                    full_sync_metrics.full_sync_remaining,
                )
            })
            .map(move |()| info!(logger2, "FullSync objects done"));

        let future = Box::new(combined_future);
        FullSync { future }
    }
    /// Returns the `ObjectVersion`s of objects that should be deleted.
    fn compute_deleted_versions(
        lump_ids: Vec<LumpId>,
        object_table: &ObjectTable,
    ) -> Vec<ObjectVersion> {
        let mut deleted_versions = Vec::new();
        // lump_ids are iterated over in the reversed order, i.e., in a newest-first manner.
        for lump_id in lump_ids.into_iter().rev() {
            let object_version = config::get_object_version_from_lump_id(lump_id);
            if !object_table.has_object(object_version) {
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
        track!(self.future.poll())
    }
}

/// Iteratively lists and deletes unnecessary objects that are stored in storage but not in machine.
#[allow(clippy::too_many_arguments)]
pub(self) fn make_list_and_delete_content(
    logger: &Logger,
    device: &DeviceHandle,
    node_id: NodeId,
    object_version_limit: ObjectVersion,
    step: u64,
    object_table: ObjectTable,
    full_sync_deleted_objects: Counter,
    full_sync_remaining: Gauge,
) -> impl Future<Item = (), Error = Error> + Send {
    assert!(step > 0);
    let logger = logger.clone();
    let device = device.clone();
    info!(
        logger,
        "Starts listing and deleting content, object_version_limit = {:?}, step = {}",
        object_version_limit,
        step
    );
    loop_fn(
        (ObjectVersion(0), object_table),
        move |(current_version, object_table)| {
            if current_version >= object_version_limit {
                // We're done. Stop processing.
                full_sync_remaining.set(0.0);
                return Either::A(ok(Loop::Break(())));
            }
            full_sync_remaining.set((object_version_limit.0 - current_version.0) as f64);
            let full_sync_deleted_objects = full_sync_deleted_objects.clone();
            let start_lump_id = config::make_lump_id(&node_id, current_version);
            let end_object_version = std::cmp::min(
                ObjectVersion(current_version.0 + step),
                object_version_limit,
            );
            let end_lump_id = config::make_lump_id(&node_id, end_object_version);
            let device_cloned = device.clone();
            let future = device
                .request()
                .deadline(Deadline::Infinity)
                .list_range(start_lump_id..end_lump_id)
                .and_then(move |lump_ids| {
                    let deleted_objects =
                        FullSync::compute_deleted_versions(lump_ids, &object_table);
                    let delete_future = make_delete_objects(
                        deleted_objects,
                        &device_cloned,
                        node_id,
                        &full_sync_deleted_objects,
                    );
                    delete_future.map(move |()| object_table)
                })
                .map(move |object_table| {
                    let next = ObjectVersion(current_version.0 + step);
                    Loop::Continue((next, object_table))
                })
                .map_err(From::from);
            Either::B(future)
        },
    )
}

pub(self) fn make_create_object_table(
    logger: Logger,
    machine: Machine,
) -> impl Future<Item = ObjectTable, Error = Error> + Send {
    debug!(logger, "Starts full sync");
    DefaultCpuTaskQueue
        .async_call(move || get_object_table(&logger, &machine))
        .map_err(From::from)
}

fn get_object_table(logger: &Logger, machine: &Machine) -> ObjectTable {
    let mut versions = machine.to_versions();
    versions.sort_unstable();
    let objects_count = versions.len();
    debug!(logger, "FullSync machine_objects_count = {}", objects_count);
    ObjectTable(versions)
}

/// Creates a future that deletes all given objects.
fn make_delete_objects(
    deleted_objects: Vec<ObjectVersion>,
    device: &DeviceHandle,
    node_id: NodeId,
    full_sync_deleted_objects: &Counter,
) -> impl Future<Item = (), Error = cannyls::Error> {
    let full_sync_deleted_objects = full_sync_deleted_objects.clone();
    let futures: Vec<_> = deleted_objects
        .into_iter()
        .map(|object| {
            let full_sync_deleted_objects = full_sync_deleted_objects.clone();
            let lump_id = config::make_lump_id(&node_id, object);
            device
                .request()
                .deadline(Deadline::Infinity)
                .delete(lump_id)
                .then(move |result| {
                    if let Ok(true) = result {
                        full_sync_deleted_objects.increment();
                    }
                    // Ignores all errors that occur in deletion
                    ok(())
                })
        })
        .collect();
    join_all(futures).map(|_| ())
}

/// A type representing a set of objects.
/// Currently this struct holds a sorted Vec<ObjectVersion>.
/// This type can change in the future. https://github.com/frugalos/frugalos/pull/166#discussion_r291900772
pub(self) struct ObjectTable(Vec<ObjectVersion>);

impl ObjectTable {
    fn has_object(&self, object_version: ObjectVersion) -> bool {
        self.0.binary_search(&object_version).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use cannyls::deadline::Deadline;
    use cannyls::device::DeviceHandle;
    use cannyls::lump::LumpId;
    use full_sync::{
        make_create_object_table, make_list_and_delete_content, FullSync, ObjectTable,
    };
    use futures::future::Future;
    use libfrugalos::entity::object::{Metadata, ObjectVersion};
    use libfrugalos::expect::Expect;
    use slog::{Discard, Logger};
    use std::{thread, time};
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    use config::make_lump_id;
    use fibers::executor::Executor;
    use frugalos_mds::machine::Machine;
    use prometrics::metrics::{Counter, Gauge};

    #[test]
    fn list_and_delete_content_works_correctly() -> TestResult {
        let logger = Logger::root(Discard, o!());
        let mut system = System::new(2, 1)?;
        let (nodes, _client) = setup_system(&mut system, 3)?;
        let (node0, _device_id0, handle0): (_, _, DeviceHandle) = nodes[0].clone();

        thread::spawn(move || loop {
            system.executor.run_once().unwrap();
            thread::sleep(time::Duration::from_micros(100));
        });

        // Puts 10 objects to device0
        for i in 0..10 {
            let lump_id = make_lump_id(&node0, ObjectVersion(i));
            let lump_data = handle0.allocate_lump_data_with_bytes(&[0; 10])?;
            wait(
                handle0
                    .request()
                    .deadline(Deadline::Immediate)
                    .put(lump_id, lump_data)
                    .map_err(From::from),
            )?;
        }

        let object_table = ObjectTable(vec![]);
        let full_sync_deleted_objects =
            Counter::new("full_sync_deleted_objects").expect("Never fails");
        let full_sync_remaining = Gauge::new("full_sync_remaining").expect("Never fails");

        wait(make_list_and_delete_content(
            &logger,
            &handle0,
            node0,
            ObjectVersion(10),
            1,
            object_table,
            full_sync_deleted_objects.clone(),
            full_sync_remaining.clone(),
        ))?;

        // Assert objects are deleted and therefore not found
        for i in 0..10 {
            let lump_id = make_lump_id(&node0, ObjectVersion(i));
            assert!(!wait(
                handle0
                    .request()
                    .deadline(Deadline::Immediate)
                    .delete(lump_id)
                    .map_err(From::from)
            )?);
        }

        // Assert 10 objects are deleted
        assert_eq!(full_sync_deleted_objects.value() as u64, 10);
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

        let create_object_table = make_create_object_table(logger, machine);
        let ObjectTable(result) = wait(create_object_table)?;
        assert_eq!(result.len(), 10);
        for (i, object_version) in result.into_iter().enumerate() {
            assert_eq!(object_version, ObjectVersion(i as u64));
        }

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
        let object_table = ObjectTable(vec![ObjectVersion(1), ObjectVersion(8), ObjectVersion(25)]);
        let deleted_objects = FullSync::compute_deleted_versions(lump_ids, &object_table);
        // deleted_objects are listed in a newest-first manner.
        assert_eq!(deleted_objects, vec![ObjectVersion(100), ObjectVersion(5)]);

        Ok(())
    }
}
