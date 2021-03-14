use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpId;
use fibers_tasque::{DefaultCpuTaskQueue, TaskQueueExt};
use frugalos_mds::StartSegmentGcReply;
use frugalos_raft::NodeId;
use futures::future::{join_all, loop_fn, ok, Either};
use futures::future::{Future, Loop};
use futures::Poll;
use libfrugalos::entity::object::ObjectVersion;
use prometrics::metrics::{Counter, Gauge, MetricBuilder};
use slog::Logger;

use crate::Error;
use crate::{config, ErrorKind};
use trackable::error::ErrorKindExt;

#[derive(Clone)]
pub(crate) struct SegmentGcMetrics {
    segment_gc_count: Counter,
    segment_gc_deleted_objects: Counter,
    // How many objects have to be swept before segment_gc is completed (including non-existent ones)
    // TODO: 多数のノードがあっても正しく動くように、変更に set 以外を使う
    segment_gc_remaining: Gauge,
    // How many jobs were aborted till now.
    pub(crate) segment_gc_aborted: Counter,
}

impl SegmentGcMetrics {
    pub(crate) fn new(metric_builder: &MetricBuilder) -> Self {
        SegmentGcMetrics {
            segment_gc_count: metric_builder
                .counter("segment_gc_count")
                .finish()
                .expect("metric should be well-formed"),
            segment_gc_deleted_objects: metric_builder
                .counter("segment_gc_deleted_objects")
                .finish()
                .expect("metric should be well-formed"),
            segment_gc_remaining: metric_builder
                .gauge("segment_gc_remaining")
                .finish()
                .expect("metric should be well-formed"),
            segment_gc_aborted: metric_builder
                .counter("segment_gc_aborted")
                .finish()
                .expect("metric should be well-formed"),
        }
    }
    pub(crate) fn reset(&self) {
        self.segment_gc_remaining.set(0.0);
    }
}

pub(crate) struct SegmentGc {
    future: Box<dyn Future<Item = (), Error = Error> + Send + 'static>,
}

impl SegmentGc {
    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    pub fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        object_versions: Vec<ObjectVersion>,
        object_version_limit: ObjectVersion,
        segment_gc_metrics: SegmentGcMetrics,
        segment_gc_step: u64,
        tx: StartSegmentGcReply,
    ) -> Self {
        let logger = logger.clone();
        info!(logger, "Starts segment_gc");
        segment_gc_metrics.segment_gc_count.increment();
        let create_object_table = make_create_object_table(logger.clone(), object_versions);

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
                    segment_gc_step,
                    object_table,
                    segment_gc_metrics.segment_gc_deleted_objects,
                    segment_gc_metrics.segment_gc_remaining,
                )
            })
            .then(move |result| {
                match &result {
                    Ok(()) => {
                        info!(logger2, "SegmentGc objects done");
                        tx.exit(Ok(()));
                    }
                    Err(e) => {
                        tx.exit(Err(Box::new(e.clone())));
                    }
                }
                futures::future::done(result)
            });

        let future = Box::new(combined_future);
        SegmentGc { future }
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

impl Future for SegmentGc {
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
    segment_gc_deleted_objects: Counter,
    segment_gc_remaining: Gauge,
) -> impl Future<Item = (), Error = Error> + Send {
    if step == 0 {
        return Either::A(futures::future::err(
            ErrorKind::Invalid.cause("step should be > 0").into(),
        ));
    }
    let logger = logger.clone();
    let device = device.clone();
    info!(
        logger,
        "Starts listing and deleting content, object_version_limit = {:?}, step = {}",
        object_version_limit,
        step
    );
    let returned_future = loop_fn(
        (ObjectVersion(0), object_table),
        move |(current_version, object_table)| {
            if current_version >= object_version_limit {
                // We're done. Stop processing.
                segment_gc_remaining.set(0.0);
                return Either::A(ok(Loop::Break(())));
            }
            segment_gc_remaining.set((object_version_limit.0 - current_version.0) as f64);
            let segment_gc_deleted_objects = segment_gc_deleted_objects.clone();
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
                        SegmentGc::compute_deleted_versions(lump_ids, &object_table);
                    let delete_future = make_delete_objects(
                        deleted_objects,
                        &device_cloned,
                        node_id,
                        &segment_gc_deleted_objects,
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
    );
    Either::B(returned_future)
}

pub(self) fn make_create_object_table(
    logger: Logger,
    object_versions: Vec<ObjectVersion>,
) -> impl Future<Item = ObjectTable, Error = Error> + Send {
    debug!(logger, "Starts segment_gc");
    DefaultCpuTaskQueue
        .async_call(move || get_object_table(&logger, object_versions))
        .map_err(From::from)
}

fn get_object_table(logger: &Logger, mut versions: Vec<ObjectVersion>) -> ObjectTable {
    versions.sort_unstable();
    let objects_count = versions.len();
    debug!(
        logger,
        "SegmentGc machine_objects_count = {}", objects_count
    );
    ObjectTable(versions)
}

/// Creates a future that deletes all given objects.
fn make_delete_objects(
    deleted_objects: Vec<ObjectVersion>,
    device: &DeviceHandle,
    node_id: NodeId,
    segment_gc_deleted_objects: &Counter,
) -> impl Future<Item = (), Error = cannyls::Error> {
    let segment_gc_deleted_objects = segment_gc_deleted_objects.clone();
    let futures: Vec<_> = deleted_objects
        .into_iter()
        .map(|object| {
            let segment_gc_deleted_objects = segment_gc_deleted_objects.clone();
            let lump_id = config::make_lump_id(&node_id, object);
            device
                .request()
                .deadline(Deadline::Infinity)
                .delete(lump_id)
                .then(move |result| {
                    if let Ok(true) = result {
                        segment_gc_deleted_objects.increment();
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
    use crate::segment_gc::{
        make_create_object_table, make_list_and_delete_content, ObjectTable, SegmentGc,
    };
    use crate::test_util::tests::{setup_system, wait, System};
    use cannyls::deadline::Deadline;
    use cannyls::device::DeviceHandle;
    use cannyls::lump::LumpId;
    use futures::future::Future;
    use libfrugalos::entity::object::ObjectVersion;
    use slog::{Discard, Logger};
    use std::{thread, time};
    use trackable::result::TestResult;

    use crate::config::make_lump_id;
    use fibers::executor::Executor;
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
        let segment_gc_deleted_objects =
            Counter::new("segment_gc_deleted_objects").expect("Never fails");
        let segment_gc_remaining = Gauge::new("segment_gc_remaining").expect("Never fails");

        wait(make_list_and_delete_content(
            &logger,
            &handle0,
            node0,
            ObjectVersion(10),
            1,
            object_table,
            segment_gc_deleted_objects.clone(),
            segment_gc_remaining,
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
        assert_eq!(segment_gc_deleted_objects.value() as u64, 10);
        Ok(())
    }

    #[test]
    fn create_object_table_lists_objects_correctly() -> TestResult {
        let logger = Logger::root(Discard, o!());
        let mut object_versions = Vec::new();
        for i in 0..10 {
            object_versions.push(ObjectVersion(i));
        }

        let create_object_table = make_create_object_table(logger, object_versions);
        let ObjectTable(result) = wait(create_object_table)?;
        assert_eq!(result.len(), 10);
        for (i, object_version) in result.into_iter().enumerate() {
            assert_eq!(object_version, ObjectVersion(i as u64));
        }

        Ok(())
    }

    #[test]
    fn compute_deleted_versions_works_correctly() {
        let lump_ids = vec![
            LumpId::new(1),
            LumpId::new(5),
            LumpId::new(8),
            LumpId::new(25),
            LumpId::new(100),
        ];
        let object_table = ObjectTable(vec![ObjectVersion(1), ObjectVersion(8), ObjectVersion(25)]);
        let deleted_objects = SegmentGc::compute_deleted_versions(lump_ids, &object_table);
        // deleted_objects are listed in a newest-first manner.
        assert_eq!(deleted_objects, vec![ObjectVersion(100), ObjectVersion(5)]);
    }
}
