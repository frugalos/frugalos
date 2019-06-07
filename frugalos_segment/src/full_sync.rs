use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpId;
use fibers_tasque::{DefaultCpuTaskQueue, TaskQueueExt};
use frugalos_mds::machine::Machine;
use frugalos_raft::NodeId;
use futures::future::{loop_fn, ok};
use futures::future::{Future, Loop};
use futures::Poll;
use libfrugalos::entity::object::ObjectVersion;
use slog::Logger;

use config;
use Error;

pub(crate) struct FullSync {
    future: Box<Future<Item = (), Error = Error> + Send + 'static>,
}

impl FullSync {
    pub fn new(logger: &Logger, node_id: NodeId, device: &DeviceHandle, machine: Machine) -> Self {
        let logger = logger.clone();
        info!(logger, "Starts full sync");
        let object_version_limit = machine
            .latest_version()
            .map(|x| ObjectVersion(x.version.0 + 1))
            .unwrap_or(ObjectVersion(0));
        let create_object_table = make_create_object_table(logger.clone(), machine);

        let step = 100; // TODO to be determined

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
                    step,
                    object_table,
                )
            })
            .map(move |()| debug!(logger2, "FullSync objects done"));

        let future = Box::new(combined_future);
        FullSync { future }
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
        track!(self.future.poll())
    }
}

pub fn make_list_and_delete_content(
    logger: &Logger,
    device: &DeviceHandle,
    node_id: NodeId,
    object_version_limit: ObjectVersion,
    step: u64,
    object_table: Vec<u64>,
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
            let start_lump_id = config::make_lump_id(&node_id, current_version);
            let end_object_version = std::cmp::min(
                ObjectVersion(current_version.0 + step),
                object_version_limit,
            );
            let end_lump_id = config::make_lump_id(&node_id, end_object_version);
            let device_cloned = device.clone();
            device
                .request()
                .deadline(Deadline::Infinity)
                .list_range(start_lump_id..end_lump_id)
                .and_then(move |lump_ids| {
                    let deleted_objects =
                        FullSync::compute_deleted_versions(lump_ids, &object_table);
                    loop_fn(deleted_objects, move |mut objects| {
                        if let Some(object) = objects.pop() {
                            let lump_id = config::make_lump_id(&node_id, object);
                            Box::new(
                                device_cloned
                                    .request()
                                    .deadline(Deadline::Infinity)
                                    .delete(lump_id)
                                    .map(move |_result| Loop::Continue(objects))
                                    .map_err(From::from),
                            )
                                as Box<dyn Future<Item = Loop<_, _>, Error = _> + Send>
                        } else {
                            Box::new(ok(Loop::Break(())))
                                as Box<dyn Future<Item = Loop<_, _>, Error = _> + Send>
                        }
                    })
                    .map(move |()| object_table)
                })
                .and_then(move |object_table| {
                    let next = ObjectVersion(current_version.0 + step);
                    if next >= object_version_limit {
                        Ok(Loop::Break(()))
                    } else {
                        Ok(Loop::Continue((next, object_table)))
                    }
                })
                .map_err(From::from)
        },
    )
}

pub fn make_create_object_table(
    logger: Logger,
    machine: Machine,
) -> impl Future<Item = Vec<u64>, Error = Error> + Send {
    debug!(logger, "Starts full sync");
    DefaultCpuTaskQueue
        .async_call(move || get_object_table(&logger, &machine))
        .map_err(From::from)
}

fn get_object_table(logger: &Logger, machine: &Machine) -> Vec<u64> {
    let result = machine.enumerate_object_versions();
    let mut objects_count = 0;
    for &entry in &result {
        objects_count += u64::from(entry.count_ones());
    }
    debug!(
        logger,
        "FullSync machine_table_size = {}, machine_objects_count = {}",
        64 * result.len(),
        objects_count
    );
    result
}

#[cfg(test)]
mod tests {
    use cannyls::deadline::Deadline;
    use cannyls::device::DeviceHandle;
    use cannyls::lump::LumpId;
    use full_sync::{make_create_object_table, make_list_and_delete_content, FullSync};
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

        let object_table = vec![0];

        wait(make_list_and_delete_content(
            &logger,
            &handle0,
            node0,
            ObjectVersion(10),
            1,
            object_table,
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
