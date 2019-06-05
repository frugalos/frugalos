use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use cannyls::lump::LumpId;
#[allow(deprecated)]
use fibers::sync::mpsc::sync_channel;
use fibers_tasque::{DefaultCpuTaskQueue, TaskQueueExt};
use frugalos_mds::machine::Machine;
use frugalos_raft::NodeId;
use futures::future::{loop_fn, ok};
use futures::future::{Future, Loop};
use futures::stream::Stream;
use futures::{Poll, Sink};
use libfrugalos::entity::object::ObjectVersion;
use slog::Logger;

use config;
use {Error, ErrorKind};

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

        let chunk_size = 100; // TODO to be determined
        let step = 100; // TODO to be determined
        #[allow(deprecated)]
        let (deleted_objects_tx, deleted_objects_rx) = sync_channel(chunk_size);

        let logger = logger.clone();
        let logger2 = logger.clone();
        let device = device.clone();

        let combined_future = create_object_table
            .and_then(move |object_table| {
                let list_deleted_content = make_list_content(
                    &logger,
                    &device,
                    node_id,
                    object_version_limit,
                    step,
                    deleted_objects_tx,
                    object_table,
                );
                let delete_objects_in_stream =
                    make_delete_objects_in_stream(&logger, &device, deleted_objects_rx, node_id);
                list_deleted_content
                    .join(delete_objects_in_stream)
                    .map(move |((), ())| ())
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

// [0..object_version_limit) will be checked.
pub fn make_list_content<
    S: Sink<SinkItem = ObjectVersion, SinkError = std::sync::mpsc::SendError<ObjectVersion>>
        + Send
        + 'static,
>(
    logger: &Logger,
    device: &DeviceHandle,
    node_id: NodeId,
    object_version_limit: ObjectVersion,
    step: u64,
    sink: S,
    object_table: Vec<u64>,
) -> impl Future<Item = (), Error = Error> + Send {
    let logger = logger.clone();
    let device = device.clone();
    debug!(logger, "Starts listing content");
    let sink = sink.sink_map_err(|_| Error::from(ErrorKind::Other));
    loop_fn(
        (ObjectVersion(0), sink, object_table),
        move |(current_version, sink, object_table)| {
            let start_lump_id = config::make_lump_id(&node_id, current_version);
            let end_object_version = std::cmp::min(
                ObjectVersion(current_version.0 + step),
                object_version_limit,
            );
            let end_lump_id = config::make_lump_id(&node_id, end_object_version);
            device
                .request()
                .deadline(Deadline::Infinity)
                .list_range(start_lump_id..end_lump_id)
                .map_err(From::from)
                .and_then(move |lump_ids| {
                    let deleted_objects =
                        FullSync::compute_deleted_versions(lump_ids, &object_table);
                    loop_fn((sink, deleted_objects), move |(sink, mut objects)| {
                        if let Some(object) = objects.pop() {
                            Box::new(
                                sink.send(object)
                                    .map(|sink| Loop::Continue((sink, objects))),
                            )
                                as Box<dyn Future<Item = _, Error = _> + Send>
                        } else {
                            Box::new(ok(Loop::Break(sink)))
                                as Box<dyn Future<Item = _, Error = _> + Send>
                        }
                    })
                    .map(|sink| (sink, object_table))
                })
                .and_then(move |(sink, object_table)| {
                    let next = ObjectVersion(current_version.0 + step);
                    if next >= object_version_limit {
                        Ok(Loop::Break(()))
                    } else {
                        Ok(Loop::Continue((next, sink, object_table)))
                    }
                })
                .map_err(From::from)
        },
    )
}

/// Creates a future that asynchronously deletes objects enumerated by stream.
fn make_delete_objects_in_stream<S: Stream<Item = ObjectVersion, Error = ()> + Send + 'static>(
    logger: &Logger,
    device: &DeviceHandle,
    stream: S,
    node_id: NodeId,
) -> impl Future<Item = (), Error = Error> + Send {
    let logger = logger.clone();
    let device = device.clone();
    stream
        .map_err(|()| Error::from(ErrorKind::Other))
        .for_each(move |object_version| {
            let lump_id = config::make_lump_id(&node_id, object_version);
            info!(logger, "Repair: deleting {:?}", lump_id);
            device
                .request()
                .deadline(Deadline::Infinity)
                .delete(lump_id)
                .map(|_result| ())
                .map_err(From::from)
        })
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
    use config::make_lump_id;
    use frugalos_mds::machine::Machine;
    use full_sync::{make_create_object_table, make_list_content, FullSync};
    use futures::stream::Stream;
    use libfrugalos::entity::object::{Metadata, ObjectVersion};
    use libfrugalos::expect::Expect;
    use slog::{Discard, Logger};
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    #[test]
    fn list_content_works_correctly() -> TestResult {
        let logger = Logger::root(Discard, o!());
        let mut system = System::new(2, 1)?;
        let (nodes, _client) = setup_system(&mut system, 3)?;
        let (node0, _device_id0, handle0): (_, _, DeviceHandle) = nodes[0].clone();

        // Puts 10 objects to device0
        for i in 0..10 {
            let lump_id = make_lump_id(&node0, ObjectVersion(i));
            let lump_data = handle0.allocate_lump_data_with_bytes(&[0; 10])?;
            handle0
                .request()
                .deadline(Deadline::Infinity)
                .put(lump_id, lump_data);
        }

        let object_table = vec![0];

        #[allow(deprecated)]
        let (sink, source) = fibers::sync::mpsc::sync_channel(100);

        wait(make_list_content(
            &logger,
            &handle0,
            node0,
            ObjectVersion(10),
            1,
            sink,
            object_table,
        ))?;
        let array: Vec<_> = source.wait().collect();
        assert_eq!(array.len(), 10);
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
