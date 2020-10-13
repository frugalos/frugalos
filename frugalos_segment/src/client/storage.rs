#![allow(clippy::needless_pass_by_value)]
use adler32;
use byteorder::{BigEndian, ByteOrder};
use cannyls::deadline::Deadline;
use cannyls::lump::LumpId;
use cannyls::storage::StorageUsage;
use cannyls_rpc::DeviceId;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use fibers_tasque::DefaultCpuTaskQueue;
use frugalos_raft::NodeId;
use futures::future;
use futures::{self, Async, Future, Poll};
use libfrugalos::entity::object::{FragmentsSummary, ObjectVersion};
use rustracing_jaeger::span::SpanHandle;
use slog::Logger;
use std::env;
use std::thread;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use client::dispersed_storage::{DispersedClient, ReconstructDispersedFragment};
use client::ec::ErasureCoder;
use client::replicated_storage::{GetReplicatedFragment, ReplicatedClient};
use config::ClientConfig;
use metrics::{DispersedClientMetrics, PutAllMetrics, ReplicatedClientMetrics};
use util::BoxFuture;
use {Error, ErrorKind, ObjectValue, Result};

#[derive(Clone)]
pub enum StorageClient {
    Metadata,
    Replicated(ReplicatedClient),
    Dispersed(DispersedClient),
}
impl StorageClient {
    pub fn new(
        logger: Logger,
        config: ClientConfig,
        rpc_service: RpcServiceHandle,
        ec: Option<ErasureCoder>,
    ) -> Result<Self> {
        use config::Storage;
        match config.storage {
            Storage::Metadata => Ok(StorageClient::Metadata),
            Storage::Replicated(c) => {
                let metrics = track!(ReplicatedClientMetrics::new())?;
                Ok(StorageClient::Replicated(ReplicatedClient::new(
                    metrics,
                    config.cluster,
                    c,
                    config.replicated_client,
                    rpc_service,
                )))
            }
            Storage::Dispersed(c) => {
                let metrics = track!(DispersedClientMetrics::new())?;
                Ok(StorageClient::Dispersed(DispersedClient::new(
                    logger,
                    metrics,
                    config.cluster,
                    c,
                    config.dispersed_client,
                    rpc_service,
                    ec,
                )))
            }
        }
    }
    pub fn is_metadata(&self) -> bool {
        matches!(*self, StorageClient::Metadata)
    }
    pub fn storage_usage(self, parent: SpanHandle) -> BoxFuture<Vec<StorageUsage>> {
        match self {
            StorageClient::Metadata => Box::new(futures::finished(Vec::new())),
            StorageClient::Replicated(c) => c.storage_usage(),
            StorageClient::Dispersed(c) => c.storage_usage(parent),
        }
    }
    pub fn get_fragment(self, local_node: NodeId, version: ObjectVersion) -> GetFragment {
        match self {
            StorageClient::Metadata => GetFragment::Failed(futures::failed(
                ErrorKind::Other.cause("unreachable").into(),
            )),
            StorageClient::Replicated(c) => {
                GetFragment::Replicated(c.get_fragment(local_node, version))
            }
            StorageClient::Dispersed(c) => {
                GetFragment::Dispersed(c.get_fragment(local_node, version))
            }
        }
    }
    pub fn get(
        self,
        object: ObjectValue,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<Vec<u8>> {
        match self {
            StorageClient::Metadata => Box::new(futures::finished(object.content)),
            StorageClient::Replicated(c) => c.get(object.version, deadline),
            StorageClient::Dispersed(c) => c.get(object.version, deadline, parent),
        }
    }
    pub fn head(
        self,
        version: ObjectVersion,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<()> {
        match self {
            StorageClient::Metadata => Box::new(future::ok(())),
            StorageClient::Replicated(c) => c.head(version, deadline),
            StorageClient::Dispersed(c) => c.head(version, deadline, parent),
        }
    }
    pub fn count_fragments(
        self,
        version: ObjectVersion,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<FragmentsSummary> {
        match self {
            StorageClient::Metadata => Box::new(future::ok(FragmentsSummary {
                is_corrupted: false,
                found_total: 0,
                lost_total: 0,
            })),
            StorageClient::Replicated(c) => c.count_fragments(version, deadline),
            StorageClient::Dispersed(c) => c.count_fragments(version, deadline, parent),
        }
    }
    pub fn put(
        self,
        version: ObjectVersion,
        content: Vec<u8>,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<()> {
        match self {
            StorageClient::Metadata => Box::new(futures::finished(())),
            StorageClient::Replicated(c) => c.put(version, content, deadline),
            StorageClient::Dispersed(c) => c.put(version, content, deadline, parent),
        }
    }
    pub fn delete_fragment(
        self,
        version: ObjectVersion,
        deadline: Deadline,
        parent: SpanHandle,
        index: usize,
    ) -> BoxFuture<Option<(bool, DeviceId, LumpId)>> {
        match self {
            StorageClient::Metadata => Box::new(future::ok(None)),
            StorageClient::Replicated(c) => c.delete_fragment(version, deadline, index),
            StorageClient::Dispersed(c) => c.delete_fragment(version, deadline, parent, index),
        }
    }
}

pub struct PutAll {
    metrics: PutAllMetrics,
    future: future::SelectAll<BoxFuture<()>>,
    ok_count: usize,
    required_ok_count: usize,
}
impl PutAll {
    pub fn new<I>(metrics: PutAllMetrics, futures: I, required_ok_count: usize) -> Result<Self>
    where
        I: Iterator<Item = BoxFuture<()>>,
    {
        let (_, upper_bound) = futures.size_hint();
        let len = track!(upper_bound.ok_or_else(
            || ErrorKind::Invalid.cause("The upper bound of the given futures is unknown.")
        ))?;
        if len < required_ok_count {
            let e = ErrorKind::Invalid.cause(format!("The length of the given futures is too short:  required_ok_count={}, futures.len={}", required_ok_count, len));
            return Err(track!(Error::from(e)));
        }
        let future = future::select_all(futures);
        Ok(PutAll {
            metrics,
            future,
            ok_count: 0,
            required_ok_count,
        })
    }
}
impl Future for PutAll {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let remainings = match self.future.poll() {
                Err((e, _, remainings)) => {
                    self.metrics.lost_fragments_total.increment();
                    // If completing required_ok_count futures becomes impossible,
                    // immediately stops execution.
                    if remainings.len() + self.ok_count < self.required_ok_count {
                        self.metrics.failures_total.increment();
                        return Err(track!(e));
                    }
                    remainings
                }
                Ok(Async::Ready(((), _, remainings))) => {
                    self.ok_count += 1;
                    if self.ok_count >= self.required_ok_count {
                        // Finish all remaining futures. This future itself will never fail.
                        enum Void {}; // TODO: use never type after it is stabilized
                        let lost_fragments_total = self.metrics.lost_fragments_total.clone();
                        let mut remaining =
                            future::join_all(remainings.into_iter().map(move |future| {
                                let lost_fragments_total = lost_fragments_total.clone();
                                future.then(move |result| {
                                    // ignore errors, just count
                                    match result {
                                        Ok(()) => (),
                                        Err(_) => lost_fragments_total.increment(),
                                    }
                                    future::ok::<(), Void>(())
                                })
                            }));

                        // Because TaskQueue accepts a closure (FnOnce()), convert the future into a closure.
                        let poll_frequency_millis =
                            env::var("FRUGALOS_PUT_ALL_POLL_FREQUENCY_MILLIS")
                                .ok()
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(100); // defaults to 100ms
                        let waiting_time = Duration::from_millis(poll_frequency_millis);
                        let doer = move || loop {
                            match remaining.poll() {
                                Ok(Async::Ready(_)) => return,
                                Ok(Async::NotReady) => (),
                                Err(_) => unreachable!(),
                            }
                            thread::sleep(waiting_time);
                        };

                        // Ask DefaultCpuTaskQueue to do it.
                        DefaultCpuTaskQueue.get().enqueue(doer);
                        return Ok(Async::Ready(()));
                    }
                    remainings
                }
                Ok(Async::NotReady) => break,
            };
            if remainings.is_empty() {
                return Ok(Async::Ready(()));
            }
            self.future = future::select_all(remainings);
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug, PartialEq, Eq)]
/// This enum represents the result of `GetFragment`.
pub enum MaybeFragment {
    /// Successfully get a content.
    Fragment(Vec<u8>),

    /// It's not responsible for storing a fragment.
    NotParticipant,
}

#[allow(clippy::large_enum_variant)]
pub enum GetFragment {
    Failed(future::Failed<Vec<u8>, Error>),
    Replicated(GetReplicatedFragment),
    Dispersed(ReconstructDispersedFragment),
}
impl Future for GetFragment {
    type Item = MaybeFragment;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            GetFragment::Failed(ref mut f) => {
                track!(f.poll().map(|content| content.map(MaybeFragment::Fragment)))
            }
            GetFragment::Replicated(ref mut f) => {
                track!(f.poll().map(|content| content.map(MaybeFragment::Fragment)))
            }
            GetFragment::Dispersed(ref mut f) => track!(f.poll()),
        }
    }
}

pub(crate) fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = adler32::adler32(&bytes[..]).expect("Never fails");
    let mut trailer = [0; 5]; // TODO: フォーマットを文書化
    BigEndian::write_u32(&mut trailer[..], checksum);
    bytes.extend_from_slice(&trailer[..]);
}

pub(crate) fn verify_and_remove_checksum(bytes: &mut Vec<u8>) -> Result<()> {
    track_assert!(bytes.len() >= 5, ErrorKind::Invalid);
    let split_pos = bytes.len() - 5;

    let checksum = adler32::adler32(&bytes[..split_pos]).expect("Never fails");
    let expected = BigEndian::read_u32(&bytes[split_pos..]);
    track_assert_eq!(checksum, expected, ErrorKind::Invalid);

    bytes.truncate(split_pos);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{ClusterConfig, ClusterMember};
    use futures::future::poll_fn;
    use rustracing_jaeger::Span;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    fn candidate_position(
        config: &ClusterConfig,
        member: ClusterMember,
        version: ObjectVersion,
    ) -> usize {
        config
            .candidates(version)
            .position(|candidate| *candidate == member)
            .unwrap()
    }

    #[test]
    fn put_all_new_works() -> TestResult {
        let metrics = track!(PutAllMetrics::new("test_client"))?;
        let futures: Vec<BoxFuture<_>> = vec![];
        assert!(PutAll::new(metrics.clone(), futures.into_iter(), 2).is_err());

        let futures: Vec<BoxFuture<_>> = vec![Box::new(futures::future::ok(()))];
        assert!(PutAll::new(metrics.clone(), futures.into_iter(), 2).is_err());

        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::ok(())),
            Box::new(futures::future::ok(())),
        ];
        let put = track!(PutAll::new(metrics.clone(), futures.into_iter(), 2))?;
        assert!(wait(put).is_ok());

        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::ok(())),
            Box::new(futures::future::ok(())),
            Box::new(futures::future::ok(())),
        ];
        let put = track!(PutAll::new(metrics, futures.into_iter(), 2))?;
        assert!(wait(put).is_ok());

        Ok(())
    }

    #[test]
    fn put_all_fails_correctly() -> TestResult {
        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::ok(())),
            Box::new(futures::future::err(ErrorKind::Other.into())),
        ];
        let metrics = track!(PutAllMetrics::new("test_client"))?;
        let put = track!(PutAll::new(metrics, futures.into_iter(), 2))?;
        assert!(wait(put).is_err());
        Ok(())
    }

    #[test]
    fn put_all_fails_even_if_last_operation_succeeds() -> TestResult {
        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::ok(())),
        ];
        let metrics = track!(PutAllMetrics::new("test_client"))?;
        let put = track!(PutAll::new(metrics, futures.into_iter(), 2))?;
        assert!(wait(put).is_err());
        Ok(())
    }

    #[test]
    fn it_puts_data_correctly() -> TestResult {
        let data_fragments = 4;
        let parity_fragments = 1;
        let cluster_size = 5;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (_members, client) = setup_system(&mut system, cluster_size)?;
        let storage_client = client.storage;
        let version = ObjectVersion(1);
        let expected = vec![0x03];

        wait(storage_client.clone().put(
            version,
            expected.clone(),
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;
        let actual = wait(storage_client.get(
            ObjectValue {
                version,
                content: expected.clone(),
            },
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn get_fragment_works() -> TestResult {
        // fragments = 5 (data_fragments = 4, parity_fragments = 1)
        let data_fragments = 4;
        let parity_fragments = 1;
        let cluster_size = 6;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (members, client) = setup_system(&mut system, cluster_size)?;
        let storage_client = client.storage;
        let (node_id, device_id, _) = members[0].clone();
        let version = ObjectVersion(4);
        let expected = vec![0x02];

        // This assersion means that
        //  the node `node_id` is a member of participants that put a data to a dispersed device.
        assert!(
            candidate_position(
                system.cluster_config(),
                ClusterMember {
                    node: node_id,
                    device: device_id,
                },
                version
            ) < system.fragments() as usize
        );

        wait(storage_client.clone().put(
            version,
            expected,
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;

        let result = wait(storage_client.get_fragment(node_id, version))?;

        if let MaybeFragment::Fragment(content) = result {
            assert!(!content.is_empty());
            return Ok(());
        }

        Err(ErrorKind::Other
            .cause("Cannot get a fragment".to_owned())
            .into())
    }

    #[test]
    fn get_fragment_returns_not_participant() -> TestResult {
        // fragments = 5 (data_fragments = 4, parity_fragments = 1)
        let data_fragments = 4;
        let parity_fragments = 1;
        let cluster_size = 6;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (members, client) = setup_system(&mut system, cluster_size)?;
        let storage_client = client.storage;
        let (node_id, device_id, _) = members[0].clone();

        let version = ObjectVersion(6);
        let expected = vec![0x02];

        // This assertion means that
        //  the node `node_id` is not a member of participants that put a data to a dispersed device
        //  with high probability.
        assert!(
            candidate_position(
                system.cluster_config(),
                ClusterMember {
                    node: node_id,
                    device: device_id
                },
                version
            ) >= system.fragments() as usize
        );

        wait(storage_client.clone().put(
            version,
            expected,
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;

        let result = wait(storage_client.get_fragment(node_id, version))?;

        assert_eq!(result, MaybeFragment::NotParticipant);

        Ok(())
    }

    #[test]
    fn put_all_ensures_all_futures_completion() -> TestResult {
        // We use the fact that PutAll doesn't know what passed futures do.
        // Specifically, it's okay to pass PutAll futures that are not related to PUT operations.
        let future_count = 12;
        let required_ok_count = 8;
        let atomic_limit = Arc::new(AtomicUsize::new(required_ok_count));
        let atomic_count = Arc::new(AtomicUsize::new(0));
        let mut futures = vec![];
        for i in 0..future_count {
            let atomic_limit = atomic_limit.clone();
            let atomic_count = atomic_count.clone();
            // Limited by atomic_limit. In the initial configuration, only 8 futures will finish,
            // and the other 4 will be run forever.
            let future = poll_fn(move || -> Poll<(), Error> {
                if i < atomic_limit.load(Ordering::SeqCst) {
                    atomic_count.fetch_add(1, Ordering::SeqCst);
                    return Ok(Async::Ready(()));
                }
                Ok(Async::NotReady)
            });
            futures.push(Box::new(future) as BoxFuture<()>);
        }
        let put_all_metrics = PutAllMetrics::new("test")?;
        let put_all = PutAll::new(
            put_all_metrics.clone(),
            futures.into_iter(),
            required_ok_count,
        )?;
        wait(put_all)?;
        // No failures so far - 8 completed, 4 pending
        assert!(put_all_metrics.lost_fragments_total.value().abs() <= 1e-5);
        assert_eq!(atomic_count.load(Ordering::SeqCst), required_ok_count);

        // Change limit to 12.
        // If futures given to PutAll are taken care of, atomic_count will eventually become 12.
        atomic_limit.store(future_count, Ordering::SeqCst);

        // Wait for at most three seconds.
        let now = Instant::now();
        let maximum_waiting_time = Duration::from_secs(3);
        while now.elapsed() < maximum_waiting_time {
            sleep(Duration::from_millis(10));
            if atomic_count.load(Ordering::SeqCst) == future_count {
                break;
            }
        }
        assert_eq!(atomic_count.load(Ordering::SeqCst), future_count);

        Ok(())
    }
    #[test]
    fn put_all_tracks_all_futures_errors() -> TestResult {
        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::ok(())),
            Box::new(futures::future::err(ErrorKind::Other.into())),
        ];
        let put_all_metrics = track!(PutAllMetrics::new("test_client"))?;
        let put = track!(PutAll::new(put_all_metrics.clone(), futures.into_iter(), 1))?;
        assert!(wait(put).is_ok());
        // The first and the third futures will eventually fail.
        // Wait for at most three seconds and check if lost_fragments_total is actually modified.
        let now = Instant::now();
        let maximum_waiting_time = Duration::from_secs(3);
        while now.elapsed() < maximum_waiting_time {
            sleep(Duration::from_millis(10));
            if (put_all_metrics.lost_fragments_total.value() - 2.0).abs() <= 1e-5 {
                break;
            }
        }

        assert!((put_all_metrics.lost_fragments_total.value() - 2.0).abs() <= 1e-5);
        Ok(())
    }
}
