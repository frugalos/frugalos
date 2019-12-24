use cannyls::deadline::Deadline;
use cannyls::lump::LumpData;
use cannyls_rpc::Client as CannyLsClient;
use cannyls_rpc::DeviceId;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use std::sync::Arc;
use trackable::error::ErrorKindExt;

use client::storage::{append_checksum, verify_and_remove_checksum, PutAll};
use config::{
    CannyLsClientConfig, ClusterConfig, ClusterMember, ReplicatedClientConfig, ReplicatedConfig,
};
use metrics::ReplicatedClientMetrics;
use util::BoxFuture;
use {Error, ErrorKind};

#[derive(Debug, Clone)]
pub struct ReplicatedClient {
    metrics: ReplicatedClientMetrics,
    cluster: Arc<ClusterConfig>,
    config: ReplicatedConfig,
    client_config: ReplicatedClientConfig,
    rpc_service: RpcServiceHandle,
}
impl ReplicatedClient {
    pub fn new(
        metrics: ReplicatedClientMetrics,
        cluster: ClusterConfig,
        config: ReplicatedConfig,
        client_config: ReplicatedClientConfig,
        rpc_service: RpcServiceHandle,
    ) -> Self {
        ReplicatedClient {
            metrics,
            cluster: Arc::new(cluster),
            config,
            client_config,
            rpc_service,
        }
    }
    pub fn get_fragment(
        self,
        _local_node: NodeId,
        version: ObjectVersion,
    ) -> GetReplicatedFragment {
        // TODO: `_local_node`は問い合わせ候補から外す(必ず失敗するので)
        let future = self.get(version, Deadline::Infinity);
        GetReplicatedFragment(future)
    }
    pub fn get(self, version: ObjectVersion, deadline: Deadline) -> BoxFuture<Vec<u8>> {
        let replica = self.config.tolerable_faults as usize + 1;
        let mut candidates = self
            .cluster
            .candidates(version)
            .take(replica)
            .cloned()
            .collect::<Vec<_>>();
        candidates.reverse();
        let future = ReplicatedGet {
            version,
            deadline,
            cannyls_config: self.client_config.cannyls.clone(),
            candidates,
            rpc_service: self.rpc_service,
            future: Box::new(futures::finished(None)),
        };
        Box::new(future)
    }
    /// TODO 実装
    pub fn head(self, _version: ObjectVersion, _deadline: Deadline) -> BoxFuture<()> {
        Box::new(futures::future::ok(()))
    }
    pub fn put(
        self,
        version: ObjectVersion,
        mut content: Vec<u8>,
        deadline: Deadline,
    ) -> BoxFuture<()> {
        let rpc_service = self.rpc_service;
        let replica = self.config.tolerable_faults as usize + 1;
        append_checksum(&mut content);

        let data = match track!(LumpData::new(content)) {
            Ok(data) => data,
            Err(error) => return Box::new(futures::failed(Error::from(error))),
        };
        let cannyls_config = self.client_config.cannyls.clone();

        let futures = self
            .cluster
            .candidates(version)
            .take(replica)
            .map(move |m| {
                let client = CannyLsClient::new(m.node.addr, rpc_service.clone());
                let mut request = client.request();
                request.rpc_options(cannyls_config.rpc_options());

                let device_id = DeviceId::new(m.device.clone());
                let lump_id = m.make_lump_id(version);
                let future: BoxFuture<_> = Box::new(
                    request
                        .deadline(deadline)
                        .max_queue_len(cannyls_config.device_max_queue_len)
                        .put_lump(device_id, lump_id, data.clone())
                        .map(|_is_new| ())
                        .map_err(|e| track!(Error::from(e))),
                );
                future
            });
        let put_all = match track!(PutAll::new(self.metrics.put_all.clone(), futures, 1)) {
            Ok(put_all) => put_all,
            Err(error) => return Box::new(futures::failed(error)),
        };
        Box::new(put_all)
    }
    pub fn delete_fragment(
        self,
        version: ObjectVersion,
        deadline: Deadline,
        index: usize,
    ) -> BoxFuture<bool> {
        let mut candidates = self
            .cluster
            .candidates(version)
            .cloned()
            .collect::<Vec<_>>();
        candidates.reverse();
        if candidates.len() <= index {
            return Box::new(futures::future::ok(false));
        }
        let cluster_member = candidates[index].clone();
        let cannyls_client = CannyLsClient::new(cluster_member.node.addr, self.rpc_service.clone());
        let lump_id = cluster_member.make_lump_id(version);
        let mut request = cannyls_client.request();
        request.rpc_options(self.client_config.cannyls.rpc_options());
        let device = cluster_member.device;
        let future = request
            .deadline(deadline)
            .delete_lump(DeviceId::new(device), lump_id)
            .then(move |result| result);
        Box::new(future.map_err(|e| track!(Error::from(e))))
    }
}

pub struct ReplicatedGet {
    version: ObjectVersion,
    deadline: Deadline,
    cannyls_config: CannyLsClientConfig,
    candidates: Vec<ClusterMember>,
    future: BoxFuture<Option<Vec<u8>>>,
    rpc_service: RpcServiceHandle,
}
impl Future for ReplicatedGet {
    type Item = Vec<u8>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.future.poll() {
                Err(e) => {
                    if self.candidates.is_empty() {
                        return Err(track!(e));
                    }
                    self.future = Box::new(futures::finished(None))
                }
                Ok(Async::Ready(None)) => {
                    let m = track!(self
                        .candidates
                        .pop()
                        .ok_or_else(|| ErrorKind::Corrupted.error(),))?;
                    let client = CannyLsClient::new(m.node.addr, self.rpc_service.clone());
                    let mut request = client.request();
                    request.rpc_options(self.cannyls_config.rpc_options());

                    let lump_id = m.make_lump_id(self.version);
                    let future = request
                        .deadline(self.deadline)
                        .get_lump(DeviceId::new(m.device), lump_id);
                    self.future = Box::new(future.map_err(|e| track!(Error::from(e))));
                }
                Ok(Async::Ready(Some(mut content))) => {
                    if let Err(e) = track!(verify_and_remove_checksum(&mut content)) {
                        if self.candidates.is_empty() {
                            return Err(track!(e));
                        }
                        self.future = Box::new(futures::finished(None));
                    } else {
                        return Ok(Async::Ready(content));
                    }
                }
                Ok(Async::NotReady) => break,
            }
        }
        Ok(Async::NotReady)
    }
}

pub struct GetReplicatedFragment(BoxFuture<Vec<u8>>);
impl Future for GetReplicatedFragment {
    type Item = Vec<u8>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self.0.poll())
    }
}
