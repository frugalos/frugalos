#[cfg(test)]
pub mod tests {
    use cannyls::device::{DeviceBuilder, DeviceHandle};
    use cannyls::nvm::MemoryNvm;
    use cannyls::storage::StorageBuilder;
    use cannyls_rpc;
    use cannyls_rpc::DeviceRegistryHandle;
    use client::Client;
    use config::*;
    use fibers::executor::Executor;
    use fibers::executor::ThreadPoolExecutor;
    use fibers_global;
    use fibers_rpc::client::{ClientService, ClientServiceHandle};
    use fibers_rpc::server::ServerBuilder;
    use frugalos_core;
    use frugalos_mds;
    use frugalos_raft::{self, LocalNodeId, NodeId};
    use futures;
    use futures::future::Future;
    use futures::Async;
    use libfrugalos::entity::device::DeviceId;
    use raftlog::cluster::ClusterMembers;
    use slog;
    use std::net::SocketAddr;
    use std::thread;
    use std::time::Duration;
    use trackable::error::ErrorKindExt;
    use {Error, ErrorKind, Result};
    use {Service, ServiceHandle};

    /// Waits for the completion of the given future.
    pub fn wait<F: Future<Error = Error>>(mut f: F) -> Result<F::Item> {
        loop {
            if let Async::Ready(result) = track!(f.poll())? {
                return Ok(result);
            }

            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Make a frugalos segment where there are `segment_size`-nodes.
    ///
    /// This method needs `segment_size >= system.fragments()`.
    #[allow(clippy::type_complexity)]
    pub fn setup_system(
        system: &mut System,
        segment_size: usize,
    ) -> Result<(Vec<(NodeId, DeviceId, DeviceHandle)>, Client)> {
        assert!(segment_size >= system.fragments() as usize);

        let mut members = Vec::new();

        for _ in 0..segment_size {
            let (node_id, device_id, device_handle) = system.make_node()?;
            members.push((node_id, device_id, device_handle));
        }

        let client = system.boot(members.clone())?;

        Ok((members, client))
    }

    /// A cluster for testing.
    /// All implementations under this struct is unstable.
    pub struct System {
        data_fragments: u8,
        parity_fragments: u8,
        logger: slog::Logger,
        device_registry_handle: DeviceRegistryHandle,
        service_handle: ServiceHandle,
        rpc_service_handle: ClientServiceHandle,
        rpc_server_addr: SocketAddr,
        node_seqno: u8,
        device_no: u8,
        cluster_config: ClusterConfig,
        pub executor: ThreadPoolExecutor,
    }

    impl System {
        /// Returns a new cluster with no node.
        pub fn new(data_fragments: u8, parity_fragments: u8) -> Result<Self> {
            let logger = slog::Logger::root(slog::Discard, o!());
            let executor = ThreadPoolExecutor::with_thread_count(10).expect("never fails");
            let mut rpc_server_builder = ServerBuilder::new(([127, 0, 0, 1], 0).into());

            let rpc_service = ClientService::new(fibers_global::handle());
            let rpc_service_handle = rpc_service.handle();
            fibers_global::spawn(rpc_service.map_err(|e| panic!("{}", e)));

            let raft_service = frugalos_raft::Service::new(logger.clone(), &mut rpc_server_builder);
            let raft_service_handle = raft_service.handle();
            fibers_global::spawn(raft_service.map_err(|e| panic!("{}", e)));

            let service = Service::new(
                logger.clone(),
                executor.handle(),
                rpc_service_handle.clone(),
                &mut rpc_server_builder,
                raft_service_handle,
                frugalos_mds::FrugalosMdsConfig::default(),
                frugalos_core::tracer::make_null_tracer(),
            )?;
            let service_handle = service.handle();
            let device_registry_handle = service.device_registry().handle();
            fibers_global::spawn(service.map_err(|e| panic!("{}", e)));

            let rpc_server = rpc_server_builder.finish(fibers_global::handle());
            let (rpc_server, bind_addr) = track!(fibers_global::execute(rpc_server.local_addr()))?;
            fibers_global::spawn(rpc_server.map_err(|e| panic!("{}", e)));

            Ok(System {
                data_fragments,
                parity_fragments,
                logger,
                device_registry_handle,
                rpc_service_handle,
                service_handle,
                rpc_server_addr: bind_addr,
                node_seqno: 0,
                device_no: 0,
                cluster_config: ClusterConfig {
                    members: Vec::new(),
                },
                executor,
            })
        }

        /// Returns the immutable reference to the cluster configuration.
        pub fn cluster_config(&self) -> &ClusterConfig {
            &self.cluster_config
        }

        /// Returns the size of fragments(data_fragments + parity_fragments).
        pub fn fragments(&self) -> u8 {
            self.data_fragments + self.parity_fragments
        }

        /// Returns a logger.
        pub fn logger(&self) -> slog::Logger {
            self.logger.clone()
        }

        /// Registers all the nodes in the `members`.
        fn register_nodes(&mut self, members: &[(NodeId, DeviceId, DeviceHandle)]) -> Result<()> {
            let cluster: ClusterMembers = self
                .cluster_config
                .members
                .iter()
                .map(|m| m.node.to_raft_node_id())
                .collect();

            for (node_id, _, device_handle) in members {
                let client = track!(self.make_segment_client())?;
                self.service_handle
                    .add_node(
                        node_id.clone(),
                        Box::new(
                            futures::future::ok::<DeviceHandle, Error>(device_handle.clone())
                                .map_err(|e| ErrorKind::Other.takes_over(e).into()),
                        ),
                        client,
                        cluster.clone(),
                    )
                    .unwrap();
            }
            Ok(())
        }

        /// Boots this cluster with the given members.
        #[allow(clippy::needless_pass_by_value)]
        pub fn boot(&mut self, members: Vec<(NodeId, DeviceId, DeviceHandle)>) -> Result<Client> {
            // at least one cluster member is required
            if members.is_empty() {
                return Err(ErrorKind::Other.into());
            }

            for member in &members {
                self.cluster_config.members.push(ClusterMember {
                    node: member.0,
                    device: member.1.clone(),
                });
            }

            track!(self.register_nodes(&members))?;
            self.make_segment_client().map_err(|e| track!(e))
        }

        /// Returns a new node.
        pub fn make_node(&mut self) -> Result<(NodeId, DeviceId, DeviceHandle)> {
            let node_id = self.make_node_id();
            let (device_id, device_handle) = self.spawn_new_memory_device()?;
            Ok((node_id, device_id, device_handle))
        }

        /// Spawns a new memory device and returns it.
        fn spawn_new_memory_device(&mut self) -> Result<(DeviceId, DeviceHandle)> {
            let nvm = MemoryNvm::new(vec![0; 1024 * 1024 * 32]);
            let storage =
                track_try_unwrap!(StorageBuilder::new().journal_region_ratio(0.05).create(nvm));
            let device = DeviceBuilder::new().spawn(|| Ok(storage));
            let device_id = self.device_no.to_string();
            let handle = device.handle();
            // Waits until the device begins to work.
            let _ = wait(
                handle
                    .request()
                    .wait_for_running()
                    .list()
                    .map_err(Error::from),
            )?;
            self.device_registry_handle
                .put_device(cannyls_rpc::DeviceId::new(device_id.clone()), device)?;
            self.device_no += 1;

            Ok((device_id, handle))
        }

        /// Creates a new SegmentClient.
        pub fn make_segment_client(&self) -> Result<Client> {
            Client::new(
                self.logger(),
                self.rpc_service_handle.clone(),
                ClientConfig {
                    cluster: self.cluster_config.clone(),
                    dispersed_client: Default::default(),
                    replicated_client: Default::default(),
                    storage: self.make_dispersed_storage(),
                    mds: MdsClientConfig::default(),
                },
                None,
            )
            .map_err(|e| track!(e))
        }

        /// Creates a new `NodeId`.
        fn make_node_id(&mut self) -> NodeId {
            let local_node_id = LocalNodeId::new([0, 0, 0, 0, 0, 0, self.node_seqno]);
            self.node_seqno += 1;
            NodeId {
                local_id: local_node_id,
                instance: 0,
                addr: self.rpc_server_addr,
            }
        }

        /// It needs massive activities to change this function,
        /// because some tests depends on this configuration of `DispersedConfig`.
        fn make_dispersed_storage(&self) -> Storage {
            Storage::Dispersed(DispersedConfig {
                tolerable_faults: self.parity_fragments,
                fragments: self.fragments(),
            })
        }
    }
}
