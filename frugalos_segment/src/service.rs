use cannyls::device::DeviceHandle;
use cannyls_rpc::Server as CannyLsRpcServer;
use cannyls_rpc::{DeviceRegistry, DeviceRegistryHandle};
use fibers::sync::mpsc;
use fibers::Spawn;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_core::tracer::ThreadLocalTracer;
use frugalos_mds::{
    FrugalosMdsConfig, Node, Service as RaftMdsService, ServiceHandle as MdsHandle,
};
use frugalos_raft::{self, NodeId};
use futures::{Async, Future, Poll, Stream};
use raftlog::cluster::ClusterMembers;
use slog::Logger;
use std::env;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use client::storage::StorageClient;
use synchronizer::Synchronizer;
use {Client, Error, ErrorKind, Result};

/// セグメント群を管理するためのサービス。
pub struct Service<S> {
    logger: Logger,
    spawner: S,
    rpc_service: RpcServiceHandle,
    raft_service: frugalos_raft::ServiceHandle,
    mds_service: RaftMdsService,
    device_registry: DeviceRegistry,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    mds_alive: bool,
    mds_config: FrugalosMdsConfig,
}
impl<S> Service<S>
where
    S: Spawn + Send + Clone + 'static,
{
    /// 新しい`Service`インスタンスを生成する。
    pub fn new(
        logger: Logger,
        spawner: S,
        rpc_service: RpcServiceHandle,
        rpc: &mut RpcServerBuilder,
        raft_service: frugalos_raft::ServiceHandle,
        mds_config: FrugalosMdsConfig,
        tracer: ThreadLocalTracer,
    ) -> Result<Self> {
        let mds_service = track!(RaftMdsService::new(logger.clone(), rpc, tracer))?;
        let device_registry = DeviceRegistry::new(logger.clone());
        CannyLsRpcServer::new(device_registry.handle()).register(rpc);

        let (command_tx, command_rx) = mpsc::channel();
        Ok(Service {
            logger,
            rpc_service,
            spawner,
            raft_service,
            mds_service,
            device_registry,
            command_tx,
            command_rx,
            mds_alive: true,
            mds_config,
        })
    }

    /// サービスを操作するためのハンドルを返す。
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            logger: self.logger.clone(),
            mds: self.mds_service.handle(),
            device_registry: self.device_registry.handle(),
            command_tx: self.command_tx.clone(),
        }
    }

    /// サービスに停止要求を発行する。
    ///
    /// 停止処理が完了したかどうかは`Service::poll`で判断する。
    pub fn stop(&mut self) {
        self.mds_service.stop();
    }

    /// Raftのスナップショット取得要求を発行する。
    pub fn take_snapshot(&mut self) {
        self.mds_service.take_snapshot();
    }

    /// デバイスレジストリへの破壊的な参照を返す。
    pub fn device_registry_mut(&mut self) -> &mut DeviceRegistry {
        &mut self.device_registry
    }

    /// デバイスレジストリへの参照を返す。
    pub fn device_registry(&self) -> &DeviceRegistry {
        &self.device_registry
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::AddNode(node_id, device, client, cluster) => {
                // TODO: error handling
                let logger = self.logger.clone();
                let logger0 = logger.clone();
                let spawner = self.spawner.clone();
                let rpc_service = self.rpc_service.clone();
                let raft_service = self.raft_service.clone();
                let mds_config = self.mds_config.clone();
                let mds_service = self.mds_service.handle();
                let future = device
                    .map_err(|e| track!(e))
                    .and_then(move |device| {
                        track!(SegmentNode::new(
                            &logger0,
                            spawner,
                            rpc_service,
                            raft_service,
                            &mds_config,
                            mds_service,
                            node_id,
                            device,
                            client,
                            cluster,
                        ))
                    })
                    .map_err(move |e| crit!(logger, "Error: {}", e))
                    .and_then(|node| node);
                self.spawner.spawn(future);
            }
        }
    }
}
impl<S> Future for Service<S>
where
    S: Spawn + Send + Clone + 'static,
{
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let terminated = self.mds_alive && track!(self.mds_service.poll())?.is_ready();
        if terminated {
            info!(self.logger, "MDS service terminated");
            self.mds_alive = false;
            self.device_registry.stop();
        }

        let terminated = track!(self.device_registry.poll())?.is_ready();
        if terminated {
            info!(self.logger, "CannyLs service terminated");
            return Ok(Async::Ready(()));
        }

        while let Async::Ready(command) = self.command_rx.poll().expect("Never fails") {
            let command = command.expect("Never fails");
            self.handle_command(command);
        }
        Ok(Async::NotReady)
    }
}

/// サービスを操作するためのハンドル。
#[derive(Clone)]
pub struct ServiceHandle {
    logger: Logger,
    mds: MdsHandle,
    device_registry: DeviceRegistryHandle,
    command_tx: mpsc::Sender<Command>,
}
impl ServiceHandle {
    // FIXME: 将来的には`client`と`cluster`は統合可能(前者から後者を引ける)
    /// サービスにノードを登録する。
    pub fn add_node(
        &self,
        node_id: NodeId,
        device: CreateDeviceHandle,
        client: Client,
        cluster: ClusterMembers,
    ) -> Result<()> {
        let command = Command::AddNode(node_id, device, client.storage, cluster);
        track!(self
            .command_tx
            .send(command,)
            .map_err(|_| ErrorKind::Other.error(),))?;
        Ok(())
    }
}

pub type CreateDeviceHandle = Box<Future<Item = DeviceHandle, Error = Error> + Send + 'static>;

pub enum Command {
    AddNode(NodeId, CreateDeviceHandle, StorageClient, ClusterMembers),
}

struct SegmentNode {
    logger: Logger,
    node: Node,
    synchronizer: Synchronizer,
}
impl SegmentNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new<S>(
        // TODO: service: &Service<S>,
        logger: &Logger,
        spawner: S,
        rpc_service: RpcServiceHandle,
        raft_service: frugalos_raft::ServiceHandle,
        mds_config: &FrugalosMdsConfig,
        mds_service: MdsHandle,

        node_id: NodeId,
        device: DeviceHandle,
        client: StorageClient,
        cluster: ClusterMembers,
    ) -> Result<Self>
    where
        S: Clone + Spawn + Send + 'static,
    {
        let logger = logger.new(o!("node" => node_id.local_id.to_string()));

        // TODO: 正式な口を用意する
        let min_timeout = env::var("FRUGALOS_RAFT_MIN_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let max_timeout = env::var("FRUGALOS_RAFT_MAX_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5 * 1000);

        let timer = frugalos_raft::Timer::new(
            Duration::from_millis(min_timeout),
            Duration::from_millis(max_timeout),
        );
        let storage = frugalos_raft::Storage::new(
            logger.clone(),
            node_id.local_id,
            device.clone(),
            frugalos_raft::StorageMetrics::new(),
        );
        let mailer = frugalos_raft::Mailer::new(
            spawner,
            rpc_service.clone(),
            Some(frugalos_raft::RpcMetrics::new()),
        );
        let io = track!(frugalos_raft::RaftIo::new(
            raft_service,
            storage,
            mailer,
            timer
        ))?;
        let node = track!(Node::new(
            logger.clone(),
            &mds_config,
            mds_service,
            node_id,
            cluster,
            io,
            rpc_service
        ))?;

        // TODO: optionにする
        let repair_enabled = env::var("FRUGALOS_REPAIR_ENABLED")
            .ok()
            .map_or(false, |v| v == "1");
        info!(logger, "Repair enabled: {}", repair_enabled);

        let synchronizer =
            Synchronizer::new(logger.clone(), node_id, device, client, repair_enabled);

        Ok(SegmentNode {
            logger,
            node,
            synchronizer,
        })
    }
    fn run_once(&mut self) -> Result<bool> {
        while let Async::Ready(event) = track!(self.node.poll())? {
            if let Some(event) = event {
                self.synchronizer.handle_event(&event);
            } else {
                return Ok(false);
            }
        }
        track!(self.synchronizer.poll())?;
        Ok(true)
    }
}
impl Future for SegmentNode {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match track!(self.run_once()) {
            Err(e) => {
                crit!(self.logger, "Node down: {}", e);
                Err(())
            }
            Ok(false) => {
                info!(self.logger, "Node stopped");
                Ok(Async::Ready(()))
            }
            Ok(true) => Ok(Async::NotReady),
        }
    }
}
