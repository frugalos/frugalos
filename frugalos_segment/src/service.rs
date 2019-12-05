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
use frugalos_raft::{self, LocalNodeId, NodeCoordinator, NodeId, RaftDeviceId};
use futures::{Async, Future, Poll, Stream};
use raftlog::cluster::ClusterMembers;
use slog::Logger;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use trackable::error::ErrorKindExt;

use client::storage::StorageClient;
use libfrugalos::repair::{RepairConfig, RepairIdleness};
use rpc_server::RpcServer;
use std::collections::HashMap;
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
    // Senders of `SegmentNode`s
    segment_node_handles: HashMap<LocalNodeId, SegmentNodeHandle>,
    repair_concurrency: Arc<Mutex<RepairConcurrency>>,
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
        let (command_tx, command_rx) = mpsc::channel();
        CannyLsRpcServer::new(device_registry.handle()).register(rpc);

        let service = Service {
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
            segment_node_handles: HashMap::new(),
            repair_concurrency: Arc::new(Mutex::new(RepairConcurrency::new())),
        };

        RpcServer::register(service.handle(), rpc);

        Ok(service)
    }

    /// サービスを操作するためのハンドルを返す。
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            logger: self.logger.clone(),
            mds: self.mds_service.handle(),
            device_registry: self.device_registry.handle(),
            command_tx: self.command_tx.clone(),
            repair_concurrency: Arc::clone(&self.repair_concurrency),
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

    /// repair_idleness_threshold の変更要求を発行する。
    #[allow(clippy::needless_pass_by_value)]
    pub fn set_repair_config(&mut self, repair_config: RepairConfig) {
        // TODO: handle RepairConfig's remaining field (segment_gc_concurrency_limit)
        if let Some(repair_idleness_threshold) = repair_config.repair_idleness_threshold {
            for (_, segment_node_handle) in self.segment_node_handles.iter() {
                let command =
                    SegmentNodeCommand::SetRepairIdlenessThreshold(repair_idleness_threshold);
                segment_node_handle.send(command);
            }
        }
        if let Some(repair_concurrency_limit) = repair_config.repair_concurrency_limit {
            // Even if more than repair_concurrency_limit threads are running, we don't do anything to them.
            // Just let them finish their jobs.
            let mut lock = self
                .repair_concurrency
                .lock()
                .unwrap_or_else(|e| panic!("Lock failed with error: {:?}", e));
            lock.set_limit(repair_concurrency_limit.0);
        }
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
            Command::AddNode(
                node_id,
                raft_device_id,
                device,
                client,
                cluster,
                config,
                node_coordinator,
            ) => {
                // TODO: error handling
                let logger = self.logger.clone();
                let logger0 = logger.clone();
                let logger1 = logger.clone();
                let service_handle = self.handle();
                let local_id = node_id.local_id;
                let spawner = self.spawner.clone();
                let rpc_service = self.rpc_service.clone();
                let raft_service = self.raft_service.clone();
                let mds_config = self.mds_config.clone();
                let mds_service = self.mds_service.handle();
                // The sender (tx) and the receiver (rx) for SegmentNode.
                // Rather than passing both tx and rx to SegmentNode's constructor
                // and allow SegmentNode to make handles by cloning tx,
                // we pass rx only and hold tx for use in SegmentService.
                // That is because we need tx only in SegmentService.
                let (segment_node_command_tx, segment_node_command_rx) = mpsc::channel();
                // TODO: Remove a node from segment_node_handles when a SegmentNode terminates with an error
                self.segment_node_handles
                    .insert(local_id, SegmentNodeHandle(segment_node_command_tx));
                let future = device
                    .map_err(|e| track!(e))
                    .and_then(move |device| {
                        // raft のログが不正な状態になった場合に強制削除するための対応
                        // https://github.com/frugalos/frugalos/issues/157
                        // https://github.com/frugalos/raftlog/issues/18
                        let logger = logger1.new(o!("node" => local_id.to_string()));
                        let future = if config.discard_former_log {
                            let storage = frugalos_raft::Storage::new(
                                logger,
                                local_id,
                                raft_device_id,
                                device.clone(),
                                node_coordinator.clone(),
                                frugalos_raft::StorageMetrics::new(),
                            );
                            frugalos_raft::ClearLog::new(storage)
                        } else {
                            frugalos_raft::ClearLog::skip()
                        };
                        future.map(|_| device).map_err(|e| track!(Error::from(e)))
                    })
                    .and_then(move |device| {
                        track!(SegmentNode::new(
                            &logger0,
                            spawner,
                            rpc_service,
                            raft_service,
                            &mds_config,
                            mds_service,
                            node_id,
                            raft_device_id,
                            device,
                            service_handle,
                            client,
                            cluster,
                            node_coordinator.clone(),
                            segment_node_command_rx
                        ))
                    })
                    .map_err(move |e| crit!(logger, "Error: {}", e))
                    .and_then(|node| node);
                self.spawner.spawn(future);
            }
            Command::SetRepairConfig(repair_config) => {
                self.set_repair_config(repair_config);
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
            // If the channel becomes disconnected, it returns None. This is the case especially on `frugalos stop.`
            // If that happens, it is suppressed.
            if let Some(command) = command {
                self.handle_command(command);
            }
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
    repair_concurrency: Arc<Mutex<RepairConcurrency>>,
}
impl ServiceHandle {
    // FIXME: 将来的には`client`と`cluster`は統合可能(前者から後者を引ける)
    /// サービスにノードを登録する。
    pub fn add_node(
        &self,
        node_id: NodeId,
        device_id: RaftDeviceId,
        device: CreateDeviceHandle,
        client: Client,
        cluster: ClusterMembers,
        node_coordinator: NodeCoordinator,
        // NOTE: "前回の状態"は raft だけに限らないので raft を意識しない
        discard_former_state: bool,
    ) -> Result<()> {
        let raft_config = RaftConfig {
            discard_former_log: discard_former_state,
        };
        let command = Command::AddNode(
            node_id,
            device_id,
            device,
            client.storage,
            cluster,
            raft_config,
            node_coordinator,
        );
        track!(self
            .command_tx
            .send(command,)
            .map_err(|_| ErrorKind::Other.error(),))?;
        Ok(())
    }
    /// repair_config の変更要求を発行する。
    pub fn set_repair_config(&self, repair_config: RepairConfig) {
        let command = Command::SetRepairConfig(repair_config);
        let _ = self.command_tx.send(command);
    }
    /// Attempt to acquire repair lock.
    pub fn acquire_repair_lock(&self) -> Option<RepairLock> {
        RepairLock::new(&self.repair_concurrency)
    }
}

// Settings of repair's concurrency.
struct RepairConcurrency {
    repair_concurrency_limit: u64,
    current_repair_threads: u64,
}

impl RepairConcurrency {
    fn new() -> Self {
        RepairConcurrency {
            repair_concurrency_limit: 0,
            current_repair_threads: 0,
        }
    }
    fn set_limit(&mut self, limit: u64) {
        self.repair_concurrency_limit = limit;
    }
}

// Lock object for repair. Owner of this object is allowed to perform repair.
pub struct RepairLock {
    repair_concurrency: Arc<Mutex<RepairConcurrency>>,
}

impl RepairLock {
    fn new(repair_concurrency: &Arc<Mutex<RepairConcurrency>>) -> Option<Self> {
        let mut lock = repair_concurrency.lock().expect("Lock never fails");
        // Too many threads running.
        if lock.current_repair_threads >= lock.repair_concurrency_limit {
            return None;
        }
        lock.current_repair_threads += 1;
        Some(RepairLock {
            repair_concurrency: repair_concurrency.clone(),
        })
    }
}

impl Drop for RepairLock {
    fn drop(&mut self) {
        let mut lock = self.repair_concurrency.lock().expect("Lock never fails");
        lock.current_repair_threads -= 1;
    }
}

pub type CreateDeviceHandle = Box<dyn Future<Item = DeviceHandle, Error = Error> + Send + 'static>;

/// Raft に関連する設定。
struct RaftConfig {
    /// true ならノード追加前に保存されていた Raft のログを破棄する。
    discard_former_log: bool,
}

#[allow(clippy::large_enum_variant)]
enum Command {
    AddNode(
        NodeId,
        RaftDeviceId,
        CreateDeviceHandle,
        StorageClient,
        ClusterMembers,
        RaftConfig,
        NodeCoordinator,
    ),
    SetRepairConfig(RepairConfig),
}

struct SegmentNode {
    logger: Logger,
    node: Node,
    synchronizer: Synchronizer,
    segment_node_command_rx: mpsc::Receiver<SegmentNodeCommand>,
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
        raft_device_id: RaftDeviceId,
        device: DeviceHandle,
        service_handle: ServiceHandle,
        client: StorageClient,
        cluster: ClusterMembers,
        coordinator: NodeCoordinator,
        segment_node_command_rx: mpsc::Receiver<SegmentNodeCommand>,
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
            raft_device_id,
            device.clone(),
            coordinator,
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

        let full_sync_step = env::var("FRUGALOS_FULL_SYNC_STEP")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(100);
        info!(logger, "FullSync step: {}", full_sync_step);

        let synchronizer = Synchronizer::new(
            logger.clone(),
            node_id,
            device,
            service_handle,
            client,
            full_sync_step,
        );

        Ok(SegmentNode {
            logger,
            node,
            synchronizer,
            segment_node_command_rx,
        })
    }
    fn run_once(&mut self) -> Result<bool> {
        // Handle a command once at a time.
        if let Async::Ready(command) = self.segment_node_command_rx.poll().expect("Never fails") {
            // If the channel becomes disconnected, it returns None. This is the case especially on `frugalos stop.`
            // If that happens, it is suppressed.
            if let Some(command) = command {
                self.handle_command(command);
            }
        }
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
    #[allow(clippy::needless_pass_by_value)]
    fn handle_command(&mut self, command: SegmentNodeCommand) {
        match command {
            SegmentNodeCommand::SetRepairIdlenessThreshold(idleness_threshold) => {
                self.synchronizer
                    .set_repair_idleness_threshold(idleness_threshold);
            }
        }
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

#[derive(Clone)]
struct SegmentNodeHandle(mpsc::Sender<SegmentNodeCommand>);

impl SegmentNodeHandle {
    fn send(&self, command: SegmentNodeCommand) {
        let _ = self.0.send(command);
    }
}

enum SegmentNodeCommand {
    SetRepairIdlenessThreshold(RepairIdleness),
}
