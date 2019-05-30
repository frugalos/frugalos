use cannyls;
use cannyls::device::{Device, DeviceBuilder, DeviceHandle};
use cannyls::lump::LumpId;
use cannyls::nvm::MemoryNvm;
use fibers_global;
use fibers_rpc;
use fibers_rpc::client::{ClientService, ClientServiceHandle};
use fibers_rpc::server::ServerBuilder;
use futures::{Async, Future, Stream};
use raftlog;
use raftlog::cluster::ClusterMembers;
use raftlog::election::Role;
use raftlog::log::{LogEntry, LogIndex};
use raftlog::{Event, Io, ReplicatedLog};
use slog::{Discard, Logger};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use trackable::Trackable;

use node::{LocalNodeId, NodeId};
use raft_io::RaftIo;
use rpc::{Mailer, Service, ServiceHandle};
use storage::{Handle, Storage, StorageMetrics};
use timer::Timer;

pub type NodeIndex = usize;

pub(crate) struct System {
    raft_service: ServiceHandle,
    rpc_service: ClientServiceHandle,
    rpc_server_addr: SocketAddr,
    node_seqno: u8,
    members: ClusterMembers,
    rlogs: Vec<ReplicatedLog<RaftIo>>,
    handles: Vec<Handle>,
    devices: Vec<Device>, // Device がスコープを抜けると停止するので所有する
}

impl System {
    pub(crate) fn new() -> fibers_rpc::Result<Self> {
        let logger = Logger::root(Discard, o!());
        let mut rpc_server_builder = ServerBuilder::new(([127, 0, 0, 1], 0).into());

        let service = Service::new(logger, &mut rpc_server_builder);
        let service_handle = service.handle();
        fibers_global::spawn(service.map_err(|e| panic!("{}", e)));

        let rpc_server = rpc_server_builder.finish(fibers_global::handle());
        let (rpc_server, bind_addr) = track!(fibers_global::execute(rpc_server.local_addr()))?;
        fibers_global::spawn(rpc_server.map_err(|e| panic!("{}", e)));
        let rpc_service = ClientService::new(fibers_global::handle());
        let rpc_service_handle = rpc_service.handle();
        fibers_global::spawn(rpc_service.map_err(|e| panic!("{}", e)));

        Ok(System {
            raft_service: service_handle,
            rpc_service: rpc_service_handle,
            rpc_server_addr: bind_addr,
            node_seqno: 0,
            members: BTreeSet::new(),
            rlogs: Vec::new(),
            handles: Vec::new(),
            devices: Vec::new(),
        })
    }

    /// 引数で指定されたノード数で raft クラスタを構成する。
    pub(crate) fn boot(&mut self, node_size: usize) -> raftlog::Result<()> {
        let mut nodes = Vec::new();

        for _ in 0..node_size {
            let (node_id, io, device, handle) = track!(self.make_node())?;
            self.devices.push(device);
            self.handles.push(handle);
            self.members.insert(node_id.to_raft_node_id());
            nodes.push((node_id.to_raft_node_id(), io));
        }

        for (node_id, io) in nodes {
            self.rlogs
                .push(ReplicatedLog::new(node_id, self.members.clone(), io));
        }

        Ok(())
    }

    /// 最初のリーダが選出されるのを待機し、選ばれたリーダーノードの index を返す。
    /// 返された index は DeviceHandle などを取得するために使う。
    pub(crate) fn select_leader(&mut self) -> raftlog::Result<NodeIndex> {
        let leader;

        loop {
            let (i, event) = track!(poll_event(&mut self.rlogs))?;
            if let Event::RoleChanged {
                new_role: Role::Leader,
            } = event
            {
                leader = i;
                break;
            }
        }

        Ok(leader)
    }

    /// raft の propose をした後コミットされたコマンドを返す。
    pub fn propose(&mut self, leader: NodeIndex, proposal: Vec<u8>) -> raftlog::Result<Vec<u8>> {
        let proposal_id = track!(self.rlogs[leader].propose_command(proposal))?;
        loop {
            let (_, event) = track!(poll_event(&mut self.rlogs))?;
            if let Event::Committed {
                index,
                entry: LogEntry::Command { command, .. },
            } = event
            {
                if proposal_id.index == index {
                    return Ok(command);
                }
            }
        }
    }

    /// raft の propose を複数まとめて行う。
    pub fn bulk_propose(
        &mut self,
        leader: NodeIndex,
        proposals: Vec<Vec<u8>>,
    ) -> raftlog::Result<()> {
        for proposal in proposals {
            let _ = self.propose(leader, proposal)?;
        }

        Ok(())
    }

    pub fn get_handle(&self, node: NodeIndex) -> Option<Handle> {
        self.handles.get(node).cloned()
    }

    pub fn to_log_entry_lump_id(&self, node: NodeIndex, index: LogIndex) -> Option<LumpId> {
        self.handles
            .get(node)
            .map(|handle| handle.node_id.to_log_entry_lump_id(index))
    }

    pub fn get_device_handle(&self, node: NodeIndex) -> Option<DeviceHandle> {
        self.devices.get(node).map(|device| device.handle())
    }

    fn make_node(&mut self) -> raftlog::Result<(NodeId, RaftIo, Device, Handle)> {
        let timer = Timer::new(Duration::from_millis(50), Duration::from_millis(300));
        let mailer = Mailer::new(fibers_global::handle(), self.rpc_service.clone(), None);

        let local_node_id = LocalNodeId::new([0, 0, 0, 0, 0, 0, self.node_seqno]);
        self.node_seqno += 1;
        let node_id = NodeId {
            local_id: local_node_id,
            instance: 0,
            addr: self.rpc_server_addr,
        };

        let logger = Logger::root(Discard, o!());
        let nvm = MemoryNvm::new(vec![0; 10 * 1024 * 1024]);
        let device = DeviceBuilder::new().spawn(|| track!(cannyls::storage::Storage::create(nvm)));
        let storage = Storage::new(
            logger,
            local_node_id,
            device.handle(),
            StorageMetrics::new(),
        );
        let handle = storage.handle();
        let io = track!(RaftIo::new(
            self.raft_service.clone(),
            storage,
            mailer,
            timer
        ))?;

        Ok((node_id, io, device, handle))
    }
}

/// Future の実行完了を待ち、結果を返す。
pub(crate) fn wait_for<F: Future>(mut future: F) -> Result<F::Item, F::Error>
where
    F::Error: Trackable,
{
    loop {
        if let Async::Ready(item) = track!(future.poll())? {
            return Ok(item);
        }
        thread::sleep(Duration::from_millis(1));
    }
}

/// テスト用に raft クラスタに propose するために使えるデータを生成して返す。
pub(crate) fn make_proposals(size: usize) -> Vec<Vec<u8>> {
    (0..size)
        .map(|n| n.to_string().as_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>()
}

fn poll_event<IO: Io>(rlogs: &mut [ReplicatedLog<IO>]) -> raftlog::Result<(usize, Event)> {
    loop {
        for (i, rlog) in rlogs.iter_mut().enumerate() {
            if let Async::Ready(event) = track!(rlog.poll())? {
                let event =
                    track_assert_some!(event, raftlog::ErrorKind::Other, "Unexpected termination");
                return Ok((i, event));
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
}
