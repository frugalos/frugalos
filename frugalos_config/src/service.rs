use bytecodec::{DecodeExt, EncodeExt};
use cannyls::device::Device as CannylsDevice;
use fibers::sync::{mpsc, oneshot};
use fibers::Spawn;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_raft::{self, RaftIo};
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::bucket::{Bucket, BucketId, BucketSummary};
use libfrugalos::entity::device::{Device, DeviceId, DeviceSummary};
use libfrugalos::entity::server::{Server, ServerId, ServerSummary};
use raftlog::log::{LogEntry, LogIndex, ProposalId};
use raftlog::{self, ReplicatedLog};
use slog::Logger;
use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::net::SocketAddr;
use std::path::Path;
use trackable::error::ErrorKindExt;

use builder::SegmentTableBuilder;
use cluster;
use config;
use config::server_to_frugalos_raft_node;
use machine::{Command, DeviceGroup, NextSeqNo, SegmentTable, Snapshot};
use protobuf;
use rpc;
use {Error, ErrorKind, Result};

type RaftEvent = raftlog::Event;

const SNAPSHOT_THRESHOLD: usize = 128;

/// 構成管理用のサービス。
pub struct Service {
    logger: Logger,
    rlog: ReplicatedLog<RaftIo>,

    leader: Option<SocketAddr>,
    leader_waiters: Vec<Reply<SocketAddr>>,
    leader_waiters_threshold: usize,

    local_server: Server,

    request_tx: mpsc::Sender<Request>,
    request_rx: mpsc::Receiver<Request>,
    snapshot_reduction: usize,
    proposals: VecDeque<Proposal>, // TODO: VecDequeで良いかを確認(`raft_simu`を参考に)
    next_commit_index: LogIndex,

    buckets: BTreeMap<BucketId, Bucket>,
    devices: BTreeMap<DeviceId, Device>,
    servers: BTreeMap<ServerId, Server>,
    segment_tables: BTreeMap<BucketId, SegmentTable>,

    next_seqno: NextSeqNo,
    events: VecDeque<Event>,
    _device: CannylsDevice, // TODO: poll
}
impl Service {
    /// 既にクラスタに登録済みのサーバ用のサービスを起動する
    pub fn new<P: AsRef<Path>, S: Spawn + Clone + Send + 'static>(
        logger: Logger,
        data_dir: P,
        rpc_builder: &mut RpcServerBuilder,
        rpc_service: RpcServiceHandle,
        raft_service: frugalos_raft::ServiceHandle,
        config: config::FrugalosConfigConfig,
        spawner: S,
    ) -> Result<Self> {
        let server = track!(cluster::load_local_server_info(&data_dir))?;
        info!(logger, "Local server info: {:?}", server);

        // TODO
        let (device, rlog) = track!(cluster::make_rlog(
            logger.clone(),
            &data_dir,
            &server,
            rpc_service,
            spawner,
            raft_service,
            Vec::new(),
        ))?;

        let (request_tx, request_rx) = mpsc::channel();
        let this = Service {
            logger,
            rlog,
            local_server: server,
            leader: None,
            leader_waiters: Vec::new(),
            leader_waiters_threshold: config.leader_waiters_threshold,

            request_tx,
            request_rx,
            snapshot_reduction: SNAPSHOT_THRESHOLD,
            proposals: VecDeque::new(),
            next_commit_index: LogIndex::new(0),

            buckets: BTreeMap::new(),
            devices: BTreeMap::new(),
            servers: BTreeMap::new(),
            segment_tables: BTreeMap::new(),

            next_seqno: NextSeqNo::default(),
            events: VecDeque::new(),
            _device: device,
        };

        rpc::RpcServer::register(this.handle(), rpc_builder);
        Ok(this)
    }

    /// ローカルサーバの情報を返す。
    pub fn local_server(&self) -> &Server {
        &self.local_server
    }

    /// サービスを操作するためのハンドルを返す。
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            request_tx: self.request_tx.clone(),
        }
    }

    fn is_follower(&self) -> bool {
        self.rlog.local_node().role == raftlog::election::Role::Follower
    }

    fn handle_raft_event(&mut self, event: RaftEvent) -> Result<()> {
        if let raftlog::Event::Committed { index, .. } = event {
            self.snapshot_reduction = self.snapshot_reduction.saturating_sub(1);
            if self.snapshot_reduction == 0 {
                track!(self.take_snapshot())?;
            }

            track_assert_eq!(self.next_commit_index, index, ErrorKind::Other);
            self.next_commit_index = index + 1;
        }

        match event {
            raftlog::Event::Committed {
                index,
                entry: LogEntry::Command { term, command },
            } => {
                let proposal_id = ProposalId { term, index };
                let command =
                    track!(protobuf::command_decoder().decode_from_bytes(&command); command)?;
                track!(self.handle_command(proposal_id, command))?;
            }
            raftlog::Event::SnapshotLoaded { new_head, snapshot } => {
                info!(
                    self.logger,
                    "Snapshot loaded: {}",
                    dump!(new_head, snapshot.len())
                );
                self.next_commit_index = new_head.index;
                let snapshot = track!(protobuf::snapshot_decoder().decode_from_bytes(&snapshot))?;
                track!(self.handle_snapshot(snapshot))?;
            }
            // leader は `LogEntry::Noop` を待つ必要がある。
            // 詳細は `frugalos_mds::node::Node` のコメントを参照。
            raftlog::Event::NewLeaderElected if self.is_follower() => {
                let leader = &self.rlog.local_node().ballot.voted_for;
                let leader = track!(frugalos_raft::NodeId::from_raft_node_id(leader))?;
                info!(self.logger, "New leader is elected: {:?}", leader);
                self.change_leader(leader);
            }
            raftlog::Event::NewLeaderElected => {}
            _ => {
                use raftlog::election::Role;
                if let raftlog::Event::TermChanged { .. } = event {
                    info!(self.logger, "New term; reset the leader");
                    self.leader = None;
                }
                if let raftlog::Event::Committed {
                    entry: LogEntry::Noop { .. },
                    ..
                } = event
                {
                    // NOTE: `Noop`は選出直後にリーダが追加するコマンド
                    let leader = &self.rlog.local_node().ballot.voted_for;
                    info!(self.logger, "New leader is elected: {:?}", leader);

                    let leader = track!(frugalos_raft::NodeId::from_raft_node_id(leader))?;
                    self.change_leader(leader);
                }

                if let raftlog::Event::RoleChanged {
                    new_role: Role::Leader,
                } = event
                {
                    track!(self.sync_servers())?;
                }
                if let raftlog::Event::Committed {
                    entry: LogEntry::Config { .. },
                    ..
                } = event
                {
                    // TODO: sync_serversを呼ぶ箇所はもう少し精査する
                    track!(self.sync_servers())?;
                }
                info!(self.logger, "Raft event: {:?}", event)
            }
        }
        Ok(())
    }
    fn change_leader(&mut self, leader: frugalos_raft::NodeId) {
        for reply in self.leader_waiters.drain(..) {
            reply.exit(Ok(leader.addr));
        }
        self.leader = Some(leader.addr);
    }
    fn handle_command(&mut self, proposal_id: ProposalId, command: Command) -> Result<()> {
        info!(self.logger, "Committed: {}", dump!(proposal_id, command));

        match command {
            Command::PutServer { server } => self.handle_put_server(proposal_id, server),
            Command::DeleteServer { id } => self.handle_delete_server(proposal_id, id),
            Command::PutDevice { device } => self.handle_put_device(proposal_id, device),
            Command::DeleteDevice { id } => self.handle_delete_device(proposal_id, id),
            Command::PutBucket { bucket } => self.handle_put_bucket(proposal_id, bucket),
            Command::DeleteBucket { id } => self.handle_delete_bucket(proposal_id, &id),
        }
        Ok(())
    }
    fn handle_put_server(&mut self, proposal_id: ProposalId, mut server: Server) {
        // TODO: 新規作成と更新は明確に区別できた方が良いかも(事故防止のため)
        if let Some(old) = self.servers.remove(&server.id) {
            server.seqno = old.seqno;
            info!(self.logger, "Server is updated: {}", dump!(old, server));
        } else {
            server.seqno = self.next_seqno.server;
            self.next_seqno.server += 1;
            info!(self.logger, "New server is added: {:?}", server);
        }

        // SocketAddr の重複を禁止する
        if self.servers.iter().any(|(_, s)| s.addr() == server.addr()) {
            warn!(
                self.logger,
                "Server addr:{} already registered",
                server.addr()
            );
            if let Some(Proposal::PutServer { reply, .. }) =
                self.pop_committed_proposal(proposal_id)
            {
                let e = ErrorKind::InvalidInput
                    .cause(format!("Server addr:{} already registered", server.addr()));
                reply.exit(Err(Error::from(e)));
            }
            return;
        }

        if let Some(Proposal::PutServer { reply, .. }) = self.pop_committed_proposal(proposal_id) {
            reply.exit(Ok(server.clone()));
        }

        self.events.push_back(Event::PutServer(server.clone()));
        if self.servers.insert(server.id.clone(), server).is_none() {
            self.sync_servers().expect("TODO");
        }
    }
    fn handle_delete_server(&mut self, proposal_id: ProposalId, id: ServerId) {
        let deleted = if let Some(server) = self.servers.remove(&id) {
            // 削除対象サーバがどこからも参照されていないことを確認する
            let is_referred = self.devices.values().any(|d| match *d {
                Device::Virtual(_) => false,
                Device::Memory(ref d) => d.server == id,
                Device::File(ref d) => d.server == id,
            });
            if is_referred {
                warn!(
                    self.logger,
                    "Cannot DELETE this server (it is referred by some devices): {}",
                    dump!(id, server)
                );
                self.servers.insert(id, server);
                None
            } else {
                info!(self.logger, "Server is deleted: {}", dump!(id, server));
                self.events.push_back(Event::DeleteServer(server.clone()));
                Some(server)
            }
        } else {
            info!(self.logger, "No such server: {:?}", id);
            None
        };
        if deleted.is_some() {
            self.sync_servers().expect("TODO");
        }
        if let Some(Proposal::DeleteServer { reply, .. }) = self.pop_committed_proposal(proposal_id)
        {
            reply.exit(Ok(deleted))
        }
    }
    fn handle_put_device(&mut self, proposal_id: ProposalId, mut device: Device) {
        // https://github.com/frugalos/frugalos/issues/208 で発覚したバグのため、仮想デバイスであっても更新はできないようにする。
        if let Some(d) = self.devices.get(device.id()) {
            warn!(
                self.logger,
                "Updating device is not allowed: see issue #208 (https://github.com/frugalos/frugalos/issues/208): {}",
                dump!(proposal_id, device, d),
            );
            let _ = self.pop_committed_proposal(proposal_id); // TODO: ちゃんとハンドリング
            return;
        }

        if device.server().map(|s| !self.servers.contains_key(s)) == Some(true) {
            warn!(
                self.logger,
                "The device refers to undefined server: {}",
                dump!(device)
            );
            let _ = self.pop_committed_proposal(proposal_id); // TODO: ちゃんとハンドリング
            return;
        }
        if let Device::Virtual(ref d) = device {
            let unknowns = d
                .children
                .iter()
                .filter(|c| !self.devices.contains_key(c.as_str()))
                .cloned() // TODO: 無くす
                .collect::<Vec<_>>();
            if !unknowns.is_empty() {
                warn!(
                    self.logger,
                    "Virtual device includes unknown devices as children: {}",
                    dump!(proposal_id, device, unknowns)
                );
                let _ = self.pop_committed_proposal(proposal_id); // TODO: ちゃんとハンドリング
            }
        }
        // TODO: その他のバリデーション (e.g., 更新によってセグメントマッピングが失敗しないか)
        //       e.g., 循環参照禁止, DAG禁止(木のみに制限)

        // デバイスの更新を有効にするときは、ここを変更する。
        assert!(!self.devices.contains_key(device.id()));
        device.set_seqno(self.next_seqno.device);
        self.next_seqno.device += 1;
        info!(self.logger, "New device is added: {:?}", device);

        // TODO: 更新時にしか走らない処理があるので、消すか望ましい更新処理を実装する。
        let mut affected_buckets = Vec::new();
        for b in self.buckets.values() {
            let root = &self.devices[b.device()];
            if b.device() == device.id() || self.is_device_included(root, device.id()) {
                affected_buckets.push(b.id().clone());
            }
        }
        for bucket in affected_buckets {
            info!(
                self.logger,
                "Update segment table: {}",
                dump!(bucket, device.id())
            );
            self.update_segment_table(&bucket);
        }

        if let Some(Proposal::PutDevice { reply, .. }) = self.pop_committed_proposal(proposal_id) {
            reply.exit(Ok(device.clone()));
        }
        self.events.push_back(Event::PutDevice(device.clone()));
        self.devices.insert(device.id().clone(), device);
    }
    fn handle_delete_device(&mut self, proposal_id: ProposalId, id: DeviceId) {
        let deleted = if let Some(device) = self.devices.remove(&id) {
            // 削除対象デバイスがどこからも参照されていないことを確認する
            if self.is_device_referred(&id) {
                warn!(
                    self.logger,
                    "Cannot DELETE this device (it is referred to by some devices): {}",
                    dump!(id, device),
                );
                self.devices.insert(id, device);
                None
            } else {
                info!(self.logger, "Device is deleted: {}", dump!(id, device));
                self.events.push_back(Event::DeleteDevice(device.clone()));
                Some(device)
            }
        } else {
            info!(self.logger, "No such device: {:?}", id);
            None
        };
        if let Some(Proposal::DeleteDevice { reply, .. }) = self.pop_committed_proposal(proposal_id)
        {
            reply.exit(Ok(deleted))
        }
    }
    fn handle_put_bucket(&mut self, proposal_id: ProposalId, mut bucket: Bucket) {
        // TODO: 最低限`MetadataBucket`は更新可能にする
        if self.buckets.contains_key(bucket.id()) {
            warn!(
                self.logger,
                "Cannot update this bucket: {}",
                dump!(proposal_id, bucket)
            );
            let _ = self.pop_committed_proposal(proposal_id); // TODO: ちゃんとハンドリング
            return;
        }
        if !self.devices.contains_key(bucket.device()) {
            warn!(
                self.logger,
                "Referred device does not exist: {}",
                dump!(proposal_id, bucket)
            );
            let _ = self.pop_committed_proposal(proposal_id); // TODO: ちゃんとハンドリング
            return;
        }
        // TODO: セグメント群が適切に構築可能かどうかも事前にチェックする

        bucket.fix_segment_count(self.devices.len()); // TODO: 参照している物理デバイス数を渡す
        bucket.set_seqno(self.next_seqno.bucket);
        self.next_seqno.bucket += 1;
        info!(self.logger, "New bucket is added: {:?}", bucket);

        // TODO: セグメントテーブルを構築
        self.buckets.insert(bucket.id().clone(), bucket.clone());
        self.events.push_back(Event::PutBucket(bucket.clone()));
        self.update_segment_table(bucket.id());

        if let Some(Proposal::PutBucket { reply, .. }) = self.pop_committed_proposal(proposal_id) {
            reply.exit(Ok(bucket));
        }
    }
    #[allow(clippy::ptr_arg)]
    fn handle_delete_bucket(&mut self, proposal_id: ProposalId, id: &BucketId) {
        let deleted = if let Some(bucket) = self.buckets.remove(id) {
            info!(self.logger, "Bucket is deleted: {}", dump!(id, bucket));
            self.delete_segment_table(&bucket);
            self.events.push_back(Event::DeleteBucket(bucket.clone()));
            Some(bucket)
        } else {
            info!(self.logger, "No such bucket: {:?}", id);
            None
        };
        if let Some(Proposal::DeleteBucket { reply, .. }) = self.pop_committed_proposal(proposal_id)
        {
            reply.exit(Ok(deleted))
        }
    }
    fn handle_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        // TODO: 以下が成立しないケースにも対応する (proposalsの中身を調整するだけ)
        track_assert_eq!(self.proposals.len(), 0, ErrorKind::Other);

        self.next_seqno = snapshot.next_seqno;

        let old_buckets = mem::take(&mut self.buckets);
        let old_devices = mem::take(&mut self.devices);
        let old_servers = mem::take(&mut self.servers);
        self.buckets = snapshot
            .buckets
            .into_iter()
            .map(|b| (b.id().to_owned(), b))
            .collect();
        self.devices = snapshot
            .devices
            .into_iter()
            .map(|d| (d.id().to_owned(), d))
            .collect();
        self.servers = snapshot
            .servers
            .into_iter()
            .map(|s| (s.id.clone(), s))
            .collect();
        self.segment_tables = snapshot
            .segment_tables
            .into_iter()
            .map(|s| (s.bucket_id.clone(), s))
            .collect();
        info!(
            self.logger,
            "Snapshot is loaded: {}",
            dump!(
                self.next_seqno,
                self.buckets.len(),
                self.devices.len(),
                self.servers.len()
            )
        );

        // TODO: 差分チェック(完全に一致しているエントリ以外はスキップしない)
        //       (最終的には、スキップを完全に不要にするのが望ましい)
        for s in self.servers.values() {
            if old_servers.contains_key(&s.id) {
                info!(self.logger, "The server {:?} already exists", s.id);
                continue;
            }
            self.events.push_back(Event::PutServer(s.clone()));
        }
        for d in self.devices.values() {
            if old_devices.contains_key(d.id()) {
                info!(self.logger, "The device {:?} already exists", d.id());
                continue;
            }
            self.events.push_back(Event::PutDevice(d.clone()));
        }
        for b in self.buckets.values() {
            if old_buckets.contains_key(b.id()) {
                info!(self.logger, "The bucket {:?} already exists", b.id());
                continue;
            }
            self.events.push_back(Event::PutBucket(b.clone()));
            for (segment_no, segment) in self.segment_tables[b.id()].segments.iter().enumerate() {
                self.events.push_back(Event::PatchSegment {
                    bucket_no: b.seqno(),
                    segment_no: segment_no as u16,
                    groups: segment.groups.clone(),
                });
            }
        }
        // 古い bucket が存在する場合は削除する
        for b in old_buckets.values() {
            if self.buckets.contains_key(b.id()) {
                continue;
            }
            let bucket = b.clone();
            info!(self.logger, "Bucket is deleted: {}", dump!(b.id(), bucket));
            self.delete_segment_table(&bucket);
            self.events.push_back(Event::DeleteBucket(bucket.clone()));
        }

        track!(self.sync_servers())?;
        Ok(())
    }
    fn take_snapshot(&mut self) -> Result<()> {
        if self.rlog.is_snapshot_installing() {
            return Ok(());
        }
        self.snapshot_reduction = SNAPSHOT_THRESHOLD;

        let snapshot = Snapshot {
            next_seqno: self.next_seqno.clone(),
            buckets: self.buckets.values().cloned().collect(),
            devices: self.devices.values().cloned().collect(),
            servers: self.servers.values().cloned().collect(),
            segment_tables: self.segment_tables.values().cloned().collect(),
        };
        let snapshot = track!(protobuf::snapshot_encoder().encode_into_bytes(snapshot))?;
        track!(self.rlog.install_snapshot(self.next_commit_index, snapshot))?;
        Ok(())
    }
    fn handle_request(&mut self, request: Request) -> Result<()> {
        info!(self.logger, "Request: {:?}", request);
        match request {
            Request::GetLeader { .. } => {}
            _ => {
                if let Err(e) = self.check_leader() {
                    request.failed(e);
                    return Ok(());
                }
            }
        }
        match request {
            Request::GetLeader { reply } => {
                info!(self.logger, "Leader is {:?}", self.leader);
                if let Some(leader) = self.leader {
                    reply.exit(Ok(leader));
                } else {
                    self.leader_waiters.push(reply);
                    if self.leader_waiters.len() > self.leader_waiters_threshold {
                        warn!(self.logger, "Too many waitings (cleared)");
                        self.clear_leader_waiters();
                    }
                }
            }
            Request::ListServers { reply } => {
                reply.exit(Ok(self.servers.values().map(Server::to_summary).collect()));
            }
            Request::GetServer { id, reply } => reply.exit(Ok(self.servers.get(&id).cloned())),
            Request::PutServer { server, reply } => {
                let command = Command::PutServer { server };
                match track!(self.propose_command(command)) {
                    Err(e) => reply.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::PutServer { proposal_id, reply };
                        self.proposals.push_back(proposal);
                    }
                }
            }
            Request::DeleteServer { id, reply } => {
                let command = Command::DeleteServer { id };
                match track!(self.propose_command(command)) {
                    Err(e) => reply.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::DeleteServer { proposal_id, reply };
                        self.proposals.push_back(proposal);
                    }
                }
            }
            Request::ListDevices { reply } => {
                reply.exit(Ok(self.devices.values().map(Device::to_summary).collect()));
            }
            Request::GetDevice { id, reply } => reply.exit(Ok(self.devices.get(&id).cloned())),
            Request::PutDevice { device, reply } => {
                let command = Command::PutDevice { device };
                match track!(self.propose_command(command)) {
                    Err(e) => reply.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::PutDevice { proposal_id, reply };
                        self.proposals.push_back(proposal);
                    }
                }
            }
            Request::DeleteDevice { id, reply } => {
                let command = Command::DeleteDevice { id };
                match track!(self.propose_command(command)) {
                    Err(e) => reply.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::DeleteDevice { proposal_id, reply };
                        self.proposals.push_back(proposal);
                    }
                }
            }
            Request::ListBuckets { reply } => {
                reply.exit(Ok(self.buckets.values().map(Bucket::to_summary).collect()));
            }
            Request::GetBucket { id, reply } => reply.exit(Ok(self.buckets.get(&id).cloned())),
            Request::PutBucket { bucket, reply } => {
                let command = Command::PutBucket { bucket };
                match track!(self.propose_command(command)) {
                    Err(e) => reply.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::PutBucket { proposal_id, reply };
                        self.proposals.push_back(proposal);
                    }
                }
            }
            Request::DeleteBucket { id, reply } => {
                let command = Command::DeleteBucket { id };
                match track!(self.propose_command(command)) {
                    Err(e) => reply.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::DeleteBucket { proposal_id, reply };
                        self.proposals.push_back(proposal);
                    }
                }
            }
        }
        Ok(())
    }
    fn propose_command(&mut self, command: Command) -> Result<ProposalId> {
        info!(self.logger, "Propose: {}", dump!(command));
        let command = track!(protobuf::command_encoder().encode_into_bytes(command))?;
        let id = track!(self.rlog.propose_command(command))?;
        Ok(id)
    }
    fn pop_committed_proposal(&mut self, proposal_id: ProposalId) -> Option<Proposal> {
        if let Some(proposal) = self.proposals.pop_front() {
            if proposal.id() == proposal_id {
                return Some(proposal);
            } else if proposal.id().index <= proposal_id.index {
                warn!(
                    self.logger,
                    "This proposal is rejected: {}",
                    dump!(proposal.id())
                );
            } else {
                self.proposals.push_front(proposal);
            }
        }
        None
    }

    /// 登録済みサーバ群とRaftのメンバ構成を一致させる
    fn sync_servers(&mut self) -> Result<()> {
        use raftlog::election::Role;
        use std::collections::BTreeSet;
        if self.servers.is_empty() {
            return Ok(());
        }
        if self.rlog.local_node().role != Role::Leader {
            return Ok(());
        }

        // TODO: 効率化
        let members = self
            .servers
            .values()
            .map(|s| server_to_frugalos_raft_node(&s).to_raft_node_id())
            .collect::<BTreeSet<_>>();
        if members == *self.rlog.cluster_config().new_members() {
            return Ok(());
        }

        info!(
            self.logger,
            "Propose new Raft cluster configuration: {:?}", members
        );
        track!(self.rlog.propose_config(members))?;
        Ok(())
    }

    #[allow(clippy::ptr_arg)]
    fn update_segment_table(&mut self, bucket_id: &BucketId) {
        info!(self.logger, "[START] update_segment_table: {:?}", bucket_id);
        // TODO: 登録数が多くなるとそこそこ時間が掛かる可能性があるので
        //       別スレッドで実行した方が良いかもしれない.

        let _old_table = self
            .segment_tables
            .remove(bucket_id)
            .unwrap_or_else(|| SegmentTable::new(bucket_id.clone()));
        {
            let bucket = &self.buckets[bucket_id];
            let builder = SegmentTableBuilder::new(&self.devices);

            // TODO: error handling
            let new_table = track_try_unwrap!(builder.build(bucket));

            // TODO: 新旧の差分を取って云々
            for (segment_no, segment) in new_table.segments.iter().enumerate() {
                self.events.push_back(Event::PatchSegment {
                    bucket_no: bucket.seqno(),
                    segment_no: segment_no as u16,
                    groups: segment.groups.clone(),
                });
            }

            self.segment_tables.insert(bucket_id.clone(), new_table);
            info!(self.logger, "[FINISH] update_segment_table");
        }
        // NOTE: セグメント更新は重い処理なので、常にスナップショットを取る
        // TODO: error handling
        track_try_unwrap!(self.take_snapshot());
    }
    fn delete_segment_table(&mut self, bucket: &Bucket) {
        for (segment_no, segment) in self.segment_tables[bucket.id()].segments.iter().enumerate() {
            self.events.push_back(Event::DeleteSegment {
                bucket_no: bucket.seqno(),
                segment_no: segment_no as u16,
                groups: segment.groups.clone(),
            });
        }
        self.segment_tables.remove(bucket.id());
    }

    #[allow(clippy::ptr_arg)]
    fn is_device_referred(&self, id: &DeviceId) -> bool {
        self.buckets.values().any(|b| {
            b.device() == id || {
                let d = &self.devices[b.device()];
                self.is_device_included(d, id)
            }
        })
    }

    #[allow(clippy::ptr_arg)]
    fn is_device_included(&self, parent: &Device, id: &DeviceId) -> bool {
        if let Device::Virtual(ref d) = *parent {
            d.children.iter().any(|child_id| {
                child_id == id || {
                    let d = &self.devices[child_id];
                    self.is_device_included(d, id)
                }
            })
        } else {
            false
        }
    }

    fn check_leader(&self) -> Result<()> {
        use raftlog::election::Role;
        track_assert_eq!(
            self.rlog.local_node().role,
            Role::Leader,
            ErrorKind::NotLeader
        );
        Ok(())
    }

    fn clear_leader_waiters(&mut self) {
        for x in self.leader_waiters.drain(..) {
            let e = track!(Error::from(
                ErrorKind::Other.cause("Leader waiting timeout")
            ));
            x.exit(Err(e));
        }
    }
}
impl Stream for Service {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Async::Ready(request) = self.request_rx.poll().expect("Never fails") {
            let request = request.expect("Never fails");
            track!(self.handle_request(request))?;
        }
        while let Async::Ready(event) = track!(self.rlog.poll())? {
            let event = event.expect("Never fails");
            track!(self.handle_raft_event(event))?;
        }
        if let Some(event) = self.events.pop_front() {
            return Ok(Async::Ready(Some(event)));
        }
        Ok(Async::NotReady)
    }
}

/// サービスが発行するイベント。
#[allow(missing_docs)]
#[derive(Debug)]
pub enum Event {
    PutBucket(Bucket),
    DeleteBucket(Bucket),
    PutDevice(Device),
    DeleteDevice(Device),
    PutServer(Server),
    DeleteServer(Server),
    PatchSegment {
        bucket_no: u32,
        segment_no: u16,
        groups: Vec<DeviceGroup>,
    },
    DeleteSegment {
        bucket_no: u32,
        segment_no: u16,
        groups: Vec<DeviceGroup>,
    },
}

#[derive(Debug)]
enum Request {
    GetLeader {
        reply: Reply<SocketAddr>,
    },
    ListServers {
        reply: Reply<Vec<ServerSummary>>,
    },
    GetServer {
        id: ServerId,
        reply: Reply<Option<Server>>,
    },
    PutServer {
        server: Server,
        reply: Reply<Server>, // `seqno`が採番された結果が返る
    },
    DeleteServer {
        id: ServerId,
        reply: Reply<Option<Server>>,
    },
    ListDevices {
        reply: Reply<Vec<DeviceSummary>>,
    },
    GetDevice {
        id: DeviceId,
        reply: Reply<Option<Device>>,
    },
    PutDevice {
        device: Device,
        reply: Reply<Device>,
    },
    DeleteDevice {
        id: DeviceId,
        reply: Reply<Option<Device>>,
    },
    ListBuckets {
        reply: Reply<Vec<BucketSummary>>,
    },
    GetBucket {
        id: BucketId,
        reply: Reply<Option<Bucket>>,
    },
    PutBucket {
        bucket: Bucket,
        reply: Reply<Bucket>,
    },
    DeleteBucket {
        id: BucketId,
        reply: Reply<Option<Bucket>>,
    },
}
impl Request {
    pub fn failed(self, e: Error) {
        match self {
            Request::GetLeader { reply } => reply.exit(Err(track!(e))),
            Request::ListServers { reply } => reply.exit(Err(track!(e))),
            Request::GetServer { reply, .. } => reply.exit(Err(track!(e))),
            Request::PutServer { reply, .. } => reply.exit(Err(track!(e))),
            Request::DeleteServer { reply, .. } => reply.exit(Err(track!(e))),
            Request::ListDevices { reply } => reply.exit(Err(track!(e))),
            Request::GetDevice { reply, .. } => reply.exit(Err(track!(e))),
            Request::PutDevice { reply, .. } => reply.exit(Err(track!(e))),
            Request::DeleteDevice { reply, .. } => reply.exit(Err(track!(e))),
            Request::ListBuckets { reply } => reply.exit(Err(track!(e))),
            Request::GetBucket { reply, .. } => reply.exit(Err(track!(e))),
            Request::PutBucket { reply, .. } => reply.exit(Err(track!(e))),
            Request::DeleteBucket { reply, .. } => reply.exit(Err(track!(e))),
        }
    }
}
type Reply<T> = oneshot::Monitored<T, Error>;

#[derive(Debug)]
enum Proposal {
    PutServer {
        proposal_id: ProposalId,
        reply: Reply<Server>,
    },
    DeleteServer {
        proposal_id: ProposalId,
        reply: Reply<Option<Server>>,
    },
    PutDevice {
        proposal_id: ProposalId,
        reply: Reply<Device>,
    },
    DeleteDevice {
        proposal_id: ProposalId,
        reply: Reply<Option<Device>>,
    },
    PutBucket {
        proposal_id: ProposalId,
        reply: Reply<Bucket>,
    },
    DeleteBucket {
        proposal_id: ProposalId,
        reply: Reply<Option<Bucket>>,
    },
}
impl Proposal {
    pub fn id(&self) -> ProposalId {
        match *self {
            Proposal::PutServer { proposal_id, .. } => proposal_id,
            Proposal::DeleteServer { proposal_id, .. } => proposal_id,
            Proposal::PutDevice { proposal_id, .. } => proposal_id,
            Proposal::DeleteDevice { proposal_id, .. } => proposal_id,
            Proposal::PutBucket { proposal_id, .. } => proposal_id,
            Proposal::DeleteBucket { proposal_id, .. } => proposal_id,
        }
    }
}

#[derive(Debug)]
struct Response<T>(oneshot::Monitor<T, Error>);
impl<T> Response<T> {
    fn new() -> (Reply<T>, Self) {
        let (tx, rx) = oneshot::monitor();
        (tx, Response(rx))
    }
}
impl<T> Future for Response<T> {
    type Item = T;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self.0.poll().map_err(Error::from))
    }
}

/// サービスを操作するためのハンドル。
#[derive(Debug, Clone)]
pub struct ServiceHandle {
    request_tx: mpsc::Sender<Request>,
}
impl ServiceHandle {
    /// リーダノードのアドレスを取得する。
    pub fn get_leader(&self) -> impl Future<Item = SocketAddr, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::GetLeader { reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// クラスタ内のサーバ一覧を返す。
    pub fn list_servers(&self) -> impl Future<Item = Vec<ServerSummary>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::ListServers { reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// IDに対応するサーバの情報を返す。
    pub fn get_server(&self, id: ServerId) -> impl Future<Item = Option<Server>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::GetServer { id, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// サーバをクラスタに追加する。
    pub fn put_server(&self, server: Server) -> impl Future<Item = Server, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::PutServer { server, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// サーバをクラスタから削除する。
    pub fn delete_server(&self, id: ServerId) -> impl Future<Item = Option<Server>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::DeleteServer { id, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// クラスタ内のデバイス一覧を取得する。
    pub fn list_devices(&self) -> impl Future<Item = Vec<DeviceSummary>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::ListDevices { reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// IDに対応するデバイスを取得する。
    pub fn get_device(&self, id: DeviceId) -> impl Future<Item = Option<Device>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::GetDevice { id, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// デバイスを登録する。
    pub fn put_device(&self, device: Device) -> impl Future<Item = Device, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::PutDevice { device, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// デバイスを削除する。
    pub fn delete_device(&self, id: DeviceId) -> impl Future<Item = Option<Device>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::DeleteDevice { id, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// クラスタ内のバケツ一覧を取得する。
    pub fn list_buckets(&self) -> impl Future<Item = Vec<BucketSummary>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::ListBuckets { reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// IDに対応するバケツの情報を取得する。
    pub fn get_bucket(&self, id: BucketId) -> impl Future<Item = Option<Bucket>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::GetBucket { id, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// バケツを登録する。
    pub fn put_bucket(&self, bucket: Bucket) -> impl Future<Item = Bucket, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::PutBucket { bucket, reply };
        let _ = self.request_tx.send(request);
        response
    }

    /// バケツを削除する。
    pub fn delete_bucket(&self, id: BucketId) -> impl Future<Item = Option<Bucket>, Error = Error> {
        let (reply, response) = Response::new();
        let request = Request::DeleteBucket { id, reply };
        let _ = self.request_tx.send(request);
        response
    }
}
