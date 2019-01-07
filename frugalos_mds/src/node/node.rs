#![allow(clippy::type_complexity)]
use bytecodec::{DecodeExt, EncodeExt};
use fibers::sync::mpsc;
use fibers::sync::oneshot::Monitored;
use fibers::time::timer;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use fibers_tasque::{self, AsyncCall, TaskQueueExt};
use frugalos_raft::{NodeId, RaftIo};
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::object::{Metadata, ObjectVersion};
use prometrics::metrics::{Counter, CounterBuilder, Gauge, GaugeBuilder};
use raftlog::cluster::{ClusterConfig, ClusterMembers};
use raftlog::election::Role;
use raftlog::log::{LogEntry, LogIndex, LogPosition};
use raftlog::{self, ReplicatedLog};
use slog::Logger;
use std::collections::VecDeque;
use std::env;
use std::time::Duration;

use super::{Event, NodeHandle, Proposal, Request, Seconds};
use codec;
use machine::{Command, Machine};
use protobuf;
use {Error, ErrorKind, Result, ServiceHandle};

const DEFAULT_SNAPSHOT_THRESHOLD: usize = 10_000;
const DEFAULT_REELECTION_THRESHOLD: usize = 10; // 10 * 500ms = 10s
const DEFAULT_LARGE_QUEUE_THRESHOLD: usize = 1024;

type RaftEvent = raftlog::Event;

#[derive(Clone)]
struct Metrics {
    objects: Gauge,
    snapshots_total: Counter,
    snapshot_bytes_total: Counter,
    proposal_queue_len: Gauge,
}
impl Metrics {
    pub fn new(node_id: &NodeId) -> Result<Self> {
        let node = node_id.to_string();
        let objects = track!(GaugeBuilder::new("objects")
            .namespace("frugalos")
            .subsystem("mds")
            .label("node", &node)
            .label("role", "Follower")
            .default_registry()
            .finish())?;
        let proposal_queue_len = track!(GaugeBuilder::new("proposal_queue_len")
            .namespace("frugalos")
            .subsystem("mds")
            .label("node", &node)
            .default_registry()
            .finish())?;
        let snapshots_total = track!(CounterBuilder::new("snapshots_total")
            .namespace("frugalos")
            .subsystem("mds")
            .label("node", &node)
            .default_registry()
            .finish())?;
        let snapshot_bytes_total = track!(CounterBuilder::new("snapshot_bytes_total")
            .namespace("frugalos")
            .subsystem("mds")
            .label("node", &node)
            .default_registry()
            .finish())?;
        Ok(Metrics {
            objects,
            snapshots_total,
            snapshot_bytes_total,
            proposal_queue_len,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Running,
    Stopping,
    Stopped,
}

/// MDSのクラスタを形成する個々のノード.
///
/// この構造体は`Stream`トレイトを実装しており、
/// MDSが受理したイベント(i.e., メタデータオブジェクトの追加と削除)列を生成する.
/// なお、これは無限ストリームであり、エラー時を除いて自発的に終了することはない.
pub struct Node {
    logger: Logger,
    service: ServiceHandle,
    node_id: NodeId,
    rlog: ReplicatedLog<RaftIo>,
    leader: Option<NodeId>,
    leader_waitings: Vec<Monitored<NodeId, Error>>,
    leader_waiting_timeout: usize,
    request_rx: mpsc::Receiver<Request>,
    proposals: VecDeque<Proposal>,
    local_log_size: usize,
    snapshot_threshold: usize,
    next_commit: LogIndex,
    last_commit: Option<LogIndex>,
    events: VecDeque<Event>,
    machine: Machine,
    metrics: Metrics,
    ready_snapshot: Option<AsyncCall<Result<(LogIndex, Vec<u8>)>>>,
    decoding_snapshot: Option<AsyncCall<Result<(LogPosition, Machine, Vec<ObjectVersion>)>>>,
    polling_timer: timer::Timeout,
    phase: Phase,
    rpc_service: RpcServiceHandle,

    // リーダが重い場合に再選出を行うための変数群
    large_queue_rounds: usize,
    large_queue_threshold: usize,
    reelection_threshold: usize,
    commit_timeout: Option<usize>,
}
impl Node {
    /// 新しい`Node`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        service: ServiceHandle,
        node_id: NodeId,
        cluster: ClusterMembers,
        io: RaftIo,
        rpc_service: RpcServiceHandle,
    ) -> Result<Self> {
        let (request_tx, request_rx) = mpsc::channel();
        let node_handle = NodeHandle::new(request_tx.clone());
        track!(service.add_node(node_id, node_handle))?;

        let rlog = ReplicatedLog::new(node_id.to_raft_node_id(), cluster, io);

        // TODO: 関数引数経由で渡せるようにする(or setterを用意)
        let snapshot_threshold = env::var("FRUGALOS_SNAPSHOT_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SNAPSHOT_THRESHOLD);
        let reelection_threshold = env::var("FRUGALOS_REELECTION_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_REELECTION_THRESHOLD);
        let large_queue_threshold = env::var("FRUGALOS_LARE_QUEUE_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_LARGE_QUEUE_THRESHOLD);
        info!(
            logger,
            "Thresholds: snapshot={}, reelection={}, queue={}",
            snapshot_threshold,
            reelection_threshold,
            large_queue_threshold
        );

        let metrics = track!(Metrics::new(&node_id))?;
        Ok(Node {
            logger,
            service,
            node_id,
            rlog,
            leader: None,
            leader_waitings: Vec::new(),
            leader_waiting_timeout: 0,
            request_rx,
            proposals: VecDeque::new(),
            local_log_size: 0,
            snapshot_threshold,
            next_commit: LogIndex::new(0),
            last_commit: None,
            events: VecDeque::new(),
            machine: Machine::new(),
            metrics,
            ready_snapshot: None,
            decoding_snapshot: None,
            polling_timer: timer::timeout(Duration::from_millis(500)),
            phase: Phase::Running,
            large_queue_rounds: 0,
            large_queue_threshold,
            reelection_threshold,
            commit_timeout: None,
            rpc_service,
        })
    }

    /// スナップショットを取る際の閾値を取得する.
    ///
    /// ローカルログの長さが、この値を超えた場合に、スナップショットの取得が開始される.
    /// `set_snapshot_threshold`メソッドが呼び出されていない場合には、
    /// デフォルト値である`10_000`が返される.
    pub fn snapshot_threshold(&self) -> usize {
        self.snapshot_threshold
    }

    /// スナップショットを取る際の閾値を変更する.
    pub fn set_snapshot_threshold(&mut self, threshold: usize) {
        self.snapshot_threshold = threshold;
    }

    fn handle_request(&mut self, request: Request) {
        // NOTE: 整合性を保証したいので、要求を処理できるのはリーダのみとする.
        // TODO: リースないしハートビートを使って、leaderであることを保証する (READ時)
        match request {
            Request::GetLeader(_)
            | Request::Stop
            | Request::TakeSnapshot
            | Request::StartElection => {}
            _ => {
                if let Err(e) = self.check_leader() {
                    request.failed(e);
                    return;
                }
            }
        }

        match request {
            Request::StartElection => {
                info!(self.logger, "Re-election is required");
                self.rlog.start_election();
            }
            Request::GetLeader(monitored) => {
                // TODO: debugレベルにする
                info!(self.logger, "GetLeader: {:?}", self.leader);
                if let Some(leader) = self.leader {
                    monitored.exit(Ok(leader));
                } else {
                    if self.leader_waitings.is_empty() {
                        self.leader_waiting_timeout = 10; // TODO:
                    }
                    self.leader_waitings.push(monitored);
                    if self.leader_waitings.len() > 10000 {
                        // NOTE: リーダが収束しない状態で無限にメモリを消費してしまうことがないようにする
                        // TODO: もう少しちゃんと制御（e.g., timeout)
                        warn!(self.logger, "Too many waitings (cleared)");
                        self.leader_waitings.clear();
                    }
                }
            }
            Request::List(monitored) => {
                let list = self.machine.to_summaries();
                monitored.exit(Ok(list));
            }
            Request::LatestVersion(monitored) => {
                let latest = self.machine.latest_version();
                monitored.exit(Ok(latest));
            }
            Request::ObjectCount(monitored) => monitored.exit(Ok(self.machine.len() as u64)),
            Request::Get(object_id, expect, monitored) => {
                let result = self
                    .check_leader()
                    .and_then(|()| self.machine.get(&object_id, &expect));
                monitored.exit(result);
            }
            Request::Head(object_id, expect, monitored) => {
                let result = self
                    .check_leader()
                    .and_then(|()| self.machine.head(&object_id, &expect));
                monitored.exit(result);
            }
            Request::Put(object_id, data, expect, put_content_timeout, monitored) => {
                let command = Command::Put {
                    object_id,
                    userdata: data,
                    expect,
                    put_content_timeout,
                };
                let result = track!(protobuf::command_encoder().encode_into_bytes(command))
                    .map_err(Error::from)
                    .and_then(|c| track!(self.rlog.propose_command(c)).map_err(Error::from));
                match result {
                    Err(e) => monitored.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::Put(proposal_id, monitored);
                        self.push_proposal(proposal);
                    }
                }
            }
            Request::Delete(object_id, expect, monitored) => {
                let command = Command::Delete { object_id, expect };
                let result = track!(protobuf::command_encoder().encode_into_bytes(command))
                    .map_err(Error::from)
                    .and_then(|c| track!(self.rlog.propose_command(c)).map_err(Error::from));
                match result {
                    Err(e) => monitored.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::Delete(proposal_id, monitored);
                        self.push_proposal(proposal);
                    }
                }
            }
            Request::DeleteByVersion(object_version, monitored) => {
                let command = Command::DeleteByVersion { object_version };
                let result = track!(protobuf::command_encoder().encode_into_bytes(command))
                    .map_err(Error::from)
                    .and_then(|c| track!(self.rlog.propose_command(c)).map_err(Error::from));
                match result {
                    Err(e) => monitored.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::Delete(proposal_id, monitored);
                        self.push_proposal(proposal);
                    }
                }
            }
            // 次の DeleteByRange のcaseに、現在の実装で到達することはない。
            // 今後のためのスケルトン
            // 現時点で実装できない理由は、 ここでコマンドを発行しても
            // それを処理する handle_command の返値型が Option型 になっており、
            // DeleteByRange の monitored が要求している Vec型 に対して情報が少なすぎるため。
            Request::DeleteByRange(version_from, version_to, _monitored) => {
                let command = Command::DeleteByRange {
                    version_from,
                    version_to,
                };
                let result = track!(protobuf::command_encoder().encode_into_bytes(command))
                    .map_err(Error::from)
                    .and_then(|c| track!(self.rlog.propose_command(c)).map_err(Error::from));

                match result {
                    Err(_e) => {
                        unreachable!();
                    }
                    Ok(_proposal_id) => {
                        unreachable!();
                    }
                }
            }
            Request::DeleteByPrefix(prefix, monitored) => {
                let command = Command::DeleteByPrefix {
                    prefix: prefix.clone(),
                };
                let result = track!(protobuf::command_encoder().encode_into_bytes(command))
                    .map_err(Error::from)
                    .and_then(|c| track!(self.rlog.propose_command(c)).map_err(Error::from));

                match result {
                    Err(e) => monitored.exit(Err(e)),
                    Ok(proposal_id) => {
                        let proposal = Proposal::DeleteByPrefix(proposal_id, prefix, monitored);
                        self.push_proposal(proposal);
                    }
                }
            }
            Request::Stop => {
                if self.phase == Phase::Running {
                    info!(self.logger, "Starts stopping the node");
                    match track!(self.take_snapshot()) {
                        Err(e) => {
                            error!(self.logger, "Cannot take snapshot: {}", e);

                            // スナップショットが取得できないなら即座に終了
                            self.phase = Phase::Stopped;
                        }
                        Ok(false) => {
                            self.phase = Phase::Stopped;
                        }
                        Ok(true) => {
                            self.phase = Phase::Stopping;
                        }
                    }
                }
            }
            Request::TakeSnapshot => {
                if let Err(e) = track!(self.take_snapshot()) {
                    error!(self.logger, "Cannot take snapshot: {}", e);
                }
            }
        }
    }
    fn take_snapshot(&mut self) -> Result<bool> {
        let commit = if let Some(commit) = self.last_commit {
            if commit.as_u64() == 0 {
                // FIXME: `raftlog`のバグで、この状態でsnapshotを取得すると再起動時に
                // 無限ループに入ってしまうので、暫定対処.
                return Ok(false);
            }
            commit
        } else {
            return Ok(false);
        };
        if !self.rlog.is_snapshot_installing() && self.ready_snapshot.is_none() {
            self.local_log_size = 0;
            info!(
                self.logger,
                "Starts taking snapshot: objects={}",
                self.machine.len()
            );

            // TODO: 完全にインクリメンタルにする
            let machine = self.machine.clone();
            info!(self.logger, "Snapshot cloned");

            let logger = self.logger.clone();
            let metrics = self.metrics.clone();
            let future = fibers_tasque::DefaultCpuTaskQueue.async_call(move || {
                let snapshot = track!(codec::encode_machine(&machine))?;
                info!(
                    logger,
                    "Converted: machine to snapshot: {} bytes",
                    snapshot.len(),
                );

                metrics.snapshots_total.increment();
                metrics.snapshot_bytes_total.add_u64(snapshot.len() as u64);

                Ok((commit, snapshot))
            });
            self.ready_snapshot = Some(future);
        }
        Ok(true)
    }
    fn push_proposal(&mut self, proposal: Proposal) {
        while let Some(last) = self.proposals.pop_back() {
            if last.id().index < proposal.id().index {
                self.proposals.push_back(last);
                break;
            }
            warn!(self.logger, "This proposal is rejected: {:?}", last.id());
            last.notify_rejected();
        }
        self.proposals.push_back(proposal);
        if self.commit_timeout.is_none() {
            self.commit_timeout = Some(30); // TODO: parameter
        }
    }
    fn check_leader(&self) -> Result<()> {
        track_assert_eq!(
            self.rlog.local_node().role,
            Role::Leader,
            ErrorKind::NotLeader
        );
        Ok(())
    }
    fn handle_raft_event(&mut self, event: RaftEvent) -> Result<()> {
        use raftlog::Event as E;
        trace!(self.logger, "New raft event: {:?}", event);
        match event {
            E::RoleChanged { new_role } => {
                info!(self.logger, "New raft role: {:?}", new_role);
                let role = format!("{:?}", new_role);
                track!(self.metrics.objects.labels_mut().insert("role", &role))?;
            }
            E::TermChanged { new_ballot } => {
                info!(
                    self.logger,
                    "New raft election term: ballot={:?}", new_ballot
                );
                self.leader = None;
            }
            E::Committed { index, entry } => track!(self.handle_committed(index, entry))?,
            E::SnapshotLoaded { new_head, snapshot } => {
                info!(
                    self.logger,
                    "New snapshot is loaded: new_head={:?}, bytes={}",
                    new_head,
                    snapshot.len()
                );
                let logger = self.logger.clone();
                let future = fibers_tasque::DefaultCpuTaskQueue.async_call(move || {
                    let machine = track!(codec::decode_machine(&snapshot))?;
                    let versions = machine.to_versions();
                    info!(logger, "Snapshot decoded: {} bytes", snapshot.len());
                    Ok((new_head, machine, versions))
                });
                self.decoding_snapshot = Some(future);
            }
            E::SnapshotInstalled { new_head } => {
                info!(
                    self.logger,
                    "New snapshot is installed: new_head={:?}, phase={:?}", new_head, self.phase
                );
                if self.phase == Phase::Stopping {
                    self.phase = Phase::Stopped;
                }
            }
        }
        Ok(())
    }
    fn handle_committed(&mut self, commit: LogIndex, entry: LogEntry) -> Result<()> {
        track_assert_eq!(self.next_commit, commit, ErrorKind::InvalidInput);
        self.next_commit = commit + 1;

        // 保留中のリクエストを処理
        let mut proposal = None;
        while let Some(next) = self.proposals.pop_front() {
            if next.id().index == commit {
                if next.id().term == entry.term() {
                    proposal = Some(next);
                    break;
                } else {
                    warn!(self.logger, "This proposal is rejected: {:?}", next.id());
                    next.notify_rejected();
                }
            } else if commit < next.id().index {
                self.proposals.push_front(next);
                break;
            } else {
                // 不整合: コミットされたログインデックスが連続していない
                track_panic!(ErrorKind::Other, "Inconsistent state");
            }
        }

        // エントリ毎の処理を実施
        match entry {
            LogEntry::Noop { .. } => {
                let leader = track!(NodeId::from_raft_node_id(
                    &self.rlog.local_node().ballot.voted_for
                ))?;
                for x in self.leader_waitings.drain(..) {
                    x.exit(Ok(leader));
                }
                info!(
                    self.logger,
                    "New leader is elected: {:?} (commit:{:?})", leader, commit
                );
                self.leader = Some(leader);
            }
            LogEntry::Command { command, .. } => {
                self.commit_timeout = None;
                let command = track!(protobuf::command_decoder().decode_from_bytes(&command))?;
                let result = track!(self.handle_command(commit, command));
                if let Some(proposal) = proposal {
                    match result {
                        Err(e) => proposal.notify_error(e),
                        Ok(old) => proposal.notify_committed(&old),
                    }
                }
            }
            LogEntry::Config { config, .. } => self.handle_config(commit, &config),
        }

        // スナップショットの処理
        self.last_commit = Some(commit);
        self.local_log_size += 1;
        if self.local_log_size > self.snapshot_threshold {
            track!(self.take_snapshot())?;
        }
        Ok(())
    }
    fn handle_command(&mut self, commit: LogIndex, command: Command) -> Result<Vec<ObjectVersion>> {
        match command {
            Command::Put {
                object_id,
                userdata: data,
                put_content_timeout,
                expect,
            } => {
                let version = ObjectVersion(commit.as_u64());
                let metadata = Metadata { version, data };
                let old = track!(self.machine.put(object_id, metadata, &expect))?;
                if let Some(old) = old {
                    track_assert!(
                        old < version,
                        ErrorKind::InvalidInput,
                        "old={:?}, new={:?}",
                        old,
                        version
                    );
                    self.events.push_back(Event::Deleted { version: old });
                }
                self.events.push_back(Event::Putted {
                    version,
                    put_content_timeout,
                });
                self.metrics.objects.set(self.machine.len() as f64);

                Ok(old.into_iter().collect())
            }
            Command::Delete { object_id, expect } => {
                let old = track!(self.machine.delete(&object_id, &expect))?;
                if let Some(version) = old {
                    self.events.push_back(Event::Deleted { version });
                }
                self.metrics.objects.set(self.machine.len() as f64);
                Ok(old.into_iter().collect())
            }
            Command::DeleteByVersion { object_version } => {
                let old = track!(self.machine.delete_version(object_version))?;
                if let Some(version) = old {
                    self.events.push_back(Event::Deleted { version });
                }
                self.metrics.objects.set(self.machine.len() as f64);
                Ok(old.into_iter().collect())
            }
            // 現時点ではDeleteByRangeに到達することはない。
            // その理由は、Command::DeleteByRangeを発行するべき
            // handle_request中のRequest::DeleteByRangeのcaseに、
            // 現在の実装で到達することがないから。
            Command::DeleteByRange {
                version_from,
                version_to,
            } => {
                unreachable!(
                    "Command::DeleteByRange from = {:?}, to = {:?}",
                    version_from, version_to
                );
            }
            Command::DeleteByPrefix { prefix } => {
                let deleted = track!(self.machine.delete_by_prefix(&prefix))?;

                deleted
                    .iter()
                    .for_each(|&version| self.events.push_back(Event::Deleted { version }));

                self.metrics.objects.set(self.machine.len() as f64);

                Ok(deleted)
            }
        }
    }
    fn handle_config(&mut self, commit: LogIndex, config: &ClusterConfig) {
        info!(
            self.logger,
            "New cluster configuration at {:?}: {:?}", commit, config
        );
    }
    fn start_reelection(&mut self) {
        let members = self.rlog.cluster_config().primary_members();
        let local = self.rlog.local_node();
        for m in members.iter().filter(|n| **n != local.id) {
            let m = track!(NodeId::from_raft_node_id(&m)); // TODO:
            if let Ok(m) = m {
                let client = ::libfrugalos::client::mds::Client::new(
                    (m.addr, m.local_id.to_string()),
                    self.rpc_service.clone(),
                );
                client.recommend_to_leader();
            }
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Err(e) = track!(self.service.remove_node(self.node_id)) {
            warn!(
                self.logger,
                "Cannot remove the node {:?}: {}", self.node_id, e
            );
        }
    }
}
impl Stream for Node {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while self.polling_timer.poll().expect("Broken timer").is_ready() {
            // 高負荷時(?)におそらくraftlogのバグでポーリング用のイベントが
            // 適切に発行されていないことがあるようなので、確実に定期的にポーリング
            // されるようにタイマーを設定する.
            //
            // TODO: バグが修正されたら、このコードは消す
            // => 定期実行系は便利ではあるので、残しておいても良いかも
            self.polling_timer = timer::timeout(Duration::from_millis(500));

            // キュー長チェック
            let proposal_queue_len = self.rlog.proposal_queue_len();
            self.metrics
                .proposal_queue_len
                .set(proposal_queue_len as f64);
            if proposal_queue_len > self.large_queue_threshold {
                self.large_queue_rounds += 1;
                if self.large_queue_rounds >= self.reelection_threshold {
                    warn!(self.logger, "The leader may be slow. Reelection is started");
                    self.start_reelection();
                    self.large_queue_rounds = 0;
                }
            } else {
                self.large_queue_rounds = 0;
            }

            // コミットタイムアウトチェック
            if let Some(n) = self.commit_timeout {
                if n == 0 {
                    warn!(self.logger, "Commit timeout. Reelection is started");
                    self.commit_timeout = None;
                    self.start_reelection();
                } else {
                    self.commit_timeout = Some(n - 1);
                }
            }

            // リーダ待機チェック
            self.leader_waiting_timeout = self.leader_waiting_timeout.saturating_sub(1);
            if self.leader_waiting_timeout == 0 && !self.leader_waitings.is_empty() {
                warn!(self.logger, "Leader waiting timeout (cleared)");
                self.leader_waitings.clear();
            }
        }

        match track!(self.decoding_snapshot.poll().map_err(Error::from))? {
            Async::NotReady => return Ok(Async::NotReady),
            Async::Ready(None) => {}
            Async::Ready(Some(result)) => {
                let (new_head, machine, versions) = track!(result)?;
                info!(self.logger, "Snapshot decoded: new_head={:?}", new_head);
                let delay = env::var("FRUGALOS_SNAPSHOT_REPAIR_DELAY")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10);
                self.events.reserve_exact(machine.len());
                self.events
                    .extend(versions.into_iter().map(|version| Event::Putted {
                        version,
                        put_content_timeout: Seconds(delay),
                    }));
                self.next_commit = new_head.index;
                self.machine = machine;
                self.metrics.objects.set(self.machine.len() as f64);
                self.decoding_snapshot = None;
            }
        }
        if let Async::Ready(Some(result)) = track!(self.ready_snapshot.poll().map_err(Error::from))?
        {
            info!(self.logger, "Snapshot readied");
            let (commit, snapshot) = track!(result)?;
            track!(self.rlog.install_snapshot(commit, snapshot).or_else(|e| {
                if *e.kind() == ::raftlog::ErrorKind::Busy {
                    info!(self.logger, "Busy");
                    Ok(())
                } else {
                    Err(e)
                }
            }))?;
            self.ready_snapshot = None;
        }

        while let Async::Ready(polled) = self.request_rx.poll().expect("Never fails") {
            let request = polled.expect("Never fails");
            self.handle_request(request);
        }

        while let Async::Ready(polled) = track!(self.rlog.poll())? {
            if let Some(event) = polled {
                if let raftlog::Event::SnapshotLoaded { .. } = event {
                    track!(self.handle_raft_event(event))?;

                    // We need the following `break` since
                    // if we meet a SnapshotLoaded event
                    // then we need to decode the loaded snapshot before handle events that follow the SnapshotLoaded event.
                    //
                    // Indeed, by this break, we skip the subsequent events and decode the loaded snapshot
                    // at the above `match track!(self.decoding_snapshot.poll().map_err(Error::from))? { ... }`-part.
                    break;
                } else {
                    track!(self.handle_raft_event(event))?;
                }
            } else {
                track_panic!(
                    ErrorKind::Other,
                    "Unexpected termination of the Raft event stream"
                );
            }
        }

        // FIXME: もっと適切な場所に移動
        if self.phase == Phase::Stopped {
            info!(self.logger, "Stopped");
            return Ok(Async::Ready(None));
        }

        if let Some(event) = self.events.pop_front() {
            if self.events.capacity() > 32 && self.events.len() < self.events.capacity() / 2 {
                self.events.shrink_to_fit();
            }
            return Ok(Async::Ready(Some(event)));
        }
        Ok(Async::NotReady)
    }
}
