//! クラスタ構成操作系の補助関数群。
use bytecodec::{DecodeExt, EncodeExt};
use cannyls;
use cannyls::deadline::Deadline;
use cannyls::device::Device;
use fibers::{Executor, Spawn, ThreadPoolExecutor};
use fibers_rpc::client::{
    ClientServiceBuilder as RpcServiceBuilder, ClientServiceHandle as RpcServiceHandle,
};
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_raft::{self, RaftIo};
use futures::{Async, Future, Poll, Stream};
use libfrugalos::client::config::Client;
use libfrugalos::entity::server::Server;
use prometrics::metrics::MetricBuilder;
use raftlog::ReplicatedLog;
use slog::Logger;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use config::server_to_frugalos_raft_node;
use machine::Snapshot;
use protobuf;
use {Error, ErrorKind, Result};

const LOCAL_DATA_FILE_NAME: &str = "local.dat";
const CLUSTER_DATA_FILE_NAME: &str = "cluster.lusf";

/// ローカルサーバの情報を取得する。
pub fn load_local_server_info<P: AsRef<Path>>(data_dir: P) -> Result<Server> {
    use std::io::Read;
    let mut f =
        track!(fs::File::open(data_dir.as_ref().join(LOCAL_DATA_FILE_NAME)).map_err(Error::from))?;
    let mut buf = Vec::new();
    track!(f.read_to_end(&mut buf).map_err(Error::from))?;
    let server = track!(protobuf::server_decoder().decode_from_bytes(&buf))?;
    Ok(server)
}

/// ローカルサーバの情報を削除する。
pub fn delete_local_server_info<P: AsRef<Path>>(data_dir: P) -> Result<()> {
    track!(fs::remove_dir_all(&data_dir).map_err(Error::from))?;
    Ok(())
}

/// ローカルサーバの情報を保存する。
pub fn save_local_server_info<P: AsRef<Path>>(data_dir: P, server: Server) -> Result<()> {
    use std::io::Write;
    let bytes = track!(protobuf::server_encoder().encode_into_bytes(server))?;
    let mut f = track!(
        fs::File::create(data_dir.as_ref().join(LOCAL_DATA_FILE_NAME)).map_err(Error::from)
    )?;
    track!(f.write_all(&bytes).map_err(Error::from))?;
    Ok(())
}

/// Raftの複製ログを生成する。
pub fn make_rlog<P: AsRef<Path>, S: Spawn + Clone + Send + 'static>(
    logger: Logger,
    data_dir: P,
    local: &Server,
    rpc_service: RpcServiceHandle,
    spawner: S,
    raft_service: frugalos_raft::ServiceHandle,
    initial_members: Vec<Server>,
) -> Result<(Device, ReplicatedLog<RaftIo>)> {
    let node = server_to_frugalos_raft_node(local);

    let (nvm, created) = track!(cannyls::nvm::FileNvm::create_if_absent(
        data_dir.as_ref().join(CLUSTER_DATA_FILE_NAME),
        100 * 1024 * 1024 // FIXME: パラメータ化
    ))?;

    let mut storage_builder = cannyls::storage::StorageBuilder::new();
    let metrics = MetricBuilder::new().label("device", "system").clone();
    storage_builder.metrics(metrics.clone());
    let storage = if created {
        track!(storage_builder.create(nvm))?
    } else {
        track!(storage_builder.open(nvm))?
    };
    let device = cannyls::device::DeviceBuilder::new()
        .metrics(metrics.clone())
        .spawn(|| Ok(storage));

    // FIXME: パラメータ化
    let timer = frugalos_raft::Timer::new(Duration::from_secs(10), Duration::from_secs(35));
    let storage = frugalos_raft::Storage::new(
        logger,
        node.local_id,
        device.handle(),
        frugalos_raft::StorageMetrics::new(),
    );
    let mailer = frugalos_raft::Mailer::new(spawner, rpc_service, None);

    let io = track!(RaftIo::new(raft_service, storage, mailer, timer))?;

    let members = initial_members
        .into_iter()
        .map(|s| server_to_frugalos_raft_node(&s).to_raft_node_id())
        .collect();
    let rlog = ReplicatedLog::new(node.to_raft_node_id(), members, io);
    Ok((device, rlog))
}

fn assert_to_be_newbie<P: AsRef<Path>>(logger: &Logger, data_dir: P) -> Result<()> {
    track_assert!(
        !data_dir.as_ref().join(LOCAL_DATA_FILE_NAME).exists(),
        ErrorKind::InvalidInput
    );
    track_assert!(
        !data_dir.as_ref().join(CLUSTER_DATA_FILE_NAME).exists(),
        ErrorKind::InvalidInput
    );
    if !data_dir.as_ref().exists() {
        info!(logger, "Creates data directry: {:?}", data_dir.as_ref());
        track!(fs::create_dir_all(data_dir.as_ref()).map_err(Error::from))?;
    }
    Ok(())
}

/// 自分だけを含むRaftクラスタを生成する
pub fn create<P: AsRef<Path>>(logger: &Logger, mut local: Server, data_dir: P) -> Result<()> {
    info!(
        logger,
        "[START] create: {}",
        dump!(local, data_dir.as_ref())
    );

    // 既にクラスタに参加済みではないかをチェック
    track!(assert_to_be_newbie(&logger, &data_dir))?;

    // 自分だけを含むRaftクラスタを作成
    local.seqno = 0;
    let node = server_to_frugalos_raft_node(&local);

    let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;

    let rpc_service = RpcServiceBuilder::new()
        .logger(logger.clone())
        .finish(executor.handle());

    let mut rpc_server_builder = RpcServerBuilder::new(node.addr);
    let raft_service = frugalos_raft::Service::new(logger.clone(), &mut rpc_server_builder);
    let rpc_server = rpc_server_builder.finish(executor.handle());

    let (device, rlog) = track!(make_rlog(
        logger.clone(),
        &data_dir,
        &local,
        rpc_service.handle(),
        executor.handle(),
        raft_service.handle(),
        vec![local.clone()],
    ))?;
    executor.spawn(rpc_server.map_err(move |e| panic!("Error: {}", e)));
    executor.spawn(raft_service.map_err(move |e| panic!("Error: {}", e)));
    executor.spawn(rpc_service.map_err(move |e| panic!("Error: {}", e)));

    // クラスタ構成に自サーバを登録
    let monitor = executor.spawn_monitor(CreateCluster::new(logger.clone(), rlog, local.clone()));
    let result = track!(executor.run_fiber(monitor).map_err(Error::from))?;
    track!(result.map_err(Error::from))?;

    // ディスクの同期を待機 (不要かも)
    device.stop(Deadline::Immediate);
    let result = track!(executor.run_future(device).map_err(Error::from))?;
    track!(result.map_err(Error::from))?;

    // ローカルにも情報を保存
    track!(save_local_server_info(data_dir, local))?;

    info!(logger, "[FINISH] create");
    Ok(())
}

/// 既存のRaftクラスタに参加する
pub fn join<P: AsRef<Path>>(
    logger: &Logger,
    local: &Server,
    data_dir: P,
    contact_server: SocketAddr,
) -> Result<()> {
    info!(
        logger,
        "[START] join: {}",
        dump!(local, data_dir.as_ref(), contact_server)
    );

    // 既にクラスタに参加済みではないかをチェック
    track!(assert_to_be_newbie(&logger, &data_dir))?;

    // 既存クラスタへの追加を依頼する
    let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
    let rpc_service = RpcServiceBuilder::new()
        .logger(logger.clone())
        .finish(executor.handle());
    let client = Client::new(contact_server, rpc_service.handle());
    executor.spawn(rpc_service.map_err(|e| panic!("{}", e)));

    let monitor = executor.spawn_monitor(client.put_server(local.clone()));
    let result = track!(executor.run_fiber(monitor).map_err(Error::from))?;
    let joined = track!(result.map_err(Error::from))?;
    info!(
        logger,
        "This server is joined to the cluster: {}",
        dump!(joined)
    );

    // ローカルにも情報を保存
    track!(save_local_server_info(data_dir, joined))?;

    info!(logger, "[FINISH] join");
    Ok(())
}

/// Raftクラスタから抜ける
pub fn leave<P: AsRef<Path>>(
    logger: &Logger,
    data_dir: P,
    contact_server: SocketAddr,
) -> Result<()> {
    info!(
        logger,
        "[START] leave: {}",
        dump!(data_dir.as_ref(), contact_server)
    );

    let local = track!(load_local_server_info(&data_dir))?;
    info!(logger, "Local server info: {:?}", local);

    // クラスタからの離脱を依頼する
    let mut executor = track!(ThreadPoolExecutor::new().map_err(Error::from))?;
    let rpc_service = RpcServiceBuilder::new()
        .logger(logger.clone())
        .finish(executor.handle());
    let client = Client::new(contact_server, rpc_service.handle());
    executor.spawn(rpc_service.map_err(|e| panic!("{}", e)));

    let monitor = executor.spawn_monitor(client.delete_server(local.id.clone()));
    let result = track!(executor.run_fiber(monitor).map_err(Error::from))?;
    let left = track!(result.map_err(Error::from))?;
    info!(
        logger,
        "This server is left from the cluster: {}",
        dump!(left)
    );

    // ローカルの情報を削除
    track!(delete_local_server_info(data_dir))?;

    info!(logger, "[FINISH] leave");
    Ok(())
}

struct CreateCluster {
    logger: Logger,
    rlog: ReplicatedLog<RaftIo>,
    local: Server,
}
impl CreateCluster {
    pub fn new(logger: Logger, rlog: ReplicatedLog<RaftIo>, local: Server) -> Self {
        CreateCluster {
            logger,
            rlog,
            local,
        }
    }
}
impl Future for CreateCluster {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        use raftlog::log::LogEntry;
        use raftlog::Event;
        while let Async::Ready(event) = track!(self.rlog.poll())? {
            let event = event.expect("Never fails");
            debug!(self.logger, "Raft Event: {:?}", event);
            match event {
                Event::Committed {
                    entry: LogEntry::Noop { .. },
                    index,
                } => {
                    let snapshot = Snapshot::initial(self.local.clone());
                    let snapshot =
                        track!(protobuf::snapshot_encoder().encode_into_bytes(snapshot))?;
                    track!(self.rlog.install_snapshot(index + 1, snapshot))?;
                }
                Event::SnapshotInstalled { .. } => {
                    return Ok(Async::Ready(()));
                }
                _ => {}
            }
        }
        Ok(Async::NotReady)
    }
}
