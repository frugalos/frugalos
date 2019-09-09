use atomic_immut::AtomicImmut;
use fibers::sync::{mpsc, oneshot};
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_core::tracer::ThreadLocalTracer;
use frugalos_raft::{LocalNodeId, NodeId};
use futures::{Async, Future, Poll, Stream};
use slog::Logger;
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::sync::Arc;

use node::NodeHandle;
use server::Server;
use {Error, Result};

type Nodes = Arc<AtomicImmut<HashMap<LocalNodeId, NodeHandle>>>;

/// MDS用のサービスを表す`Future`実装.
///
/// MDSノードの管理やRPC要求の処理等を担当する.
///
/// 一つのサーバ(HTTPサーバ)につき、一つのサービスインスタンスが起動していることを想定.
#[derive(Debug)]
pub struct Service {
    logger: Logger,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    state: ServiceState,
}
impl Service {
    /// 新しい`Service`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        rpc: &mut RpcServerBuilder,
        tracer: ThreadLocalTracer,
    ) -> Result<Self> {
        let nodes = Arc::new(AtomicImmut::new(HashMap::new()));
        let (command_tx, command_rx) = mpsc::channel();
        let this = Service {
            logger: logger.clone(),
            command_tx,
            command_rx,
            state: ServiceState::Running { logger, nodes },
        };
        Server::register(this.handle(), rpc, tracer);
        Ok(this)
    }

    /// `Service`を操作するためのハンドルを返す.
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            nodes: self.state.nodes(),
            command_tx: self.command_tx.clone(),
        }
    }

    /// サービスを停止する.
    ///
    /// サービス停止前には、全てのローカルノードでスナップショットが取得される.
    ///
    /// # サービスの停止時に考慮すべきこと
    ///
    /// (a) すべてのノードが停止するまでに発生するノードの取得エラー(存在しない
    /// ノードへの参照)を減らすこと.
    ///
    /// (b) 停止時のノードのスナップショット取得以降に `LogSuffix` を伸ばしすぎ
    /// ないこと. 次回起動に時間がかかるようになってしまうため.
    ///
    /// # ノードを順次停止することの問題点
    ///
    /// 旧実装のようにスナップショットを取得し終えたノードから順次停止して
    /// いくと存在しないノードに対するリクエストが発生しやすくなる.なぜなら、
    /// MDS に RPC 呼び出しをするクライアント側は RPC サーバが停止するまでは
    /// 停止済みのノードに対するリクエストを送り続けてくるからである.
    ///
    /// 特にここで問題となるのは、ノードがすべて停止するまでの間に発生する、
    /// 停止済みのノードの取得失敗によるエラーであり、この状況ではクライアント
    /// 側のリトライで状況が改善しないため実質的にリトライが意味をなさない.
    ///
    /// # 停止時のエラーを極力抑える新実装
    ///
    /// 上述の問題を避けるためにサービスの停止処理を以下の2段階に分ける.
    ///
    /// 1. スナップショットの取得
    /// 2. 1 がすべてのノードで完了するまで待ち合わせてからノードを停止
    ///
    /// 1 で `Node` の状態が `Stopping` に変更され、スナップショットの取得
    /// もされる.スナップショットの取得が完了した際にそれを `Service` に
    /// `Monitored` 経由で通知する.
    ///
    /// すべてのノードがスナップショットを取得したら(あるいは、スキップ)、
    /// `Request::Exit` を `Node` に送り `Node` の状態を `Stopped` に変更
    /// する.
    ///
    /// この実装と Leader ノード以外もリクエストに応答できるようにする変更
    /// を組み合わせることで停止時のエラーを減らすことが可能になっている.
    ///
    /// # 新実装のデメリット
    ///
    /// (b) について、スナップショットの取得に時間がかかる環境では `LogSuffix`
    /// が伸びて、スナップショット取得の効果が薄れてしまうことは許容する.
    pub fn stop(&mut self) {
        self.state = match mem::replace(&mut self.state, ServiceState::Stopped(self.logger.clone()))
        {
            ServiceState::Running { nodes, .. } => {
                let mut futures = Vec::new();
                for (id, node) in nodes.load().iter() {
                    let logger = self.logger.clone();
                    info!(logger, "Sends stop request: {:?}", id);
                    let (monitored, monitor) = oneshot::monitor();
                    futures.push(monitor.then(move |result| {
                        if let Err(e) = result {
                            warn!(logger, "{}", e);
                        }
                        futures::future::ok(())
                    }));
                    node.stop(monitored);
                }
                ServiceState::Stopping {
                    logger: self.logger.clone(),
                    nodes: nodes.clone(),
                    future: Box::new(futures::future::join_all(futures)),
                }
            }
            next => next,
        };
    }

    /// スナップショットを取得する.
    pub fn take_snapshot(&mut self) {
        for (id, node) in self.state.nodes().load().iter() {
            info!(self.logger, "Sends taking snapshot request: {:?}", id);
            node.take_snapshot();
        }
    }

    fn exit(&mut self) {
        for (id, node) in self.state.nodes().load().iter() {
            info!(self.logger, "Sends exit request: {:?}", id);
            node.exit();
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::AddNode(id, node) => {
                self.state.add_node(id, node);
            }
            Command::RemoveNode(id) => {
                self.state.remove_node(id);
            }
        }
    }
}
impl Future for Service {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // NOTE: `Err` は返ってこないので考慮しなくてよい
        if let Async::Ready(true) = self.state.poll().expect("Never fails") {
            self.exit();
            self.state = ServiceState::Stopped(self.logger.clone());
        }
        loop {
            let polled = self.command_rx.poll().expect("Never fails");
            if let Async::Ready(command) = polled {
                let command = command.expect("Unreachable");
                self.handle_command(command);
                if self.state.is_stopped() {
                    return Ok(Async::Ready(()));
                }
            } else {
                return Ok(Async::NotReady);
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    AddNode(LocalNodeId, NodeHandle),
    RemoveNode(LocalNodeId),
}

/// `Service`を操作するためのハンドル.
///
/// `Service`に対する操作はクレート内で閉じているため、
/// 利用者に公開されているメソッドは存在しない.
#[derive(Debug, Clone)]
pub struct ServiceHandle {
    nodes: Nodes,
    command_tx: mpsc::Sender<Command>,
}
impl ServiceHandle {
    pub(crate) fn add_node(&self, id: NodeId, node: NodeHandle) -> Result<()> {
        let command = Command::AddNode(id.local_id, node);
        track!(
            self.command_tx.send(command).map_err(Error::from),
            "id={:?}",
            id
        )?;
        Ok(())
    }
    pub(crate) fn remove_node(&self, id: NodeId) -> Result<()> {
        let command = Command::RemoveNode(id.local_id);
        track!(
            self.command_tx.send(command).map_err(Error::from),
            "id={:?}",
            id
        )?;
        Ok(())
    }
    pub(crate) fn get_node(&self, local_id: LocalNodeId) -> Option<NodeHandle> {
        self.nodes().get(&local_id).cloned()
    }
    pub(crate) fn nodes(&self) -> Arc<HashMap<LocalNodeId, NodeHandle>> {
        self.nodes.load()
    }
}

// ノード群の管理は `ServiceState` の責務.
enum ServiceState {
    Running {
        logger: Logger,
        nodes: Nodes,
    },
    Stopping {
        logger: Logger,
        nodes: Nodes,
        future: Box<dyn Future<Item = Vec<()>, Error = ()> + Send + 'static>,
    },
    Stopped(Logger),
}
impl ServiceState {
    fn nodes(&self) -> Nodes {
        match self {
            ServiceState::Running { nodes, .. } => nodes.clone(),
            ServiceState::Stopping { nodes, .. } => nodes.clone(),
            ServiceState::Stopped(_) => Arc::new(AtomicImmut::new(HashMap::new())),
        }
    }
    fn logger(&self) -> &Logger {
        match self {
            ServiceState::Running { ref logger, .. } => logger,
            ServiceState::Stopping { ref logger, .. } => logger,
            ServiceState::Stopped(ref logger) => logger,
        }
    }
    fn add_node(&mut self, id: LocalNodeId, node: NodeHandle) {
        if !self.is_running() {
            warn!(self.logger(), "Ignored: id={:?}, node={:?}", id, node);
            return;
        }
        info!(self.logger(), "Adds node: id={:?}, node={:?}", id, node);

        let mut nodes = (&*self.nodes().load()).clone();
        nodes.insert(id, node);
        self.nodes().store(nodes);
    }
    fn remove_node(&mut self, id: LocalNodeId) {
        let mut nodes = (&*self.nodes().load()).clone();
        let removed = nodes.remove(&id);
        let len = nodes.len();
        self.nodes().store(nodes);

        info!(
            self.logger(),
            "Removes node: id={:?}, node={:?} (len={})", id, removed, len
        );
    }
    fn is_running(&self) -> bool {
        if let ServiceState::Running { .. } = self {
            return true;
        }
        false
    }
    fn is_stopped(&self) -> bool {
        if let ServiceState::Stopped(_) = self {
            return true;
        }
        false
    }
}
impl Future for ServiceState {
    type Item = bool;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            ServiceState::Running { .. } => Ok(Async::NotReady),
            ServiceState::Stopping { ref mut future, .. } => future.poll().map(|f| f.map(|_| true)),
            ServiceState::Stopped(_) => Ok(Async::Ready(false)),
        }
    }
}
impl fmt::Debug for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self {
            ServiceState::Running { .. } => "Running",
            ServiceState::Stopping { .. } => "Stopping",
            ServiceState::Stopped(_) => "Stopped",
        };
        write!(f, "{}", state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibers::sync::mpsc;
    use rustracing::sampler::NullSampler;
    use slog::Discard;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::str::FromStr;
    use trackable::result::TestResult;

    use node::Request;

    struct TestNodeForStop {
        node_id: NodeId,
        tx: mpsc::Sender<Request>,
        rx: mpsc::Receiver<Request>,
    }
    impl TestNodeForStop {
        fn new(node_id: &str) -> Self {
            let node_id = NodeId::from_str(node_id).unwrap();
            let (tx, rx) = mpsc::channel();
            Self { node_id, tx, rx }
        }
        fn handle(&self) -> NodeHandle {
            NodeHandle::new(self.tx.clone())
        }
    }
    impl Future for TestNodeForStop {
        type Item = ();
        type Error = Error;
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            while let Async::Ready(Some(request)) = self.rx.poll().unwrap() {
                if let Request::Stop(monitored) = request {
                    monitored.exit(Ok(()));
                }
            }
            Ok(Async::NotReady)
        }
    }

    #[test]
    fn stop_works() -> TestResult {
        let mut node = TestNodeForStop::new("1000a00.0@127.0.0.1:14278");
        let (tracer, _) = rustracing_jaeger::Tracer::new(NullSampler);
        let tracer = ThreadLocalTracer::new(tracer);
        let logger = Logger::root(Discard, o!());
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let mut rpc_server_builder = RpcServerBuilder::new(addr);
        let mut service = track!(Service::new(logger, &mut rpc_server_builder, tracer))?;
        track!(service.handle().add_node(node.node_id, node.handle()))?;
        service.stop();
        while track!(service.poll())?.is_not_ready() {
            track!(node.poll())?;
        }
        Ok(())
    }
}
