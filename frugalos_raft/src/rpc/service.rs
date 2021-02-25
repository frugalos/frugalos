use atomic_immut::AtomicImmut;
use core::task::{Context, Poll as Poll03};
use fibers_rpc::server::ServerBuilder;
use futures03::Future as Future03;
use raftlog::{ErrorKind, Result};
use slog::Logger;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::mail::{Mailbox, Mailer};
use super::server::RpcServer;
use crate::rpc;
use crate::LocalNodeId;

type Nodes = Arc<AtomicImmut<HashMap<LocalNodeId, Mailbox>>>;

/// Raft用のサービス.
///
/// RPCメッセージ送受信のためのノードを管理を主な責務としている.
#[derive(Debug)]
pub struct Service {
    logger: Logger,
    nodes: Nodes,
    command_tx: mpsc::UnboundedSender<Command>,
    command_rx: mpsc::UnboundedReceiver<Command>,
}
impl Service {
    /// 新しい`Service`インスタンスを生成する.
    pub fn new(logger: Logger, builder: &mut ServerBuilder) -> Self {
        let nodes = Arc::new(AtomicImmut::new(HashMap::new()));
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let this = Service {
            logger,
            nodes,
            command_tx,
            command_rx,
        };
        builder.add_cast_handler::<rpc::RequestVoteCallRpc, _>(RpcServer::new(this.handle()));
        builder.add_cast_handler::<rpc::RequestVoteReplyRpc, _>(RpcServer::new(this.handle()));
        builder.add_cast_handler::<rpc::AppendEntriesCallRpc, _>(RpcServer::new(this.handle()));
        builder.add_cast_handler::<rpc::AppendEntriesReplyRpc, _>(RpcServer::new(this.handle()));
        builder.add_cast_handler::<rpc::InstallSnapshotCastRpc, _>(RpcServer::new(this.handle()));
        this
    }

    /// サービスを操作するためのハンドルを返す.
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            nodes: self.nodes.clone(),
            command_tx: self.command_tx.clone(),
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::AddNode(id, mailbox) => {
                info!(self.logger, "Adds node: {}", dump!(id, mailbox));

                let mut nodes = (&*self.nodes.load()).clone();
                nodes.insert(id, mailbox);
                self.nodes.store(nodes);
            }
            Command::RemoveNode(id) => {
                let mut nodes = (&*self.nodes.load()).clone();
                let removed = nodes.remove(&id);
                self.nodes.store(nodes);

                info!(self.logger, "Removes node: {}", dump!(id, removed));
            }
        }
    }
}
impl Future03 for Service {
    /// NOTE: この`Future`が正常終了することはない.
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll03<Self::Output> {
        loop {
            let polled = self.command_rx.poll_recv(cx);
            if let Poll03::Ready(command) = polled {
                let command = command.expect("Unreachable");
                self.as_mut().handle_command(command);
            } else {
                return Poll03::Pending;
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    AddNode(LocalNodeId, Mailbox),
    RemoveNode(LocalNodeId),
}

/// `Service`を操作するためのハンドル.
///
/// `Service`をクレートの利用者が直接操作することはないため、
/// 外部に公開されているメソッドは存在しない.
#[derive(Debug, Clone)]
pub struct ServiceHandle {
    nodes: Nodes,
    command_tx: mpsc::UnboundedSender<Command>,
}
impl ServiceHandle {
    pub(crate) fn add_node(&self, id: LocalNodeId, mailer: &Mailer) -> Result<()> {
        let mailbox = mailer.mailbox();
        let command = Command::AddNode(id, mailbox);
        if self.command_tx.send(command).is_err() {
            track_panic!(ErrorKind::Other, "Service down: {}", dump!(id));
        }
        Ok(())
    }
    pub(crate) fn remove_node(&self, id: LocalNodeId) -> Result<()> {
        let command = Command::RemoveNode(id);
        if self.command_tx.send(command).is_err() {
            track_panic!(ErrorKind::Other, "Service down: {}", dump!(id));
        }
        Ok(())
    }
    pub(crate) fn get_node(&self, id: LocalNodeId) -> Option<Mailbox> {
        self.nodes.load().get(&id).cloned()
    }
}
