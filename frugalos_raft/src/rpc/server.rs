use fibers_rpc::server::{HandleCast, NoReply};
use prometrics::metrics::{Counter, MetricBuilder};
use raftlog::message::{
    AppendEntriesCall, AppendEntriesReply, InstallSnapshotCast, Message, RequestVoteCall,
    RequestVoteReply,
};

use super::service::ServiceHandle;
use rpc;
use NodeId;

#[derive(Debug, Clone)]
pub struct RpcServer {
    service: ServiceHandle,
    metrics: Metrics,
}
impl RpcServer {
    pub fn new(service: ServiceHandle) -> Self {
        RpcServer {
            service,
            metrics: Metrics::new(),
        }
    }

    fn handle_message(&self, message: Message) -> NoReply {
        let node: NodeId = match message.header().destination.as_str().parse() {
            Err(_) => {
                self.metrics.unknown_node_messages.increment();
                return NoReply::done();
            }
            Ok(n) => n,
        };
        if let Some(node) = self.service.get_node(node.local_id) {
            if node.deliver(message).is_err() {
                self.metrics.downed_node_messages.increment();
            }
        } else {
            self.metrics.unknown_node_messages.increment();
        }
        NoReply::done()
    }
}
impl HandleCast<rpc::RequestVoteCallRpc> for RpcServer {
    fn handle_cast(&self, m: RequestVoteCall) -> NoReply {
        self.handle_message(m.into())
    }
}
impl HandleCast<rpc::RequestVoteReplyRpc> for RpcServer {
    fn handle_cast(&self, m: RequestVoteReply) -> NoReply {
        self.handle_message(m.into())
    }
}
impl HandleCast<rpc::AppendEntriesCallRpc> for RpcServer {
    fn handle_cast(&self, m: AppendEntriesCall) -> NoReply {
        self.handle_message(m.into())
    }
}
impl HandleCast<rpc::AppendEntriesReplyRpc> for RpcServer {
    fn handle_cast(&self, m: AppendEntriesReply) -> NoReply {
        self.handle_message(m.into())
    }
}
impl HandleCast<rpc::InstallSnapshotCastRpc> for RpcServer {
    fn handle_cast(&self, m: InstallSnapshotCast) -> NoReply {
        self.handle_message(m.into())
    }
}

/// Prometheus metrics.
#[derive(Debug, Clone)]
pub struct Metrics {
    unknown_node_messages: Counter,
    downed_node_messages: Counter,
}
impl Metrics {
    /// Makes a new `Metrics` instance.
    pub fn new() -> Self {
        let mut builder = MetricBuilder::new();
        builder.namespace("frugalos_raft").subsystem("rpc_server");
        Metrics {
            unknown_node_messages: builder
                .counter("unknown_node_messages_total")
                .finish()
                .unwrap(),
            downed_node_messages: builder
                .counter("downed_node_messages_total")
                .finish()
                .unwrap(),
        }
    }
}
