use fibers_rpc::client::{ClientServiceHandle, Options};
use fibers_rpc::Cast;
use raftlog::message::Message;
use std::net::SocketAddr;

use rpc;

#[derive(Debug, Clone)]
pub struct RpcClient<'a> {
    server: SocketAddr,
    rpc_service: &'a ClientServiceHandle,
}
impl<'a> RpcClient<'a> {
    pub fn new(server: SocketAddr, rpc_service: &'a ClientServiceHandle) -> Self {
        RpcClient {
            server,
            rpc_service,
        }
    }
    pub fn send_rpc_message(&self, message: Message) -> bool {
        let force_wakeup = match message {
            Message::RequestVoteReply(_) | Message::AppendEntriesReply(_) => true,
            _ => false,
        };

        let (max_queue_len, priority) = match message {
            Message::RequestVoteCall(_) | Message::RequestVoteReply(_) => (None, 32),
            Message::AppendEntriesCall(ref x) => {
                if x.suffix.entries.len() < 2 {
                    (Some(4096), 64)
                } else {
                    (Some(512), 128)
                }
            }
            Message::AppendEntriesReply(_) => (Some(512), 128),
            Message::InstallSnapshotCast(_) => (Some(2048), 200),
        };

        let mut options = Options::default();
        options.force_wakeup = force_wakeup;
        options.max_queue_len = max_queue_len;
        options.priority = priority;

        match message {
            Message::RequestVoteCall(m) => {
                let mut client = rpc::RequestVoteCallRpc::client(&self.rpc_service);
                *client.options_mut() = options;
                client.cast(self.server, m).is_ok()
            }
            Message::RequestVoteReply(m) => {
                let mut client = rpc::RequestVoteReplyRpc::client(&self.rpc_service);
                *client.options_mut() = options;
                client.cast(self.server, m).is_ok()
            }
            Message::AppendEntriesCall(m) => {
                let mut client = rpc::AppendEntriesCallRpc::client(&self.rpc_service);
                *client.options_mut() = options;
                client.cast(self.server, m).is_ok()
            }
            Message::AppendEntriesReply(m) => {
                let mut client = rpc::AppendEntriesReplyRpc::client(&self.rpc_service);
                *client.options_mut() = options;
                client.cast(self.server, m).is_ok()
            }
            Message::InstallSnapshotCast(m) => {
                let mut client = rpc::InstallSnapshotCastRpc::client(&self.rpc_service);
                *client.options_mut() = options;
                client.cast(self.server, m).is_ok()
            }
        }
    }
}
