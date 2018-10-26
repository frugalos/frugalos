use fibers::sync::mpsc;
use fibers::{BoxSpawn, Spawn};
use fibers_rpc::client::ClientServiceHandle;
use futures::{Async, Stream};
use prometrics::metrics::{Counter, MetricBuilder};
use raftlog::message::Message;
use raftlog::{self, ErrorKind, Result};

use super::client::RpcClient;
use NodeId;

/// Raft用のRPCメッセージの送受信を行うためのコンポーネント.
#[derive(Debug)]
pub struct Mailer {
    spawner: BoxSpawn,
    rpc_service: ClientServiceHandle,
    message_tx: mpsc::Sender<Message>,
    message_rx: mpsc::Receiver<Message>,
    metrics: Option<Metrics>,
}
impl Mailer {
    /// 新しい`Mailer`インスタンスを生成する.
    pub fn new<S>(spawner: S, rpc_service: ClientServiceHandle, metrics: Option<Metrics>) -> Self
    where
        S: Spawn + Send + 'static,
    {
        let (message_tx, message_rx) = mpsc::channel();
        Mailer {
            spawner: spawner.boxed(),
            rpc_service,
            message_tx,
            message_rx,
            metrics,
        }
    }

    pub(crate) fn mailbox(&self) -> Mailbox {
        Mailbox {
            message_tx: self.message_tx.clone(),
        }
    }
    pub(crate) fn try_recv_message(&mut self) -> Result<Option<Message>> {
        if let Async::Ready(message) = self.message_rx.poll().expect("Never fails") {
            if let Some(ref metrics) = self.metrics {
                if let Some(ref message) = message {
                    match *message {
                        raftlog::message::Message::RequestVoteCall { .. } => {
                            metrics.recv_request_vote_call_messages.increment()
                        }
                        raftlog::message::Message::RequestVoteReply { .. } => {
                            metrics.recv_request_vote_reply_messages.increment()
                        }
                        raftlog::message::Message::AppendEntriesCall { .. } => {
                            metrics.recv_append_entries_call_messages.increment()
                        }
                        raftlog::message::Message::AppendEntriesReply { .. } => {
                            metrics.recv_append_entries_reply_messages.increment()
                        }
                        raftlog::message::Message::InstallSnapshotCast { .. } => {
                            metrics.recv_install_snapshot_cast_messages.increment()
                        }
                    }
                }
            }
            if message.is_some() {
                Ok(message)
            } else {
                track_panic!(
                    ErrorKind::Other,
                    "Message receiving channel is disconnected"
                );
            }
        } else {
            Ok(None)
        }
    }
    pub(crate) fn send_message(&mut self, destination: &NodeId, message: Message) {
        if let Some(ref metrics) = self.metrics {
            match message {
                raftlog::message::Message::RequestVoteCall { .. } => {
                    metrics.send_request_vote_call_messages.increment()
                }
                raftlog::message::Message::RequestVoteReply { .. } => {
                    metrics.send_request_vote_reply_messages.increment()
                }
                raftlog::message::Message::AppendEntriesCall { .. } => {
                    metrics.send_append_entries_call_messages.increment()
                }
                raftlog::message::Message::AppendEntriesReply { .. } => {
                    metrics.send_append_entries_reply_messages.increment()
                }
                raftlog::message::Message::InstallSnapshotCast { .. } => {
                    metrics.send_install_snapshot_cast_messages.increment()
                }
            }
        }

        let client = RpcClient::new(destination.addr, &self.rpc_service);
        client.send_rpc_message(message);
    }
}

#[derive(Debug, Clone)]
pub struct Mailbox {
    message_tx: mpsc::Sender<Message>,
}
impl Mailbox {
    pub fn deliver(&self, message: Message) -> Result<()> {
        if self.message_tx.send(message).is_err() {
            track_panic!(ErrorKind::Other, "The receiver side is down");
        }
        Ok(())
    }
}

/// Prometheus metrics for Raft messages.
#[derive(Debug, Clone)]
pub struct Metrics {
    send_request_vote_call_messages: Counter,
    send_request_vote_reply_messages: Counter,
    send_append_entries_call_messages: Counter,
    send_append_entries_reply_messages: Counter,
    send_install_snapshot_cast_messages: Counter,
    recv_request_vote_call_messages: Counter,
    recv_request_vote_reply_messages: Counter,
    recv_append_entries_call_messages: Counter,
    recv_append_entries_reply_messages: Counter,
    recv_install_snapshot_cast_messages: Counter,
}
impl Metrics {
    /// Makes a new `Metrics` instance.
    pub fn new() -> Self {
        let mut builder = MetricBuilder::new();
        builder.namespace("frugalos_raft").subsystem("rpc");
        Metrics {
            send_request_vote_call_messages: builder
                .counter("send_messages_total")
                .label("type", "request_vote_call")
                .finish()
                .unwrap(),
            send_request_vote_reply_messages: builder
                .counter("send_messages_total")
                .label("type", "request_vote_reply")
                .finish()
                .unwrap(),
            send_append_entries_call_messages: builder
                .counter("send_messages_total")
                .label("type", "append_entries_call")
                .finish()
                .unwrap(),
            send_append_entries_reply_messages: builder
                .counter("send_messages_total")
                .label("type", "append_entries_reply")
                .finish()
                .unwrap(),
            send_install_snapshot_cast_messages: builder
                .counter("send_messages_total")
                .label("type", "install_snapshot_cast")
                .finish()
                .unwrap(),
            recv_request_vote_call_messages: builder
                .counter("recv_messages_total")
                .label("type", "request_vote_call")
                .finish()
                .unwrap(),
            recv_request_vote_reply_messages: builder
                .counter("recv_messages_total")
                .label("type", "request_vote_reply")
                .finish()
                .unwrap(),
            recv_append_entries_call_messages: builder
                .counter("recv_messages_total")
                .label("type", "append_entries_call")
                .finish()
                .unwrap(),
            recv_append_entries_reply_messages: builder
                .counter("recv_messages_total")
                .label("type", "append_entries_reply")
                .finish()
                .unwrap(),
            recv_install_snapshot_cast_messages: builder
                .counter("recv_messages_total")
                .label("type", "install_snapshot_cast")
                .finish()
                .unwrap(),
        }
    }
}
impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
