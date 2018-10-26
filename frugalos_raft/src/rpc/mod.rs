use fibers_rpc::{Cast, ProcedureId};
use raftlog::message::{
    AppendEntriesCall, AppendEntriesReply, InstallSnapshotCast, RequestVoteCall, RequestVoteReply,
};
use raftlog_protobuf::message::{
    AppendEntriesCallDecoder, AppendEntriesCallEncoder, AppendEntriesReplyDecoder,
    AppendEntriesReplyEncoder, InstallSnapshotCastDecoder, InstallSnapshotCastEncoder,
    RequestVoteCallDecoder, RequestVoteCallEncoder, RequestVoteReplyDecoder,
    RequestVoteReplyEncoder,
};

pub use self::client::RpcClient;
pub use self::mail::{Mailer, Metrics as RpcMetrics};
pub use self::server::RpcServer;
pub use self::service::{Service, ServiceHandle};

mod client;
mod mail;
mod server;
mod service;

pub struct RequestVoteCallRpc;
impl Cast for RequestVoteCallRpc {
    const ID: ProcedureId = ProcedureId(0x0100_0000);
    const NAME: &'static str = "frugalos.raft.request_vote.call";

    type Notification = RequestVoteCall;
    type Encoder = RequestVoteCallEncoder;
    type Decoder = RequestVoteCallDecoder;
}

pub struct RequestVoteReplyRpc;
impl Cast for RequestVoteReplyRpc {
    const ID: ProcedureId = ProcedureId(0x0100_0001);
    const NAME: &'static str = "frugalos.raft.request_vote.reply";

    type Notification = RequestVoteReply;
    type Encoder = RequestVoteReplyEncoder;
    type Decoder = RequestVoteReplyDecoder;
}

pub struct AppendEntriesCallRpc;
impl Cast for AppendEntriesCallRpc {
    const ID: ProcedureId = ProcedureId(0x0100_0002);
    const NAME: &'static str = "frugalos.raft.append_entries.call";

    type Notification = AppendEntriesCall;
    type Encoder = AppendEntriesCallEncoder;
    type Decoder = AppendEntriesCallDecoder;
}

pub struct AppendEntriesReplyRpc;
impl Cast for AppendEntriesReplyRpc {
    const ID: ProcedureId = ProcedureId(0x0100_0003);
    const NAME: &'static str = "frugalos.raft.append_entries.reply";

    type Notification = AppendEntriesReply;
    type Encoder = AppendEntriesReplyEncoder;
    type Decoder = AppendEntriesReplyDecoder;
}

pub struct InstallSnapshotCastRpc;
impl Cast for InstallSnapshotCastRpc {
    const ID: ProcedureId = ProcedureId(0x0100_0004);
    const NAME: &'static str = "frugalos.raft.install_snapshot.cast";

    type Notification = InstallSnapshotCast;
    type Encoder = InstallSnapshotCastEncoder;
    type Decoder = InstallSnapshotCastDecoder;
}
