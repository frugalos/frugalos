use raftlog::election::{Ballot, Role};
use raftlog::log::{LogIndex, LogPrefix, LogSuffix};
use raftlog::message::Message;
use raftlog::{ErrorKind, Io, Result};
use slog::Logger;
use trackable::error::ErrorKindExt;

use storage::{self, Storage};
use timer::{Timeout, Timer};
use {LocalNodeId, Mailer, ServiceHandle};

/// `raftlog::Io`トレイトの実装.
#[derive(Debug)]
pub struct RaftIo {
    logger: Logger,
    node_id: LocalNodeId,
    service: ServiceHandle,
    storage: Storage,
    mailer: Mailer,
    timer: Timer,
}
impl RaftIo {
    /// 新しい`RaftIo`インスタンスを生成する.
    pub fn new(
        service: ServiceHandle,
        storage: Storage,
        mailer: Mailer,
        timer: Timer,
    ) -> Result<Self> {
        let node_id = storage.node_id();
        track!(service.add_node(node_id, &mailer))?;
        Ok(RaftIo {
            logger: storage.logger(),
            node_id,
            service,
            storage,
            mailer,
            timer,
        })
    }
}
impl Io for RaftIo {
    type SaveBallot = storage::SaveBallot;
    type LoadBallot = storage::LoadBallot;
    type SaveLog = storage::SaveLog;
    type LoadLog = storage::LoadLog;
    type Timeout = Timeout;
    fn try_recv_message(&mut self) -> Result<Option<Message>> {
        self.mailer
            .try_recv_message()
            .map_err(|e| ErrorKind::Other.takes_over(e).into())
    }
    fn send_message(&mut self, message: Message) {
        let node = match message.header().destination.as_str().parse() {
            Err(e) => {
                crit!(self.logger, "Wrong destination: {}", e);
                return;
            }
            Ok(id) => id,
        };
        self.mailer.send_message(&node, message);
    }
    fn save_ballot(&mut self, ballot: Ballot) -> Self::SaveBallot {
        self.storage.save_ballot(ballot)
    }
    fn load_ballot(&mut self) -> Self::LoadBallot {
        self.storage.load_ballot()
    }
    fn save_log_prefix(&mut self, prefix: LogPrefix) -> Self::SaveLog {
        self.storage.save_log_prefix(prefix)
    }
    fn save_log_suffix(&mut self, suffix: &LogSuffix) -> Self::SaveLog {
        self.storage.save_log_suffix(suffix)
    }
    fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> Self::LoadLog {
        self.storage.load_log(start, end)
    }
    fn create_timeout(&mut self, role: Role) -> Self::Timeout {
        self.timer.create_timeout(role)
    }
    fn is_busy(&mut self) -> bool {
        self.storage.is_busy()
    }
}
impl Drop for RaftIo {
    fn drop(&mut self) {
        if let Err(e) = track!(self.service.remove_node(self.node_id)) {
            warn!(
                self.logger,
                "Cannot remove the node {:?}: {}", self.node_id, e
            );
        }
    }
}
