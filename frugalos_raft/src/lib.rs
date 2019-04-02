//! `frugalos`のための`raftlog::Io`トレイトの実装を提供するクレート.
//!
//! ストレージとしては`cannyls`を、RPCとしては`fibers_rpc`を使用している.
#![warn(missing_docs)]
#![allow(clippy::new_ret_no_self)]
extern crate adler32;
extern crate atomic_immut;
extern crate bytecodec;
extern crate byteorder;
extern crate cannyls;
extern crate fibers;
#[cfg(test)]
extern crate fibers_global;
extern crate fibers_rpc;
extern crate futures;
extern crate prometrics;
#[macro_use]
extern crate protobuf_codec;
extern crate raftlog;
extern crate raftlog_protobuf;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

macro_rules! dump {
    ($($e:expr),*) => {
        format!(concat!($(stringify!($e), "={:?}; "),*), $($e),*)
    }
}

pub mod future_impls {
    //! `Future`トレイトの実装群.
    pub use storage::{LoadBallot, LoadLog, SaveBallot, SaveLog};
    pub use timer::Timeout;
}

pub use node::{LocalNodeId, NodeId};
pub use raft_io::RaftIo;
pub use rpc::{Mailer, RpcMetrics, Service, ServiceHandle};
pub use storage::{Storage, StorageMetrics};
pub use timer::Timer;

mod node;
mod protobuf;
mod raft_io;
mod rpc;
mod storage;
#[cfg(test)]
mod test_util;
mod timer;
mod util;

#[cfg(test)]
mod tests {
    use test_util::System;
    use trackable::result::TestResult;

    #[test]
    fn it_works() -> TestResult {
        let mut system = track!(System::new())?;
        track!(system.boot(3))?;
        let leader = track!(system.select_leader())?;
        let command = track!(system.propose(leader, b"foo".to_vec()))?;

        // 正常に受理(commit)された
        assert_eq!(command, b"foo");

        Ok(())
    }
}
