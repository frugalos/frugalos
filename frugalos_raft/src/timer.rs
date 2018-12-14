use fibers;
use futures::{Future, Poll};
use raftlog::election::Role;
use raftlog::{Error as RaftError, ErrorKind as RaftErrorKind};
use rand::{self, Rng};
use std::time::Duration;
use trackable::error::ErrorKindExt;

/// Raft用のタイマー実装.
///
/// このタイマーは、パラメータとして「最小タイムアウト時間」と「最大タイムアウト時間」を受け取り、
/// 以下のルールに従って、各ロール用のタイムアウト時間を決定する.
///
/// - `Role::Follower`: 常に最大タイムアウト時間
/// - `Role::Leader`: 常に最小タイムアウト時間
/// - `Role::Candidate`: 最小と最大の間のいずれかの値を無作為に選択
#[derive(Debug, Clone)]
pub struct Timer {
    min_timeout: Duration,
    max_timeout: Duration,
}
impl Timer {
    /// 新しい`Timer`インスタンスを生成する.
    pub fn new(min_timeout: Duration, max_timeout: Duration) -> Self {
        assert!(min_timeout <= max_timeout);
        Timer {
            min_timeout,
            max_timeout,
        }
    }

    pub(crate) fn create_timeout(&self, role: Role) -> Timeout {
        let duration = match role {
            Role::Follower => self.max_timeout,
            Role::Candidate => {
                let min = duration_to_millis(self.min_timeout);
                let max = duration_to_millis(self.max_timeout);
                let millis = rand::thread_rng().gen_range(min, max);
                Duration::from_millis(millis)
            }
            Role::Leader => self.min_timeout,
        };
        let inner = fibers::time::timer::timeout(duration);
        Timeout(inner)
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_secs() * 1000 + u64::from(duration.subsec_nanos()) / 1_000_000
}

/// タイムアウトを表現した`Future`実装.
///
/// `Timer`によって内部的に生成される.
#[derive(Debug)]
pub struct Timeout(fibers::time::timer::Timeout);
impl Future for Timeout {
    type Item = ();
    type Error = RaftError;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self
            .0
            .poll()
            .map_err(|e| RaftErrorKind::Other.cause(e).into(),))
    }
}
