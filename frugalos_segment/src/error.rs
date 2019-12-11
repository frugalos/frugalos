use cannyls;
use ecpool;
use fibers::sync::oneshot::MonitorError;
use frugalos_mds;
use libfrugalos;
use libfrugalos::entity::object::ObjectVersion;
use prometrics;
use raftlog;
use std::io;
use std::sync::mpsc::RecvError;
use trackable::error::TrackableError;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt};

/// エラーの種類。
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum ErrorKind {
    UnexpectedVersion {
        current: Option<ObjectVersion>,
    },
    Invalid,
    Busy,
    Corrupted,

    /// Monitor was aborted
    MonitorAborted,
    Other,
}
impl TrackableErrorKind for ErrorKind {}

/// クレート固有の`Error`型。
#[derive(Debug, Clone, TrackableError)]
pub struct Error(TrackableError<ErrorKind>);
impl From<libfrugalos::Error> for Error {
    fn from(f: libfrugalos::Error) -> Self {
        let kind = match *f.kind() {
            libfrugalos::ErrorKind::InvalidInput => ErrorKind::Invalid,
            libfrugalos::ErrorKind::Unexpected(current) => ErrorKind::UnexpectedVersion { current },
            libfrugalos::ErrorKind::Unavailable => ErrorKind::Busy,
            libfrugalos::ErrorKind::Timeout
            | libfrugalos::ErrorKind::NotLeader
            | libfrugalos::ErrorKind::Other => ErrorKind::Other,
        };
        kind.takes_over(f).into()
    }
}
impl From<io::Error> for Error {
    fn from(f: io::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<RecvError> for Error {
    fn from(f: RecvError) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<frugalos_mds::Error> for Error {
    fn from(f: frugalos_mds::Error) -> Self {
        match *f.kind() {
            frugalos_mds::ErrorKind::Unexpected(version) => {
                let current = version.map(|v| ObjectVersion(v.0));
                ErrorKind::UnexpectedVersion { current }
                    .takes_over(f)
                    .into()
            }
            _ => ErrorKind::Other.takes_over(f).into(),
        }
    }
}
impl From<raftlog::Error> for Error {
    fn from(f: raftlog::Error) -> Self {
        // TODO: kindを見る
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<ecpool::Error> for Error {
    fn from(f: ecpool::Error) -> Self {
        // TODO: kindを見る
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<fibers_rpc::Error> for Error {
    fn from(f: fibers_rpc::Error) -> Self {
        let kind = match *f.kind() {
            fibers_rpc::ErrorKind::InvalidInput => ErrorKind::Invalid,
            fibers_rpc::ErrorKind::Unavailable => ErrorKind::Busy,
            fibers_rpc::ErrorKind::Timeout | fibers_rpc::ErrorKind::Other => ErrorKind::Other,
        };
        kind.takes_over(f).into()
    }
}
impl From<cannyls::Error> for Error {
    fn from(f: cannyls::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<MonitorError<Error>> for Error {
    fn from(f: MonitorError<Error>) -> Self {
        f.unwrap_or_else(|| {
            ErrorKind::Other
                .cause("Monitor channel disconnected")
                .into()
        })
    }
}
impl From<prometrics::Error> for Error {
    fn from(f: prometrics::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}

impl From<fibers_tasque::AsyncCallError> for Error {
    fn from(f: fibers_tasque::AsyncCallError) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
