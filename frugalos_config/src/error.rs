use bytecodec;
use cannyls;
use fibers::sync::oneshot::MonitorError;
use libfrugalos;
use raftlog;
use std;
use std::convert::Into;
use trackable::error::TrackableError;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt};

/// クレート固有の`Error`型。
#[derive(Debug, Clone, TrackableError)]
pub struct Error(TrackableError<ErrorKind>);
impl From<std::net::AddrParseError> for Error {
    fn from(f: std::net::AddrParseError) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
impl From<std::io::Error> for Error {
    fn from(f: std::io::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<cannyls::Error> for Error {
    fn from(f: cannyls::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<raftlog::Error> for Error {
    fn from(f: raftlog::Error) -> Self {
        let kind = match *f.kind() {
            raftlog::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            raftlog::ErrorKind::NotLeader => ErrorKind::NotLeader,
            _ => ErrorKind::Other,
        };
        kind.takes_over(f).into()
    }
}
impl From<bytecodec::Error> for Error {
    fn from(f: bytecodec::Error) -> Self {
        ErrorKind::InvalidInput.takes_over(f).into()
    }
}
impl<E: Into<Error>> From<MonitorError<E>> for Error {
    fn from(f: MonitorError<E>) -> Self {
        f.map(Into::into).unwrap_or_else(|| {
            ErrorKind::Other
                .cause("Monitor channel disconnected")
                .into()
        })
    }
}
impl From<libfrugalos::Error> for Error {
    fn from(f: libfrugalos::Error) -> Self {
        let kind = match *f.kind() {
            libfrugalos::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            libfrugalos::ErrorKind::NotLeader => ErrorKind::NotLeader,
            _ => ErrorKind::Other,
        };
        kind.takes_over(f).into()
    }
}
impl From<fibers_rpc::Error> for Error {
    fn from(f: fibers_rpc::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl<T> From<std::sync::mpsc::SendError<T>> for Error
where
    T: Send + Sync + 'static,
{
    fn from(f: std::sync::mpsc::SendError<T>) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<std::sync::mpsc::TryRecvError> for Error {
    fn from(f: std::sync::mpsc::TryRecvError) -> Self {
        let kind = match f {
            std::sync::mpsc::TryRecvError::Disconnected => ErrorKind::Disconnected,
            std::sync::mpsc::TryRecvError::Empty => ErrorKind::WouldBlock,
        };
        kind.cause(f).into()
    }
}

pub fn to_rpc_error(e: Error) -> libfrugalos::Error {
    let kind = match *e.kind() {
        ErrorKind::InvalidInput => libfrugalos::ErrorKind::InvalidInput,
        ErrorKind::NotLeader => libfrugalos::ErrorKind::NotLeader,
        _ => libfrugalos::ErrorKind::Other,
    };
    kind.cause(e).into()
}

/// エラー種類。
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorKind {
    InvalidInput,
    NotLeader,
    Disconnected,
    WouldBlock,
    Other,
}
impl TrackableErrorKind for ErrorKind {}
