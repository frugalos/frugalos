use bytecodec;
use cannyls;
use fibers::sync::oneshot::MonitorError;
use fibers_tasque;
use libfrugalos;
use libfrugalos::entity::object::ObjectVersion;
use prometrics;
use raftlog;
use std::sync::mpsc::SendError;
use std::{io, net, num, string};
use trackable::error::TrackableError;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt};

/// 発生し得るエラーの種類.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// 入力値が不正.
    InvalidInput,

    /// 期待されたバージョン以外のオブジェクトに対して操作が行われようとした.
    ///
    /// 値は、オブジェクトの実際のバージョン.
    Unexpected(Option<ObjectVersion>),

    /// リーダ以外に対して要求が発行された.
    NotLeader,

    /// その他のエラー.
    Other,
}
impl TrackableErrorKind for ErrorKind {}

/// クレート固有の`Error`型.
#[derive(Debug, Clone, TrackableError)]
pub struct Error(TrackableError<ErrorKind>);
impl From<libfrugalos::Error> for Error {
    fn from(f: libfrugalos::Error) -> Self {
        let kind = match *f.kind() {
            libfrugalos::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            libfrugalos::ErrorKind::NotLeader => ErrorKind::NotLeader,
            libfrugalos::ErrorKind::Unexpected(v) => ErrorKind::Unexpected(v),
            libfrugalos::ErrorKind::Unavailable
            | libfrugalos::ErrorKind::Timeout
            | libfrugalos::ErrorKind::Other => ErrorKind::Other,
        };
        kind.takes_over(f).into()
    }
}
impl From<string::FromUtf8Error> for Error {
    fn from(f: string::FromUtf8Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<net::AddrParseError> for Error {
    fn from(f: net::AddrParseError) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
impl From<num::ParseIntError> for Error {
    fn from(f: num::ParseIntError) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
impl From<io::Error> for Error {
    fn from(f: io::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<cannyls::Error> for Error {
    fn from(f: cannyls::Error) -> Self {
        match *f.kind() {
            cannyls::ErrorKind::InvalidInput => ErrorKind::InvalidInput.takes_over(f).into(),
            _ => ErrorKind::Other.takes_over(f).into(),
        }
    }
}
impl From<raftlog::Error> for Error {
    fn from(f: raftlog::Error) -> Self {
        match *f.kind() {
            raftlog::ErrorKind::Busy => ErrorKind::Other.takes_over(f).into(),
            raftlog::ErrorKind::InconsistentState => ErrorKind::Other.takes_over(f).into(),
            raftlog::ErrorKind::InvalidInput => ErrorKind::InvalidInput.takes_over(f).into(),
            raftlog::ErrorKind::NotLeader => ErrorKind::NotLeader.takes_over(f).into(),
            raftlog::ErrorKind::Other => ErrorKind::Other.takes_over(f).into(),
        }
    }
}
impl From<MonitorError<Error>> for Error {
    fn from(f: MonitorError<Error>) -> Self {
        f.unwrap_or_else(|| {
            ErrorKind::Other
                .cause("Monitoring channel is disconnected")
                .into()
        })
    }
}
impl<T> From<SendError<T>> for Error {
    fn from(_: SendError<T>) -> Self {
        ErrorKind::Other.cause("Channel disconnected").into()
    }
}
impl From<prometrics::Error> for Error {
    fn from(f: prometrics::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<bytecodec::Error> for Error {
    fn from(f: bytecodec::Error) -> Self {
        ErrorKind::InvalidInput.takes_over(f).into()
    }
}
impl From<fibers_tasque::AsyncCallError> for Error {
    fn from(f: fibers_tasque::AsyncCallError) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}

pub fn to_rpc_error(e: Error) -> libfrugalos::Error {
    let kind = match *e.kind() {
        ErrorKind::InvalidInput => libfrugalos::ErrorKind::InvalidInput,
        ErrorKind::NotLeader => libfrugalos::ErrorKind::NotLeader,
        ErrorKind::Unexpected(v) => libfrugalos::ErrorKind::Unexpected(v),
        ErrorKind::Other => libfrugalos::ErrorKind::Other,
    };
    kind.takes_over(e).into()
}
