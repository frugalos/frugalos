use bytecodec;
use cannyls;
use fibers::sync::oneshot::MonitorError;
use fibers_http_server;
use fibers_rpc;
use fibers_tasque;
use frugalos_config;
use frugalos_segment;
use libfrugalos;
use libfrugalos::entity::object::ObjectVersion;
use prometrics;
use raftlog;
use std;
use std::io;
use trackable::error::TrackableError;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt};

/// クレート固有の`Error`型。
#[derive(Debug, Clone, TrackableError, Serialize)]
pub struct Error(TrackableError<ErrorKind>);
impl From<io::Error> for Error {
    fn from(f: io::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<frugalos_segment::Error> for Error {
    fn from(f: frugalos_segment::Error) -> Self {
        if let frugalos_segment::ErrorKind::UnexpectedVersion { current } = *f.kind() {
            ErrorKind::Unexpected(current).takes_over(f).into()
        } else {
            ErrorKind::Other.takes_over(f).into()
        }
    }
}
impl From<raftlog::Error> for Error {
    fn from(f: raftlog::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<frugalos_config::Error> for Error {
    fn from(f: frugalos_config::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<cannyls::Error> for Error {
    fn from(f: cannyls::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<fibers_tasque::AsyncCallError> for Error {
    fn from(f: fibers_tasque::AsyncCallError) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<MonitorError<frugalos_segment::Error>> for Error {
    fn from(f: MonitorError<frugalos_segment::Error>) -> Self {
        match f {
            MonitorError::Failed(e) => Error::from(e),
            MonitorError::Aborted => ErrorKind::Other
                .cause("Monitor channel disconnected")
                .into(),
        }
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
impl From<fibers_http_server::Error> for Error {
    fn from(f: fibers_http_server::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<prometrics::Error> for Error {
    fn from(f: prometrics::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<fibers_rpc::Error> for Error {
    fn from(f: fibers_rpc::Error) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<libfrugalos::Error> for Error {
    fn from(f: libfrugalos::Error) -> Self {
        let kind = match *f.kind() {
            libfrugalos::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            _ => ErrorKind::Other,
        };
        kind.cause(f).into()
    }
}
impl From<std::num::ParseIntError> for Error {
    fn from(f: std::num::ParseIntError) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
impl From<bytecodec::Error> for Error {
    fn from(f: bytecodec::Error) -> Self {
        ErrorKind::IllegalEncoding.cause(f).into()
    }
}
impl From<std::string::FromUtf8Error> for Error {
    fn from(f: std::string::FromUtf8Error) -> Self {
        ErrorKind::InvalidUtf8String.cause(f).into()
    }
}

/// エラーの種類。
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ErrorKind {
    InvalidInput,
    NotFound,
    Unexpected(Option<ObjectVersion>),
    /// An error occurred while encoding/decoding bytes.
    IllegalEncoding,
    /// An error occurred while encoding/decoding a utf8 string.
    InvalidUtf8String,
    Other,
}
impl TrackableErrorKind for ErrorKind {}
