//! [frugalos]の構成管理用クレート。
//!
//! [frugalos]: https://githug.com/frugalos/frugalos
#![warn(missing_docs)]
#![allow(clippy::new_ret_no_self)]
extern crate bytecodec;
extern crate byteorder;
extern crate cannyls;
extern crate fibers;
extern crate fibers_rpc;
extern crate fibers_tasque;
extern crate futures;
extern crate libfrugalos;
extern crate prometrics;
#[macro_use]
extern crate protobuf_codec;
extern crate frugalos_raft;
extern crate raftlog;
extern crate rendezvous_hash;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;
extern crate serde;
#[macro_use]
extern crate serde_derive;

macro_rules! dump {
    ($($e:expr),*) => {
        format!(concat!($(stringify!($e), "={:?}; "),*), $($e),*)
    }
}

pub use self::error::{Error, ErrorKind};
pub use crate::config::FrugalosConfigConfig;
pub use crate::machine::DeviceGroup;
pub use crate::rpc::RpcServer;
pub use crate::service::{Event, Service, ServiceHandle};

pub mod cluster;

mod builder;
mod config;
mod device_tree;
mod error;
mod machine;
mod protobuf;
mod rpc;
mod service;
#[cfg(test)]
mod test_util;

/// クレート固有の`Result`型。
pub type Result<T> = ::std::result::Result<T, Error>;
