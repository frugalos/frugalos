//! [Frugalos]クラスタ内の一つのセグメントに対応する構成要素群。
//!
//! [Frugalos]: https://github.com/frugalos/frugalos
#![warn(missing_docs)]
extern crate adler32;
extern crate byteorder;
extern crate cannyls;
extern crate cannyls_rpc;
extern crate ecpool;
extern crate fibers;
extern crate fibers_rpc;
extern crate frugalos_mds;
extern crate frugalos_raft;
extern crate futures;
extern crate libfrugalos;
extern crate prometrics;
extern crate raftlog;
extern crate rand;
extern crate rustracing;
extern crate rustracing_jaeger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate siphasher;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use client::storage::{build_ec, ErasureCoder};
pub use client::Client;
pub use error::{Error, ErrorKind};
pub use service::{Service, ServiceHandle};

pub mod config;

mod client;
mod error;
mod service;
mod synchronizer;
mod util;

/// クレート固有の`Result`型。
pub type Result<T> = ::std::result::Result<T, Error>;

/// オブジェクトの値。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectValue {
    /// バージョン番号。
    pub version: libfrugalos::entity::object::ObjectVersion,

    /// 中身。
    pub content: Vec<u8>,
}
