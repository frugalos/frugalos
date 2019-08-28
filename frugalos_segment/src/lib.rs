//! [Frugalos]クラスタ内の一つのセグメントに対応する構成要素群。
//!
//! [Frugalos]: https://github.com/frugalos/frugalos
#![warn(missing_docs)]
#![allow(clippy::new_ret_no_self)]
extern crate adler32;
extern crate byteorder;
extern crate cannyls;
extern crate cannyls_rpc;
extern crate ecpool;
extern crate fibers;
#[cfg(test)]
extern crate fibers_global;
extern crate fibers_rpc;
extern crate fibers_tasque;
extern crate frugalos_core;
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
mod full_sync;
mod metrics;
mod rpc_server;
mod service;
mod synchronizer;
mod test_util;
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

/// `frugalos_segment` の設定。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosSegmentConfig {
    /// A configuration for a dispersed client.
    #[serde(default)]
    pub dispersed_client: config::DispersedClientConfig,
    /// A configuration for a replicated client.
    #[serde(default)]
    pub replicated_client: config::ReplicatedClientConfig,
    /// A configuration for `MdsClient`.
    #[serde(default)]
    pub mds_client: config::MdsClientConfig,
}

impl Default for FrugalosSegmentConfig {
    fn default() -> Self {
        Self {
            dispersed_client: Default::default(),
            replicated_client: Default::default(),
            mds_client: Default::default(),
        }
    }
}
