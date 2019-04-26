//! Raftを使ったMDS(MetaData Store)を提供するcrate.
//!
//! MDSは、登録済みのオブジェクト一覧を保持しており、
//! 各オブジェクトは、以下の情報(メタデータ)を保持している:
//!
//! - バージョン番号
//! - ユーザ定義の任意のメタデータ(バイト列)
#![warn(missing_docs)]
#![allow(clippy::new_ret_no_self)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate byteorder;
extern crate cannyls;
extern crate fibers;
#[cfg(test)]
extern crate fibers_global;
extern crate fibers_rpc;
extern crate fibers_tasque;
extern crate futures;
extern crate libfrugalos;
extern crate patricia_tree;
extern crate prometrics;
#[macro_use]
extern crate protobuf_codec;
extern crate frugalos_core;
extern crate frugalos_raft;
extern crate raftlog;
extern crate rand;
extern crate rustracing;
extern crate rustracing_jaeger;
#[macro_use]
extern crate slog;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate trackable;

pub use config::FrugalosMdsConfig;
pub use error::{Error, ErrorKind};
pub use node::{Event, Node};
pub use service::{Service, ServiceHandle};

mod codec;
mod config;
mod error;
mod machine;
mod node;
mod protobuf;
mod server;
mod service;

/// クレート固有の`Result`型.
pub type Result<T> = ::std::result::Result<T, Error>;
