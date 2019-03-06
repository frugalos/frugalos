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
extern crate frugalos_raft;
extern crate raftlog;
extern crate rand;
extern crate rustracing;
#[macro_use]
extern crate slog;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate trackable;

use std::ops::Range;

pub use error::{Error, ErrorKind};
pub use node::{Event, Node};
pub use service::{Service, ServiceHandle};

mod codec;
mod error;
mod machine;
mod node;
mod protobuf;
mod server;
mod service;

/// クレート固有の`Result`型.
pub type Result<T> = ::std::result::Result<T, Error>;

/// `frugalos_mds` の設定.
/// 以下の理由で現時点では module 毎に設定を struct で分けていない。
/// - ほとんどの設定が `Node` のためのものであること。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosMdsConfig {
    /// スナップショットを取る際の閾値(レンジの両端を含む).
    ///
    /// `Node` のローカルログの長さが、この値を超えた場合に、スナップショットの取得が開始される.
    #[serde(default = "default_threshold")]
    pub snapshot_threshold: Range<usize>,
}

impl Default for FrugalosMdsConfig {
    fn default() -> Self {
        Self {
            snapshot_threshold: default_threshold(),
        }
    }
}

fn default_threshold() -> Range<usize> {
    Range {
        start: 9_500,
        end: 10_500,
    }
}
