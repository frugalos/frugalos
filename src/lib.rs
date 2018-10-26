//! Frugal Object Storage.
#![warn(missing_docs)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate cannyls;
extern crate cannyls_rpc;
extern crate fibers;
extern crate fibers_http_server;
extern crate fibers_rpc;
extern crate fibers_tasque;
extern crate frugalos_config;
extern crate frugalos_mds;
extern crate frugalos_raft;
extern crate frugalos_segment;
extern crate futures;
extern crate httpcodec;
extern crate jemalloc_ctl;
extern crate libfrugalos;
extern crate num_cpus;
extern crate prometrics;
extern crate raftlog;
extern crate rustracing;
extern crate rustracing_jaeger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate siphasher;
extern crate url;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

macro_rules! dump {
    ($($e:expr),*) => {
        format!(concat!($(stringify!($e), "={:?}; "),*), $($e),*)
    }
}

pub use error::{Error, ErrorKind};

pub mod daemon;

mod bucket;
mod client;
mod codec;
mod config_server;
mod error;
mod http;
mod rpc_server;
mod server;
mod service;

/// クレート固有の`Result`型。
pub type Result<T> = ::std::result::Result<T, Error>;
