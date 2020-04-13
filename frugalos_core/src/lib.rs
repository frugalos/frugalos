//! Frugal shared utilities.
#![allow(clippy::new_ret_no_self)]
extern crate rustracing;
extern crate rustracing_jaeger;
extern crate serde;
#[cfg(test)]
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate trackable;

pub mod serde_ext;
pub mod tracer;

pub const LUMP_ID_NAMESPACE_RAFTLOG: u8 = 0;
pub const LUMP_ID_NAMESPACE_OBJECT: u8 = 1;
