//! Frugal shared utilities.
#![allow(clippy::new_ret_no_self)]
extern crate serde;
#[cfg(test)]
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate trackable;

pub mod serde_ext;
