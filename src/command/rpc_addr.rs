//! Definitions for RPC_ADDR argument.

use clap::{Arg, ArgMatches};
use std::net::{SocketAddr, ToSocketAddrs};

/// Returns an Arg for RPC_ADDR argument.
pub fn get_arg() -> Arg<'static, 'static> {
    Arg::with_name("RPC_ADDR")
        .long("rpc-addr")
        .takes_value(true)
        .default_value(default_rpc_server_bind_addr())
}

/// Retrieve a SocketAddr from RPC_ADDR argument.
pub fn from_matches(matches: &ArgMatches) -> SocketAddr {
    let mut rpc_addrs = track_try_unwrap!(track_any_err!(matches
        .value_of("RPC_ADDR")
        .unwrap()
        .to_socket_addrs()));
    rpc_addrs.next().expect("No available TCP address")
}

/// Returns default rpc server's port.
pub fn default_rpc_server_bind_addr() -> &'static str {
    "127.0.0.1:14278"
}
