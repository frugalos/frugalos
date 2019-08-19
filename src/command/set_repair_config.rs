//! Definitions for frugalos set-repair-config
use clap::{App, Arg, ArgMatches, SubCommand};
use libfrugalos::repair::{RepairConfig, RepairIdleness};
use sloggers::Build;
use sloggers::LoggerBuilder;
use std::net::ToSocketAddrs;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use command::{default_rpc_server_bind_addr, warn_if_there_are_unknown_fields, FrugalosSubcommand};
use {Error, ErrorKind};

/// frugalos set-repair-config
pub struct SetRepairConfigCommand;

impl FrugalosSubcommand for SetRepairConfigCommand {
    fn get_subcommand<'a, 'b: 'a>(&self) -> App<'a, 'b> {
        SubCommand::with_name("set-repair-config")
            .arg(
                Arg::with_name("RPC_ADDR")
                    .long("rpc-addr")
                    .takes_value(true)
                    .default_value(default_rpc_server_bind_addr()),
            )
            .arg(
                Arg::with_name("REPAIR_IDLENESS_THRESHOLD")
                    .long("repair-idleness-threshold")
                    .takes_value(true),
            )
    }

    fn check_matches<'a>(&self, matches: &'a ArgMatches<'a>) -> Option<&'a ArgMatches<'a>> {
        matches.subcommand_matches("set-repair-config")
    }

    fn handle_matches(
        &self,
        logger_builder: LoggerBuilder,
        matches: &ArgMatches,
        unknown_fields: &[String],
    ) {
        // Set repair_idleness_threshold
        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let mut rpc_addrs = track_try_unwrap!(track_any_err!(matches
            .value_of("RPC_ADDR")
            .unwrap()
            .to_socket_addrs()));
        let repair_idleness_threshold = matches.value_of("REPAIR_IDLENESS_THRESHOLD").map(|str| {
            if str == "disabled" {
                RepairIdleness::Disabled
            } else {
                let duration_secs: f64 = track_try_unwrap!(str.parse().map_err(|_| Error::from(
                    ErrorKind::InvalidInput
                        .cause("repair-idleness-threshold must be a float or \"disabled\"")
                )));
                // TODO check if duration_secs is non-negative
                RepairIdleness::Threshold(Duration::from_millis((duration_secs * 1000.0) as u64))
            }
        });
        // TODO: accept repair_concurrency_limit and segment_gc_concurrency_limit
        let repair_config = RepairConfig {
            repair_concurrency_limit: None,
            repair_idleness_threshold,
            segment_gc_concurrency_limit: None,
        };
        let rpc_addr = rpc_addrs.nth(0).expect("No available TCP address");
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string(),
            "repair_config" => format!("{:?}", repair_config)));
        track_try_unwrap!(crate::daemon::set_repair_config(
            &logger,
            rpc_addr,
            repair_config,
        ));
    }
}
