//! A module for defining frugalos' subcommands.

use clap::{App, ArgMatches};
use sloggers::LoggerBuilder;

pub mod rpc_addr;
pub mod set_repair_config;

/// Trait for frugalos' subcommands.
pub trait FrugalosSubcommand {
    /// Returns a subcommand definition to be passed to App::subcommand.
    fn get_subcommand<'a, 'b: 'a>(&self) -> App<'a, 'b>;

    /// Checks if given arguments match this command.
    fn check_matches<'a>(&self, matches: &'a ArgMatches<'a>) -> Option<&'a ArgMatches<'a>>;

    /// Performs action.
    fn handle_matches(
        &self,
        logger_builder: LoggerBuilder,
        matches: &ArgMatches,
        unknown_fields: &[String],
    );
}

/// Emits warnings if there are unknown fields in parsing the config file.
pub fn warn_if_there_are_unknown_fields(logger: &mut slog::Logger, unknown_fields: &[String]) {
    if !unknown_fields.is_empty() {
        warn!(
            logger,
            "The following unknown fields were passed:\n{:?}", unknown_fields
        );
    }
}
