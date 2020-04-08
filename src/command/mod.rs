//! A module for defining frugalos' subcommands.

use clap::{App, Arg, ArgMatches};
use sloggers::LoggerBuilder;
use trackable::error::ErrorKindExt;

pub mod delete_bucket_contents;
pub mod rpc_addr;
pub mod set_repair_config;

use {Error, ErrorKind, Result};

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
/// バケツ番号の引数定義
pub fn bucket_seqno_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("BUCKET_SEQNO")
        .help("seqno of bucket")
        .long("seqno")
        .takes_value(true)
}
/// バケツ番号引数を取り出す
pub fn get_bucket_seqno(matches: &ArgMatches) -> Result<u32> {
    matches
        .value_of("BUCKET_SEQNO")
        .map(|v| v.parse::<u32>().map_err(|e| track!(Error::from(e))))
        .unwrap_or_else(|| {
            Err(Error::from(
                ErrorKind::InvalidInput.cause("bucket seqno must be specified"),
            ))
        })
}

#[cfg(test)]
mod tests {
    use clap::App;
    use command::{delete_bucket_contents, get_bucket_seqno, FrugalosSubcommand};

    #[test]
    fn get_bucket_seqno_matches_works() {
        let delete_buckets_contents_command = delete_bucket_contents::DeleteBucketContentsCommand;
        let matches = App::new("frugalos-test")
            .subcommand(delete_buckets_contents_command.get_subcommand())
            .get_matches_from(vec![
                "frugalos-test",
                "delete-bucket-contents",
                "--seqno",
                "123",
            ]);
        if let Some(matches) = delete_buckets_contents_command.check_matches(&matches) {
            let bucket_seqno = get_bucket_seqno(matches).unwrap();
            assert_eq!(bucket_seqno, 123);
        } else {
            panic!();
        }
    }
}
