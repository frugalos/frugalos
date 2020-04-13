//! Definitions for frugalos truncate-bucket
use clap::{App, ArgMatches, SubCommand};
use sloggers::Build;
use sloggers::LoggerBuilder;

use command::rpc_addr;
use command::{
    bucket_seqno_arg, get_bucket_seqno, warn_if_there_are_unknown_fields, FrugalosSubcommand,
};

/// frugalos delete-bucket-contents
pub struct TruncateBucketCommand;

impl FrugalosSubcommand for TruncateBucketCommand {
    fn get_subcommand<'a, 'b: 'a>(&self) -> App<'a, 'b> {
        SubCommand::with_name("delete-bucket-contents")
            .arg(rpc_addr::get_arg())
            .arg(bucket_seqno_arg())
    }

    fn check_matches<'a>(&self, matches: &'a ArgMatches<'a>) -> Option<&'a ArgMatches<'a>> {
        matches.subcommand_matches("delete-bucket-contents")
    }

    fn handle_matches(
        &self,
        logger_builder: LoggerBuilder,
        matches: &ArgMatches,
        unknown_fields: &[String],
    ) {
        let mut logger = track_try_unwrap!(logger_builder.build());
        warn_if_there_are_unknown_fields(&mut logger, &unknown_fields);
        let rpc_addr = rpc_addr::from_matches(&matches);
        let bucket_seqno = track_try_unwrap!(get_bucket_seqno(matches));
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string(),
                                   "bucket_seqno" => format!("{:?}", bucket_seqno)));
        track_try_unwrap!(crate::daemon::truncate_bucket(
            &logger,
            rpc_addr,
            bucket_seqno
        ));
    }
}
