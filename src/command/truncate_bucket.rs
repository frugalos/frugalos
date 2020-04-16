//! Definitions for frugalos truncate-bucket
use clap::{App, ArgMatches, SubCommand};
use sloggers::Build;
use sloggers::LoggerBuilder;

use command::bucket;
use command::rpc_addr;
use command::{warn_if_there_are_unknown_fields, FrugalosSubcommand};

/// frugalos truncate-bucket
pub struct TruncateBucketCommand;

impl FrugalosSubcommand for TruncateBucketCommand {
    fn get_subcommand<'a, 'b: 'a>(&self) -> App<'a, 'b> {
        SubCommand::with_name("truncate-bucket")
            .arg(rpc_addr::get_arg())
            .arg(bucket::seqno_arg())
    }

    fn check_matches<'a>(&self, matches: &'a ArgMatches<'a>) -> Option<&'a ArgMatches<'a>> {
        matches.subcommand_matches("truncate-bucket")
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
        let bucket_seqno = track_try_unwrap!(bucket::get_seqno(matches));
        let logger = logger.new(o!("rpc_addr" => rpc_addr.to_string(),
                                   "bucket_seqno" => format!("{:?}", bucket_seqno)));
        track_try_unwrap!(crate::daemon::truncate_bucket(
            &logger,
            rpc_addr,
            bucket_seqno
        ));
    }
}

#[cfg(test)]
mod tests {
    use clap::App;
    use command::bucket::get_seqno;
    use command::{truncate_bucket, FrugalosSubcommand};

    #[test]
    fn get_seqno_matches_works() {
        let truncate_bucket_command = truncate_bucket::TruncateBucketCommand;
        let matches = App::new("frugalos-test")
            .subcommand(truncate_bucket_command.get_subcommand())
            .get_matches_from(vec!["frugalos-test", "truncate-bucket", "--seqno", "123"]);
        if let Some(matches) = truncate_bucket_command.check_matches(&matches) {
            let bucket_seqno = get_seqno(matches).unwrap();
            assert_eq!(bucket_seqno, 123);
        } else {
            panic!();
        }
    }
}
