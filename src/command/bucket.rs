//! Definitions for bucket argument.

use clap::{Arg, ArgMatches};
use trackable::error::ErrorKindExt;

use super::super::{Error, ErrorKind, Result};

/// バケツ番号の引数定義
pub fn seqno_arg<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("BUCKET_SEQNO")
        .help("seqno of bucket")
        .long("seqno")
        .takes_value(true)
}
/// バケツ番号引数を取り出す
pub fn get_seqno(matches: &ArgMatches) -> Result<u32> {
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
    use command::bucket::get_seqno;
    use command::{truncate_bucket, FrugalosSubcommand};

    #[test]
    fn get_seqno_matches_works() {
        let truncate_bucket_command = truncate_bucket::TruncateBucketCommand;
        let matches = App::new("frugalos-test")
            .subcommand(truncate_bucket_command.get_subcommand())
            .get_matches_from(vec![
                "frugalos-test",
                "delete-bucket-contents",
                "--seqno",
                "123",
            ]);
        if let Some(matches) = truncate_bucket_command.check_matches(&matches) {
            let bucket_seqno = get_seqno(matches).unwrap();
            assert_eq!(bucket_seqno, 123);
        } else {
            panic!();
        }
    }
}
