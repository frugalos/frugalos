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
