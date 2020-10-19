//! https://github.com/frugalos/frugalos_raft/blob/master/schema/raft.proto
#![allow(missing_docs)]
use bytecodec::{self, Decode, DecodeExt, EncodeExt, SizedEncode};
use byteorder::{BigEndian, ByteOrder};
use protobuf_codec::field::num::{F1, F2};
use protobuf_codec::scalar::{Uint64Decoder, Uint64Encoder};
use raftlog::election::Ballot;
use raftlog::log::{LogEntry, LogPrefix};
use raftlog::{self, Result};
use std::ops::Range;
use trackable::error::ErrorKindExt;

pub fn decode_ballot(buf: &[u8]) -> Result<Ballot> {
    track!(decode_from_bytes(
        buf,
        raftlog_protobuf::state::BallotDecoder::default()
    ))
}

pub fn decode_log_prefix(buf: &[u8]) -> Result<LogPrefix> {
    track!(decode_from_bytes(
        buf,
        raftlog_protobuf::log::LogPrefixDecoder::default()
    ))
}

pub fn decode_log_prefix_index(buf: &[u8]) -> Result<Range<u64>> {
    track!(decode_from_bytes(buf, log_prefix_index_decoder()))
}

pub fn decode_log_entry(buf: &[u8]) -> Result<LogEntry> {
    track!(decode_from_bytes(
        buf,
        raftlog_protobuf::log::LogEntryDecoder::default()
    ))
}

pub fn encode_ballot(x: Ballot) -> Result<Vec<u8>> {
    track!(encode_into_bytes(
        x,
        raftlog_protobuf::state::BallotEncoder::default()
    ))
}

pub fn encode_log_prefix(x: LogPrefix) -> Result<Vec<u8>> {
    track!(encode_into_bytes(
        x,
        raftlog_protobuf::log::LogPrefixEncoder::default()
    ))
}

pub fn encode_log_prefix_index(x: Range<u64>) -> Result<Vec<u8>> {
    track!(encode_into_bytes(x, log_prefix_index_encoder()))
}

pub fn encode_log_entry(x: LogEntry) -> Result<Vec<u8>> {
    track!(encode_into_bytes(
        x,
        raftlog_protobuf::log::LogEntryEncoder::default()
    ))
}

pub fn log_prefix_index_decoder() -> impl Decode<Item = Range<u64>> {
    let base = protobuf_message_decoder!((F1, Uint64Decoder::new()), (F2, Uint64Decoder::new()));
    base.map(|(start, end)| Range { start, end })
}

pub fn log_prefix_index_encoder() -> impl SizedEncode<Item = Range<u64>> {
    let base = protobuf_message_encoder!((F1, Uint64Encoder::new()), (F2, Uint64Encoder::new()));
    base.map_from(|x: Range<u64>| (x.start, x.end))
}

fn decode_from_bytes<T, D>(bytes: &[u8], mut decoder: D) -> Result<T>
where
    D: Decode<Item = T>,
{
    // verify and remove checksum
    track_assert!(bytes.len() >= 5, raftlog::ErrorKind::InvalidInput);
    let (payload, trailer) = bytes.split_at(bytes.len() - 5);

    let checksum = adler32::adler32(payload).expect("Never fails");
    let expected = BigEndian::read_u32(trailer);
    track_assert_eq!(checksum, expected, raftlog::ErrorKind::InvalidInput);

    track!(decoder.decode_from_bytes(payload)).map_err(into_raftlog_error)
}

fn encode_into_bytes<T, E>(item: T, mut encoder: E) -> Result<Vec<u8>>
where
    E: SizedEncode<Item = T>,
{
    let mut bytes = track!(encoder.encode_into_bytes(item)).map_err(into_raftlog_error)?;

    // append checksum
    let checksum = adler32::adler32(&bytes[..]).expect("Never fails");
    let mut trailer = [0; 5];
    BigEndian::write_u32(&mut trailer[..], checksum);
    bytes.extend_from_slice(&trailer[..]);
    Ok(bytes)
}

fn into_raftlog_error(e: bytecodec::Error) -> raftlog::Error {
    raftlog::ErrorKind::InvalidInput.takes_over(e).into()
}
