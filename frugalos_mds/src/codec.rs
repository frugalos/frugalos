// FIXME: protobufモジュールと統合(?)
use bytecodec::{DecodeExt, EncodeExt};

use crate::machine::Machine;
use crate::protobuf;
use crate::{ErrorKind, Result};

pub fn encode_machine(machine: &Machine) -> Result<Vec<u8>> {
    let snapshot = machine.to_snapshot();
    let bytes = track!(protobuf::snapshot_encoder().encode_into_bytes(snapshot))?;
    Ok(bytes)
}

pub fn decode_machine(snapshot: &[u8]) -> Result<Machine> {
    track_assert!(!snapshot.is_empty(), ErrorKind::InvalidInput);
    let snapshot = track!(protobuf::snapshot_decoder().decode_from_bytes(&snapshot))?;
    Ok(Machine::from_snapshot(snapshot))
}
