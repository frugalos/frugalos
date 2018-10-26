use bytecodec::{DecodeExt, EncodeExt, ErrorKind, Result, SizedEncode};
use libfrugalos::entity::bucket::{Bucket, DispersedBucket, MetadataBucket, ReplicatedBucket};
use libfrugalos::entity::device::{
    Device, FileDevice, MemoryDevice, SegmentAllocationPolicy, VirtualDevice, Weight,
};
use libfrugalos::entity::server::Server;
use protobuf_codec::field::branch::{Branch2, Branch3, Branch6};
use protobuf_codec::field::num::{F1, F2, F3, F4, F5, F6};
use protobuf_codec::message::{MessageDecode, MessageEncode};
use protobuf_codec::scalar::{
    DoubleDecoder, DoubleEncoder, StringDecoder, StringEncoder, Uint32Decoder, Uint32Encoder,
    Uint64Decoder, Uint64Encoder,
};
use trackable::error::ErrorKindExt;

use machine::{Command, DeviceGroup, NextSeqNo, Segment, SegmentTable, Snapshot};

//
// https://github.com/frugalos/frugalos/blob/master/frugalos_config/schema/config.proto
//
pub fn bucket_decoder() -> impl MessageDecode<Item = Bucket> {
    let base = protobuf_message_decoder![(
        required_oneof,
        (F1, metadata_bucket_decoder(), message),
        (F2, replicated_bucket_decoder(), message),
        (F3, dispersed_bucket_decoder(), message)
    )];
    base.map(|x| match x {
        Branch3::A(x) => Bucket::Metadata(x),
        Branch3::B(x) => Bucket::Replicated(x),
        Branch3::C(x) => Bucket::Dispersed(x),
    })
}

pub fn bucket_encoder() -> impl SizedEncode<Item = Bucket> + MessageEncode<Item = Bucket> {
    let base = protobuf_message_encoder![(
        required_oneof,
        (F1, metadata_bucket_encoder(), message),
        (F2, replicated_bucket_encoder(), message),
        (F3, dispersed_bucket_encoder(), message)
    )];
    base.map_from(|x: Bucket| match x {
        Bucket::Metadata(x) => Branch3::A(x),
        Bucket::Replicated(x) => Branch3::B(x),
        Bucket::Dispersed(x) => Branch3::C(x),
    })
}

pub fn metadata_bucket_decoder() -> impl MessageDecode<Item = MetadataBucket> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, StringDecoder::new()),
        (F4, Uint32Decoder::new()),
        (F5, Uint32Decoder::new())
    ];
    base.map(|x| MetadataBucket {
        id: x.0,
        seqno: x.1,
        device: x.2,
        segment_count: x.3,
        tolerable_faults: x.4,
    })
}

pub fn metadata_bucket_encoder(
) -> impl SizedEncode<Item = MetadataBucket> + MessageEncode<Item = MetadataBucket> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, StringEncoder::new()),
        (F4, Uint32Encoder::new()),
        (F5, Uint32Encoder::new())
    ];
    base.map_from(|x: MetadataBucket| {
        (x.id, x.seqno, x.device, x.segment_count, x.tolerable_faults)
    })
}

pub fn replicated_bucket_decoder() -> impl MessageDecode<Item = ReplicatedBucket> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, StringDecoder::new()),
        (F4, Uint32Decoder::new()),
        (F5, Uint32Decoder::new())
    ];
    base.map(|x| ReplicatedBucket {
        id: x.0,
        seqno: x.1,
        device: x.2,
        segment_count: x.3,
        tolerable_faults: x.4,
    })
}

pub fn replicated_bucket_encoder(
) -> impl SizedEncode<Item = ReplicatedBucket> + MessageEncode<Item = ReplicatedBucket> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, StringEncoder::new()),
        (F4, Uint32Encoder::new()),
        (F5, Uint32Encoder::new())
    ];
    base.map_from(|x: ReplicatedBucket| {
        (x.id, x.seqno, x.device, x.segment_count, x.tolerable_faults)
    })
}

pub fn dispersed_bucket_decoder() -> impl MessageDecode<Item = DispersedBucket> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, StringDecoder::new()),
        (F4, Uint32Decoder::new()),
        (F5, Uint32Decoder::new()),
        (F6, Uint32Decoder::new())
    ];
    base.map(|x| DispersedBucket {
        id: x.0,
        seqno: x.1,
        device: x.2,
        segment_count: x.3,
        tolerable_faults: x.4,
        data_fragment_count: x.5,
    })
}

pub fn dispersed_bucket_encoder(
) -> impl SizedEncode<Item = DispersedBucket> + MessageEncode<Item = DispersedBucket> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, StringEncoder::new()),
        (F4, Uint32Encoder::new()),
        (F5, Uint32Encoder::new()),
        (F6, Uint32Encoder::new())
    ];
    base.map_from(|x: DispersedBucket| {
        (
            x.id,
            x.seqno,
            x.device,
            x.segment_count,
            x.tolerable_faults,
            x.data_fragment_count,
        )
    })
}

pub fn server_decoder() -> impl MessageDecode<Item = Server> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, StringDecoder::new()),
        (F4, Uint32Decoder::new())
    ];
    base.try_map(|(id, seqno, host, port)| -> Result<_> {
        let host = track!(host.parse().map_err(|e| ErrorKind::InvalidInput.cause(e)))?;
        Ok(Server {
            id,
            seqno,
            host,
            port: port as u16,
        })
    })
}

pub fn server_encoder() -> impl SizedEncode<Item = Server> + MessageEncode<Item = Server> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, StringEncoder::new()),
        (F4, Uint32Encoder::new())
    ];
    base.map_from(|x: Server| (x.id, x.seqno, x.host.to_string(), u32::from(x.port)))
}

pub fn device_decoder() -> impl MessageDecode<Item = Device> {
    let base = protobuf_message_decoder![(
        required_oneof,
        (F1, virtual_device_decoder(), message),
        (F2, memory_device_decoder(), message),
        (F3, file_device_decoder(), message)
    )];
    base.map(|x| match x {
        Branch3::A(x) => Device::Virtual(x),
        Branch3::B(x) => Device::Memory(x),
        Branch3::C(x) => Device::File(x),
    })
}

pub fn device_encoder() -> impl SizedEncode<Item = Device> + MessageEncode<Item = Device> {
    let base = protobuf_message_encoder![(
        required_oneof,
        (F1, virtual_device_encoder(), unsized_message),
        (F2, memory_device_encoder(), message),
        (F3, file_device_encoder(), message)
    )];
    base.map_from(|x: Device| match x {
        Device::Virtual(x) => Branch3::A(x),
        Device::Memory(x) => Branch3::B(x),
        Device::File(x) => Branch3::C(x),
    })
}

pub fn virtual_device_decoder() -> impl MessageDecode<Item = VirtualDevice> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, weight_decoder(), message),
        (F4, StringDecoder::new(), repeated),
        (F5, Uint32Decoder::new())
    ];
    base.try_map(|x| -> Result<_> {
        let policy = match x.4 {
            0 => SegmentAllocationPolicy::ScatterIfPossible,
            1 => SegmentAllocationPolicy::Scatter,
            2 => SegmentAllocationPolicy::Neutral,
            3 => SegmentAllocationPolicy::Gather,
            n => track_panic!(ErrorKind::InvalidInput, "Unknown policy: {}", n),
        };
        Ok(VirtualDevice {
            id: x.0,
            seqno: x.1,
            weight: x.2.unwrap_or(Weight::Auto),
            children: x.3,
            policy,
        })
    })
}

pub fn virtual_device_encoder() -> impl MessageEncode<Item = VirtualDevice> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, weight_encoder(), required_message),
        (F4, StringEncoder::new(), repeated),
        (F5, Uint32Encoder::new())
    ];
    base.map_from(|x: VirtualDevice| (x.id, x.seqno, x.weight, x.children, x.policy as u32))
}

pub fn memory_device_decoder() -> impl MessageDecode<Item = MemoryDevice> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, weight_decoder(), message),
        (F4, StringDecoder::new()),
        (F5, Uint64Decoder::new())
    ];
    base.map(|x| MemoryDevice {
        id: x.0,
        seqno: x.1,
        weight: x.2.unwrap_or(Weight::Auto),
        server: x.3,
        capacity: x.4,
    })
}

pub fn memory_device_encoder(
) -> impl SizedEncode<Item = MemoryDevice> + MessageEncode<Item = MemoryDevice> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, weight_encoder(), required_message),
        (F4, StringEncoder::new()),
        (F5, Uint64Encoder::new())
    ];
    base.map_from(|x: MemoryDevice| (x.id, x.seqno, x.weight, x.server, x.capacity))
}

pub fn file_device_decoder() -> impl MessageDecode<Item = FileDevice> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, weight_decoder(), message),
        (F4, StringDecoder::new()),
        (F5, Uint64Decoder::new()),
        (F6, StringDecoder::new())
    ];
    base.map(|x| FileDevice {
        id: x.0,
        seqno: x.1,
        weight: x.2.unwrap_or(Weight::Auto),
        server: x.3,
        capacity: x.4,
        filepath: x.5.into(),
    })
}

pub fn file_device_encoder(
) -> impl SizedEncode<Item = FileDevice> + MessageEncode<Item = FileDevice> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, weight_encoder(), required_message),
        (F4, StringEncoder::new()),
        (F5, Uint64Encoder::new()),
        (F6, StringEncoder::new())
    ];
    base.try_map_from(|x: FileDevice| -> Result<_> {
        let filepath = track_assert_some!(x.filepath.to_str(), ErrorKind::InvalidInput);
        let capacity = track!(x.capacity().map_err(|e| ErrorKind::InvalidInput.cause(e)))?;
        Ok((
            x.id,
            x.seqno,
            x.weight,
            x.server,
            capacity,
            filepath.to_owned(),
        ))
    })
}

pub fn weight_decoder() -> impl MessageDecode<Item = Weight> {
    let base = protobuf_message_decoder![(
        oneof,
        (F1, Uint64Decoder::new()),
        (F2, DoubleDecoder::new())
    )];
    base.map(|x| match x {
        Some(Branch2::A(x)) => Weight::Absolute(x),
        Some(Branch2::B(x)) => Weight::Relative(x),
        None => Weight::Auto,
    })
}

pub fn weight_encoder() -> impl SizedEncode<Item = Weight> + MessageEncode<Item = Weight> {
    let base = protobuf_message_encoder![(
        oneof,
        (F1, Uint64Encoder::new()),
        (F2, DoubleEncoder::new())
    )];
    base.map_from(|x: Weight| match x {
        Weight::Absolute(x) => Some(Branch2::A(x)),
        Weight::Relative(x) => Some(Branch2::B(x)),
        Weight::Auto => None,
    })
}

//
// https://github.com/frugalos/frugalos/blob/master/frugalos_config/schema/state.proto
//
pub fn command_decoder() -> impl MessageDecode<Item = Command> {
    let base = protobuf_message_decoder![(
        required_oneof,
        (F1, put_bucket_decoder(), message),
        (F2, delete_bucket_decoder(), message),
        (F3, put_device_decoder(), message),
        (F4, delete_device_decoder(), message),
        (F5, put_server_decoder(), message),
        (F6, delete_server_decoder(), message)
    )];
    base.map(|x| match x {
        Branch6::A(bucket) => Command::PutBucket { bucket },
        Branch6::B(id) => Command::DeleteBucket { id },
        Branch6::C(device) => Command::PutDevice { device },
        Branch6::D(id) => Command::DeleteDevice { id },
        Branch6::E(server) => Command::PutServer { server },
        Branch6::F(id) => Command::DeleteServer { id },
    })
}

pub fn put_bucket_decoder() -> impl MessageDecode<Item = Bucket> {
    protobuf_message_decoder![(F1, bucket_decoder(), required_message)]
}

pub fn delete_bucket_decoder() -> impl MessageDecode<Item = String> {
    protobuf_message_decoder![(F1, StringDecoder::new())]
}

pub fn put_device_decoder() -> impl MessageDecode<Item = Device> {
    protobuf_message_decoder![(F1, device_decoder(), required_message)]
}

pub fn delete_device_decoder() -> impl MessageDecode<Item = String> {
    protobuf_message_decoder![(F1, StringDecoder::new())]
}

pub fn put_server_decoder() -> impl MessageDecode<Item = Server> {
    protobuf_message_decoder![(F1, server_decoder(), required_message)]
}

pub fn delete_server_decoder() -> impl MessageDecode<Item = String> {
    protobuf_message_decoder![(F1, StringDecoder::new())]
}

pub fn command_encoder() -> impl SizedEncode<Item = Command> + MessageEncode<Item = Command> {
    let base = protobuf_message_encoder![(
        required_oneof,
        (F1, put_bucket_encoder(), message),
        (F2, delete_bucket_encoder(), message),
        (F3, put_device_encoder(), message),
        (F4, delete_device_encoder(), message),
        (F5, put_server_encoder(), message),
        (F6, delete_server_encoder(), message)
    )];
    base.map_from(|x: Command| match x {
        Command::PutBucket { bucket } => Branch6::A(bucket),
        Command::DeleteBucket { id } => Branch6::B(id),
        Command::PutDevice { device } => Branch6::C(device),
        Command::DeleteDevice { id } => Branch6::D(id),
        Command::PutServer { server } => Branch6::E(server),
        Command::DeleteServer { id } => Branch6::F(id),
    })
}

pub fn put_bucket_encoder() -> impl SizedEncode<Item = Bucket> + MessageEncode<Item = Bucket> {
    protobuf_message_encoder![(F1, bucket_encoder(), required_message)]
}

pub fn delete_bucket_encoder() -> impl SizedEncode<Item = String> + MessageEncode<Item = String> {
    protobuf_message_encoder![(F1, StringEncoder::new())]
}

pub fn put_device_encoder() -> impl SizedEncode<Item = Device> + MessageEncode<Item = Device> {
    protobuf_message_encoder![(F1, device_encoder(), required_message)]
}

pub fn delete_device_encoder() -> impl SizedEncode<Item = String> + MessageEncode<Item = String> {
    protobuf_message_encoder![(F1, StringEncoder::new())]
}

pub fn put_server_encoder() -> impl SizedEncode<Item = Server> + MessageEncode<Item = Server> {
    protobuf_message_encoder![(F1, server_encoder(), required_message)]
}

pub fn delete_server_encoder() -> impl SizedEncode<Item = String> + MessageEncode<Item = String> {
    protobuf_message_encoder![(F1, StringEncoder::new())]
}

pub fn snapshot_decoder() -> impl MessageDecode<Item = Snapshot> {
    let base = protobuf_message_decoder![
        (F1, next_seqno_decoder(), message),
        (F2, bucket_decoder(), repeated_message),
        (F3, device_decoder(), repeated_message),
        (F4, server_decoder(), repeated_message),
        (F5, segment_table_decoder(), repeated_message)
    ];
    let base = protobuf_message_decoder![(F1, base, required_message)];

    base.map(|x| Snapshot {
        next_seqno: x.0.unwrap_or_else(Default::default),
        buckets: x.1,
        devices: x.2,
        servers: x.3,
        segment_tables: x.4,
    })
}

pub fn snapshot_encoder() -> impl MessageEncode<Item = Snapshot> {
    let base = protobuf_message_encoder![
        (F1, next_seqno_encoder(), required_message),
        (F2, bucket_encoder(), repeated_message),
        (F3, device_encoder(), repeated_message),
        (F4, server_encoder(), repeated_message),
        (F5, segment_table_encoder(), repeated_unsized_message)
    ];
    let base = protobuf_message_encoder![(F1, base, required_unsized_message)];

    base.map_from(|x: Snapshot| {
        (
            x.next_seqno,
            x.buckets,
            x.devices,
            x.servers,
            x.segment_tables,
        )
    })
}

pub fn next_seqno_decoder() -> impl MessageDecode<Item = NextSeqNo> {
    let base = protobuf_message_decoder![
        (F1, Uint32Decoder::new()),
        (F2, Uint32Decoder::new()),
        (F3, Uint32Decoder::new())
    ];
    base.map(|x| NextSeqNo {
        bucket: x.0,
        device: x.1,
        server: x.2,
    })
}

pub fn next_seqno_encoder() -> impl SizedEncode<Item = NextSeqNo> + MessageEncode<Item = NextSeqNo>
{
    let base = protobuf_message_encoder![
        (F1, Uint32Encoder::new()),
        (F2, Uint32Encoder::new()),
        (F3, Uint32Encoder::new())
    ];
    base.map_from(|x: NextSeqNo| (x.bucket, x.device, x.server))
}

pub fn segment_table_decoder() -> impl MessageDecode<Item = SegmentTable> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, segment_decoder(), repeated_message)
    ];
    base.map(|x| SegmentTable {
        bucket_id: x.0,
        segments: x.1,
    })
}

pub fn segment_table_encoder() -> impl MessageEncode<Item = SegmentTable> {
    let base = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, segment_encoder(), repeated_unsized_message)
    ];
    base.map_from(|x: SegmentTable| (x.bucket_id, x.segments))
}

pub fn segment_decoder() -> impl MessageDecode<Item = Segment> {
    let base = protobuf_message_decoder![(F1, device_group_decoder(), repeated_message)];
    base.map(|x| Segment { groups: x })
}

pub fn segment_encoder() -> impl MessageEncode<Item = Segment> {
    let base = protobuf_message_encoder![(F1, device_group_encoder(), repeated_unsized_message)];
    base.map_from(|x: Segment| x.groups)
}

pub fn device_group_decoder() -> impl MessageDecode<Item = DeviceGroup> {
    let base = protobuf_message_decoder![(F1, Uint32Decoder::new(), packed)];
    base.map(|x| DeviceGroup { members: x })
}

pub fn device_group_encoder() -> impl MessageEncode<Item = DeviceGroup> {
    let base = protobuf_message_encoder![(F1, Uint32Encoder::new(), packed)];
    base.map_from(|x: DeviceGroup| x.members)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_decoder_works() {
        let input = [
            10, 26, 10, 2, 24, 1, 34, 20, 10, 4, 115, 114, 118, 48, 26, 9, 49, 50, 55, 46, 48, 46,
            48, 46, 49, 32, 198, 111,
        ];
        track_try_unwrap!(snapshot_decoder().decode_from_bytes(&input));
    }

    #[test]
    fn command_decoder_works() {
        let input = [
            26, 66, 10, 64, 26, 62, 10, 9, 115, 114, 118, 48, 95, 100, 101, 118, 48, 26, 0, 34, 4,
            115, 114, 118, 48, 40, 209, 216, 180, 241, 114, 50, 35, 47, 116, 109, 112, 47, 102,
            114, 117, 103, 97, 108, 111, 115, 95, 116, 101, 115, 116, 47, 47, 115, 114, 118, 48,
            47, 48, 47, 100, 101, 118, 46, 108, 117, 115, 102,
        ];
        track_try_unwrap!(command_decoder().decode_from_bytes(&input));
    }
}
