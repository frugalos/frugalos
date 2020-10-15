// https://github.com/frugalos/frugalos/blob/master/frugalos_mds/schema/mds.proto
#![allow(missing_docs)]
use bytecodec::fixnum::{U64beDecoder, U64beEncoder};
use bytecodec::{DecodeExt, EncodeExt, SizedEncode};
use libfrugalos::entity::object::{Metadata, ObjectPrefix, ObjectVersion};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use patricia_tree::node::{NodeDecoder, NodeEncoder};
use protobuf_codec::field::branch::{Branch2, Branch3, Branch5};
use protobuf_codec::field::num::{F1, F2, F3, F4, F5};
use protobuf_codec::message::{MessageDecode, MessageEncode};
use protobuf_codec::scalar::{
    BytesDecoder, BytesEncoder, CustomBytesDecoder, CustomBytesEncoder, StringDecoder,
    StringEncoder, Uint64Decoder, Uint64Encoder,
};

use crate::machine::{Command, Snapshot};

pub fn command_decoder() -> impl MessageDecode<Item = Command> {
    let base = protobuf_message_decoder![(
        required_oneof,
        (F1, put_command_decoder(), message),
        (F2, delete_command_decoder(), message),
        (F3, delete_version_command_decoder(), message),
        (F4, delete_by_range_command_decoder(), message),
        (F5, delete_by_prefix_command_decoder(), message)
    )];
    base.map(|x| match x {
        Branch5::A(x) => Command::Put {
            object_id: x.0,
            userdata: x.1,
            expect: x.2,
            put_content_timeout: Seconds(x.3),
        },
        Branch5::B(x) => Command::Delete {
            object_id: x.0,
            expect: x.1,
        },
        Branch5::C(x) => Command::DeleteByVersion {
            object_version: ObjectVersion(x),
        },
        Branch5::D(x) => Command::DeleteByRange {
            version_from: ObjectVersion(x.0),
            version_to: ObjectVersion(x.1),
        },
        Branch5::E(x) => Command::DeleteByPrefix {
            prefix: ObjectPrefix(x),
        },
    })
}

pub fn command_encoder() -> impl SizedEncode<Item = Command> + MessageEncode<Item = Command> {
    let base = protobuf_message_encoder![(
        required_oneof,
        (F1, put_command_encoder(), message),
        (F2, delete_command_encoder(), message),
        (F3, delete_version_command_encoder(), message),
        (F4, delete_by_range_command_encoder(), message),
        (F5, delete_by_prefix_command_encoder(), message)
    )];
    base.map_from(|x: Command| match x {
        Command::Put {
            object_id,
            userdata,
            expect,
            put_content_timeout,
        } => Branch5::A((object_id, userdata, expect, put_content_timeout.0)),
        Command::Delete { object_id, expect } => Branch5::B((object_id, expect)),
        Command::DeleteByVersion { object_version } => Branch5::C(object_version.0),
        Command::DeleteByRange {
            version_from,
            version_to,
        } => Branch5::D((version_from.0, version_to.0)),
        Command::DeleteByPrefix { prefix } => Branch5::E(prefix.0),
    })
}

#[allow(dead_code)]
pub type PutCommand = (String, Vec<u8>, Expect, u64);

#[allow(dead_code)]
pub type DeleteCommand = (String, Expect);

#[allow(dead_code)]
pub type DeleteVersionCommand = u64;

#[allow(dead_code)]
pub type DeleteByRangeCommand = (u64, u64);

#[allow(dead_code)]
pub type DeleteByPrefixCommand = String;

pub fn put_command_decoder() -> impl MessageDecode<Item = PutCommand> {
    let base = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, BytesDecoder::new()),
        (F3, expect_decoder(), message),
        (F4, Uint64Decoder::new())
    ];
    base.map(|x| (x.0, x.1, x.2.unwrap_or(Expect::Any), x.3))
}

pub fn put_command_encoder(
) -> impl SizedEncode<Item = PutCommand> + MessageEncode<Item = PutCommand> {
    protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, BytesEncoder::new()),
        (F3, expect_encoder(), required_unsized_message),
        (F4, Uint64Encoder::new())
    ]
}

pub fn delete_command_decoder() -> impl MessageDecode<Item = DeleteCommand> {
    let base =
        protobuf_message_decoder![(F1, StringDecoder::new()), (F2, expect_decoder(), message)];
    base.map(|x| (x.0, x.1.unwrap_or(Expect::Any)))
}

pub fn delete_command_encoder(
) -> impl SizedEncode<Item = DeleteCommand> + MessageEncode<Item = DeleteCommand> {
    protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, expect_encoder(), required_unsized_message)
    ]
}

pub fn delete_version_command_decoder() -> impl MessageDecode<Item = DeleteVersionCommand> {
    protobuf_message_decoder![(F1, Uint64Decoder::new())]
}

pub fn delete_version_command_encoder(
) -> impl SizedEncode<Item = DeleteVersionCommand> + MessageEncode<Item = DeleteVersionCommand> {
    protobuf_message_encoder![(F1, Uint64Encoder::new())]
}

pub fn delete_by_range_command_decoder() -> impl MessageDecode<Item = DeleteByRangeCommand> {
    let base = protobuf_message_decoder![(F1, Uint64Decoder::new()), (F2, Uint64Decoder::new())];
    base.map(|x| (x.0, x.1))
}

pub fn delete_by_range_command_encoder(
) -> impl SizedEncode<Item = DeleteByRangeCommand> + MessageEncode<Item = DeleteByRangeCommand> {
    protobuf_message_encoder![(F1, Uint64Encoder::new()), (F2, Uint64Encoder::new())]
}

pub fn delete_by_prefix_command_decoder() -> impl MessageDecode<Item = DeleteByPrefixCommand> {
    protobuf_message_decoder![(F1, StringDecoder::new())]
}

pub fn delete_by_prefix_command_encoder(
) -> impl SizedEncode<Item = DeleteByPrefixCommand> + MessageEncode<Item = DeleteByPrefixCommand> {
    protobuf_message_encoder![(F1, StringEncoder::new())]
}

pub fn expect_decoder() -> impl MessageDecode<Item = Expect> {
    let base = protobuf_message_decoder![(
        oneof,
        (F1, versions_decoder(), message),
        (F2, versions_decoder(), message),
        (F3, empty_decoder(), message)
    )];
    base.map(|x| match x {
        Some(Branch3::A(versions)) => Expect::IfMatch(versions),
        Some(Branch3::B(versions)) => Expect::IfNoneMatch(versions),
        Some(Branch3::C(_)) => Expect::None,
        None => Expect::Any,
    })
}

pub fn expect_encoder() -> impl MessageEncode<Item = Expect> {
    let base = protobuf_message_encoder![(
        oneof,
        (F1, versions_encoder(), unsized_message),
        (F2, versions_encoder(), unsized_message),
        (F3, empty_encoder(), message)
    )];
    base.map_from(|x: Expect| match x {
        Expect::IfMatch(versions) => Some(Branch3::A(versions)),
        Expect::IfNoneMatch(versions) => Some(Branch3::B(versions)),
        Expect::None => Some(Branch3::C(())),
        Expect::Any => None,
    })
}

pub fn versions_decoder() -> impl MessageDecode<Item = Vec<ObjectVersion>> {
    // FIXME: `collect()`を呼ばなくて済むようにする
    let base = protobuf_message_decoder![(F1, Uint64Decoder::new(), packed)];
    base.map(|versions: Vec<_>| versions.into_iter().map(ObjectVersion).collect())
}

pub fn versions_encoder() -> impl MessageEncode<Item = Vec<ObjectVersion>> {
    let base = protobuf_message_encoder![(F1, Uint64Encoder::new(), packed)];
    base.map_from(|x: Vec<ObjectVersion>| x.into_iter().map(|v| v.0))
}

pub fn empty_decoder() -> impl MessageDecode<Item = ()> {
    protobuf_message_decoder![]
}

pub fn empty_encoder() -> impl SizedEncode<Item = ()> + MessageEncode<Item = ()> {
    protobuf_message_encoder![]
}

pub fn snapshot_decoder() -> impl MessageDecode<Item = Snapshot> {
    let patricia =
        CustomBytesDecoder::new(NodeDecoder::new(U64beDecoder::new().map(ObjectVersion)));
    let base = protobuf_message_decoder![(
        required_oneof,
        (F1, objects_decoder(), message),
        (F2, patricia)
    )];
    base.map(|x| match x {
        Branch2::A(x) => Snapshot::Assoc(x),
        Branch2::B(x) => Snapshot::Patricia(x.into()),
    })
}

pub fn snapshot_encoder() -> impl MessageEncode<Item = Snapshot> {
    let patricia = CustomBytesEncoder::new(
        NodeEncoder::new(U64beEncoder::new().map_from(|v: ObjectVersion| v.0)).pre_encode(),
    );
    let base = protobuf_message_encoder![(
        required_oneof,
        (F1, objects_encoder(), unsized_message),
        (F2, patricia)
    )];
    base.map_from(|x: Snapshot| match x {
        Snapshot::Assoc(x) => Branch2::A(x),
        Snapshot::Patricia(x) => Branch2::B(x.into()),
    })
}

pub fn objects_decoder() -> impl MessageDecode<Item = Vec<(String, Metadata)>> {
    let map = protobuf_message_decoder![
        (F1, StringDecoder::new()),
        (F2, metadata_decoder(), required_message)
    ];
    protobuf_message_decoder![(F1, map, repeated_message)]
}

pub fn objects_encoder() -> impl MessageEncode<Item = Vec<(String, Metadata)>> {
    let map = protobuf_message_encoder![
        (F1, StringEncoder::new()),
        (F2, metadata_encoder(), required_message)
    ];
    protobuf_message_encoder![(F1, map, repeated_message)]
}

pub fn metadata_decoder() -> impl MessageDecode<Item = Metadata> {
    let base = protobuf_message_decoder![(F1, Uint64Decoder::new()), (F2, BytesDecoder::new())];
    base.map(|x| Metadata {
        version: ObjectVersion(x.0),
        data: x.1,
    })
}

pub fn metadata_encoder() -> impl SizedEncode<Item = Metadata> + MessageEncode<Item = Metadata> {
    let base = protobuf_message_encoder![(F1, Uint64Encoder::new()), (F2, BytesEncoder::new())];
    base.map_from(|x: Metadata| (x.version.0, x.data))
}
