use libfrugalos::entity::bucket::{Bucket, BucketId};
use libfrugalos::entity::device::{Device, DeviceId};
use libfrugalos::entity::server::{Server, ServerId};

#[derive(Debug, Clone)]
pub enum Command {
    PutBucket { bucket: Bucket },
    DeleteBucket { id: BucketId },
    PutDevice { device: Device },
    DeleteDevice { id: DeviceId },
    PutServer { server: Server },
    DeleteServer { id: ServerId },
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub next_seqno: NextSeqNo,
    pub buckets: Vec<Bucket>,
    pub devices: Vec<Device>,
    pub servers: Vec<Server>,
    pub segment_tables: Vec<SegmentTable>,
}
impl Snapshot {
    pub fn initial(server: Server) -> Self {
        Snapshot {
            next_seqno: NextSeqNo {
                bucket: 0,
                server: server.seqno + 1,
                device: 0,
            },
            buckets: Vec::new(),
            devices: Vec::new(),
            servers: vec![server],
            segment_tables: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct NextSeqNo {
    pub bucket: u32,
    pub device: u32,
    pub server: u32,
}

#[derive(Debug, Clone)]
pub struct SegmentTable {
    pub bucket_id: BucketId,
    pub segments: Vec<Segment>,
}
impl SegmentTable {
    pub fn new(bucket_id: BucketId) -> Self {
        SegmentTable {
            bucket_id,
            segments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub groups: Vec<DeviceGroup>,
}

/// デバイスグループ。
///
/// 同一セグメントに属するメンバ群、を表現している。
#[derive(Debug, Clone)]
pub struct DeviceGroup {
    /// 同一セグメントに属するデバイス群のシーケンス番号。
    pub members: Vec<u32>,
}
