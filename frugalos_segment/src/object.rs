pub use frugalos_mds::rpc::ObjectVersion;

pub type ObjectId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectValue {
    pub content: Vec<u8>,
    pub version: ObjectVersion,
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ObjectHeader {
//     pub id: ObjectId,
//     pub version: ObjectVersion,
// }
pub use frugalos_mds::rpc::ObjectSummary as ObjectHeader;
