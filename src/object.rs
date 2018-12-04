use libfrugalos::entity::object::ObjectVersion;

/// An object with its segment number.
#[derive(Debug, Clone, Copy)]
pub struct SegmentedObject {
    /// The segment number.
    pub segment: u16,

    /// The version ob the object.
    pub version: ObjectVersion,
}
