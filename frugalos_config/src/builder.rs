use libfrugalos::entity::bucket::Bucket;
use libfrugalos::entity::device::{Device, DeviceId, SegmentAllocationPolicy, VirtualDevice};
use rendezvous_hash::{Capacity, IdNode, WeightedNode};
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use std::collections::{BTreeMap, HashMap, HashSet};

use machine::{DeviceGroup, Segment, SegmentTable};
use {ErrorKind, Result};

type BucketNo = u32;
type DeviceNo = u32;
type Devices = BTreeMap<DeviceId, Device>;
type SegmentNo = u16;
type HashRing = RendezvousNodes<WeightedNode<IdNode<DeviceNo>>, DefaultNodeHasher>;

#[derive(Debug)]
pub struct SegmentTableBuilder<'a> {
    devices: &'a Devices,
}
impl<'a> SegmentTableBuilder<'a> {
    pub fn new(devices: &'a Devices) -> Self {
        SegmentTableBuilder { devices }
    }
    pub fn build(&self, bucket: &Bucket) -> Result<SegmentTable> {
        let segments_builder = SegmentsBuilder {
            bucket_no: bucket.seqno(),
            root: &self.devices[bucket.device()],
            devices: self.devices,
            segment_count: bucket.segment_count(),
            device_group_size: bucket.device_group_size(),

            device_states: HashMap::new(),
            segment_owners: HashMap::new(),
            gathered_segments: HashMap::new(),
        };
        let segments = track!(segments_builder.build())?;
        Ok(SegmentTable {
            bucket_id: bucket.id().clone(),
            segments,
        })
    }
}

struct SegmentsBuilder<'a> {
    bucket_no: BucketNo,
    root: &'a Device,
    devices: &'a Devices,
    segment_count: u16,
    device_group_size: u8,

    device_states: HashMap<DeviceNo, DeviceState<'a>>,

    // 直接的・間接的に該当セグメントを保持しているデバイスの集合
    segment_owners: HashMap<SegmentNo, HashSet<DeviceNo>>,

    gathered_segments: HashMap<SegmentNo, DeviceNo>,
}
impl<'a> SegmentsBuilder<'a> {
    pub fn build(mut self) -> Result<Vec<Segment>> {
        let mut device_states = HashMap::new();
        self.init_device_states(self.root, &mut device_states);
        self.assign_capacities(
            self.segment_count as usize * self.device_group_size as usize,
            self.root,
            &mut device_states,
        );
        self.device_states = device_states;

        let mut segments = Vec::with_capacity(self.segment_count as usize);
        for segment_no in 0..self.segment_count {
            let mut members = Vec::with_capacity(self.device_group_size as usize);
            for member_no in 0..self.device_group_size {
                let key = SlotKey {
                    bucket_no: self.bucket_no,
                    segment_no,
                    member_no,
                };
                let allocated_device =
                    track!(self.allocate_segment_slot(key, self.root), "{}", dump!(key))?;
                members.push(allocated_device);
            }
            let segment = Segment {
                groups: vec![DeviceGroup { members }],
            };
            segments.push(segment);
        }
        Ok(segments)
    }

    fn allocate_segment_slot(&mut self, key: SlotKey, device: &Device) -> Result<DeviceNo> {
        self.segment_owners
            .entry(key.segment_no)
            .or_insert_with(HashSet::new)
            .insert(device.seqno());
        self.device_states
            .get_mut(&device.seqno())
            .expect("Never fails")
            .allocated += 1;

        if let Device::Virtual(ref d) = *device {
            track_assert!(!d.children.is_empty(), ErrorKind::InvalidInput);
            let child_no = match d.policy {
                SegmentAllocationPolicy::Neutral => self.select_neutral_slot(key, d),
                SegmentAllocationPolicy::Scatter => {
                    if self.device_group_size as usize <= d.children.len() {
                        self.select_scatter_slot(key, d)
                    } else {
                        track_panic!(ErrorKind::InvalidInput, "Too few children");
                    }
                }
                SegmentAllocationPolicy::ScatterIfPossible => {
                    if self.device_group_size as usize <= d.children.len() {
                        self.select_scatter_slot(key, d)
                    } else {
                        self.select_neutral_slot(key, d)
                    }
                }
                SegmentAllocationPolicy::Gather => self.select_gather_slot(key, d),
            };
            let child = self.get_device(child_no);
            track!(self.allocate_segment_slot(key, child))
        } else {
            Ok(device.seqno())
        }
    }
    fn is_same_device_group(&self, segment_no: SegmentNo, device_no: DeviceNo) -> bool {
        self.segment_owners
            .get(&segment_no)
            .map_or(false, |g| g.contains(&device_no))
    }
    fn select_scatter_slot(&mut self, key: SlotKey, parent: &VirtualDevice) -> DeviceNo {
        let ring = self.get_ring(parent.seqno);
        let child = ring
            .calc_candidates(&key)
            .find(|item| {
                let device_no = *item.node;
                let d = &self.device_states[&device_no];
                d.allocated <= d.capacity && !self.is_same_device_group(key.segment_no, device_no)
            }).map(|item| *item.node);
        if let Some(child) = child {
            child
        } else {
            // FIXME: この場合(i.e., capacity over)の割当方式を少し検討したいかも
            ring.calc_candidates(&key)
                .find(|item| {
                    let device_no = *item.node;
                    !self.is_same_device_group(key.segment_no, device_no)
                }).map(|item| *item.node)
                .expect("Never fails")
        }
    }
    fn select_neutral_slot(&mut self, key: SlotKey, parent: &VirtualDevice) -> DeviceNo {
        let ring = self.get_ring(parent.seqno);
        let child = ring
            .calc_candidates(&key)
            .find(|item| {
                let d = &self.device_states[&*item.node];
                d.allocated <= d.capacity
            }).map(|item| *item.node);
        if let Some(child) = child {
            child
        } else {
            // NOTE: gather経由の場合等にここに来ることがある
            // FIXME: この場合の割当方式を少し検討したいかも
            *ring.calc_candidates(&key).nth(0).expect("Never fails").node
        }
    }
    fn select_gather_slot(&mut self, key: SlotKey, parent: &VirtualDevice) -> DeviceNo {
        if key.member_no == 0 {
            let child_no = self.select_neutral_slot(key, parent);
            self.gathered_segments.insert(key.segment_no, child_no);
            child_no
        } else {
            self.gathered_segments[&key.segment_no]
        }
    }

    #[cfg_attr(feature = "cargo-clippy", allow(mut_from_ref))]
    fn get_device<'b, 'c>(&'b self, device_no: DeviceNo) -> &'c Device {
        // NOTE: 現状のRustの借用チェックの制約を回避するためのワークアラウンド
        let device = self.device_states[&device_no].device;
        unsafe { &*(device as *const _) }
    }

    #[cfg_attr(feature = "cargo-clippy", allow(mut_from_ref))]
    fn get_ring(&self, device_no: DeviceNo) -> &mut HashRing {
        // NOTE: 現状のRustの借用チェックの制約を回避するためのワークアラウンド
        let ring = &self.device_states[&device_no].ring;
        unsafe { &mut *(ring as *const _ as *mut _) }
    }

    fn init_device_states(
        &self,
        device: &'a Device,
        states: &mut HashMap<DeviceNo, DeviceState<'a>>,
    ) {
        let mut state = DeviceState {
            weight: 0,
            allocated: 0,
            capacity: 0,
            ring: HashRing::default(),
            device,
        };
        match *device {
            Device::Virtual(ref d) => {
                let mut total_weight = 0;
                for c in &d.children {
                    let c = &self.devices[c];
                    self.init_device_states(c, states);
                    let child_weight = states[&c.seqno()].weight;
                    total_weight += child_weight;
                    state.ring.insert(WeightedNode::new(
                        IdNode::new(c.seqno()),
                        Capacity::new(child_weight as f64).expect("Never fails"),
                    ));
                }
                state.weight = d.weight.calculate(total_weight);
            }
            Device::Memory(ref d) => state.weight = d.weight(),
            Device::File(ref d) => state.weight = d.weight(),
        }
        states.insert(device.seqno(), state);
    }
    fn assign_capacities(
        &self,
        slots: usize,
        device: &'a Device,
        states: &mut HashMap<DeviceNo, DeviceState<'a>>,
    ) {
        if slots == 0 {
            return;
        }
        states
            .get_mut(&device.seqno())
            .expect("Never fails")
            .capacity = slots;
        let parent_weight = states[&device.seqno()].weight as f64;
        if let Device::Virtual(ref d) = *device {
            for c in &d.children {
                let child = &self.devices[c];
                let child_weight = states[&child.seqno()].weight as f64;
                let child_slots = (slots as f64 * child_weight / parent_weight).ceil();
                self.assign_capacities(child_slots as usize, child, states);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SlotKey {
    bucket_no: BucketNo,
    segment_no: u16,
    member_no: u8,
}

// デバイスの状態(e.g., 割当状況)
//
// NOTE: デバイス群は、木を形成している、ということが前提
// FIXME: これを満たすためのバリデーションを別の箇所に入れる
struct DeviceState<'a> {
    weight: u64,

    // 割当済みのスロット数
    allocated: usize,

    // 割当スロット数の期待値
    // FIXME: `capacity`という用語は不適切なので変更する
    capacity: usize,

    ring: HashRing,

    device: &'a Device,
}
