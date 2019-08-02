use libfrugalos::entity::device::{
    Device, DeviceId, MemoryDevice, SegmentAllocationPolicy, VirtualDevice, Weight,
};
use std::collections::{BTreeMap, BTreeSet};

/// Creates a tree of devices, with a given sequence of arities.
pub(crate) fn build_device_tree(
    numbers_of_children: &[u32],
    policy: SegmentAllocationPolicy,
) -> (BTreeMap<DeviceId, Device>, DeviceId) {
    let mut result = BTreeMap::new();
    let mut seqno = 0;
    let root_device_id =
        build_device_tree_dfs(numbers_of_children, policy, &mut result, &mut seqno);
    (result, root_device_id)
}

fn build_device_tree_dfs<'a>(
    numbers_of_children: &[u32],
    policy: SegmentAllocationPolicy,
    map: &'a mut BTreeMap<DeviceId, Device>,
    seqno: &mut u32,
) -> DeviceId {
    let device_id = format!("dev{}", *seqno);
    let current_seqno = *seqno;
    *seqno += 1;
    let device = if numbers_of_children.is_empty() {
        // base: creates a leaf memory device
        let memory_device = MemoryDevice {
            id: device_id.clone(),
            seqno: current_seqno,
            weight: Weight::Auto,
            server: "dummy".to_string(),
            capacity: 1 << 30, // 1 GiB
        };
        Device::Memory(memory_device)
    } else {
        // step: creates a virtual device that manages children
        let current_arity = numbers_of_children[0];
        let mut children = BTreeSet::new();
        for _ in 0..current_arity {
            let child_id =
                build_device_tree_dfs(&numbers_of_children[1..], policy.clone(), map, seqno);
            children.insert(child_id);
        }
        let virtual_device = VirtualDevice {
            id: device_id.clone(),
            seqno: current_seqno,
            weight: Weight::Auto,
            children,
            policy,
        };
        Device::Virtual(virtual_device)
    };
    map.insert(device_id.clone(), device);
    device_id
}

mod tests {
    use libfrugalos::entity::device::{Device, SegmentAllocationPolicy};
    use test_util::build_device_tree;

    #[test]
    fn build_device_tree_creates_correct_number_of_devices() {
        // Create a virtual device which has 3 children, each of which has 8 memory devices.
        let numbers_of_children = [3, 8];
        let (devices, root_id) = build_device_tree(
            &numbers_of_children,
            SegmentAllocationPolicy::ScatterIfPossible,
        );
        // (1 + 3) virtual devices and 3 * 8 memory devices
        assert_eq!(devices.len(), 1 + 3 + 3 * 8);

        // root_id has 3 children
        let root_device = &devices[&root_id];
        assert!(root_device.is_virtual());
        let virtual_device = match root_device {
            Device::Virtual(x) => x,
            _ => unreachable!(),
        };
        assert_eq!(virtual_device.children.len(), 3);
    }
}
