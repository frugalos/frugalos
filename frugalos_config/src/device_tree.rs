//! デバイスで構成されるグラフ構造に関する補助関数を定義する
//!
//! - 任意のデバイスを根とする部分グラフ構造は木でなければならない
//!   - 全デバイスからなるグラフは木である必要はない

use libfrugalos::entity::device::{Device, DeviceId, DeviceKind};
use std::collections::{BTreeMap, HashSet};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};

/// 新しく追加されるデバイスの検証を行う
pub fn verify_new_device(device: &Device, devices: &BTreeMap<DeviceId, Device>) -> Result<()> {
    if let DeviceKind::Virtual = device.kind() {
        let mut new_devices = devices.clone();
        new_devices.insert(device.id().to_owned(), device.clone());
        verify_device_tree(device.id(), &new_devices)
    } else {
        Ok(())
    }
}

/// 引数デバイスを根としたデバイスツリーの検証を行う
#[allow(clippy::ptr_arg)]
pub fn verify_device_tree(
    device_id: &DeviceId,
    devices: &BTreeMap<DeviceId, Device>,
) -> Result<()> {
    let mut visit = HashSet::new();
    verify_device_tree_dfs(&mut visit, device_id, devices)
}

/// デバイスツリーの検証を行う
///
/// いずれかの条件を満たす時デバイスツリーに問題があるとみなす
/// - 木でない
/// - 存在しないデバイスを参照している
#[allow(clippy::ptr_arg)]
pub fn verify_device_tree_dfs(
    visit: &mut HashSet<DeviceId>,
    device_id: &DeviceId,
    devices: &BTreeMap<DeviceId, Device>,
) -> Result<()> {
    visit.insert(device_id.to_owned());
    let device = devices.get(device_id);
    if device.is_none() {
        let e =
            ErrorKind::InvalidInput.cause(format!("Referred device:{} does not exist.", device_id));
        return Err(Error::from(e));
    }
    let device = device.expect("Never fail");
    if let Device::Virtual(v) = device {
        for child_device_id in v.children.iter() {
            if visit.get(child_device_id).is_none() {
                verify_device_tree_dfs(visit, child_device_id, devices)?
            } else {
                let e = ErrorKind::InvalidInput
                    .cause("TODO: Illegal device construct detected.".to_owned());
                return Err(Error::from(e));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Result;
    use device_tree::{verify_device_tree, verify_new_device};
    use libfrugalos::entity::device::{
        Device, DeviceId, FileDevice, SegmentAllocationPolicy, VirtualDevice, Weight,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use test_util::build_device_tree;

    fn create_file_device(id: DeviceId) -> Device {
        Device::File(FileDevice {
            id,
            seqno: 0,
            weight: Weight::Auto,
            server: String::from("frugalos-server"),
            capacity: 0,
            filepath: PathBuf::new(),
        })
    }

    fn create_virtual_device(id: DeviceId, children: BTreeSet<DeviceId>) -> Device {
        Device::Virtual(VirtualDevice {
            id,
            seqno: 0,
            weight: Weight::Auto,
            children,
            policy: SegmentAllocationPolicy::ScatterIfPossible,
        })
    }

    fn create_device_btree_map_1(root_id: DeviceId) -> BTreeMap<DeviceId, Device> {
        // generate device tree (include illegal device)
        //
        // root
        // - sub_root
        //   - file0
        //   - file1
        //   - file2
        //   - file3 ( not regiesterd )

        let file0_id = "file0".to_owned();
        let file0 = create_file_device(file0_id.clone());
        let file1_id = "file1".to_owned();
        let file1 = create_file_device(file1_id.clone());
        let file2_id = "file2".to_owned();
        let file2 = create_file_device(file2_id.clone());

        let sub_root_id = "sub_root".to_owned();
        let mut sub_root_children = BTreeSet::new();
        sub_root_children.insert(file0_id.clone());
        sub_root_children.insert(file1_id.clone());
        sub_root_children.insert(file2_id.clone());
        sub_root_children.insert("file3".to_owned());
        let sub_root = create_virtual_device(sub_root_id.clone(), sub_root_children);

        let mut root_children = BTreeSet::new();
        root_children.insert(sub_root_id.clone());
        let root = create_virtual_device(root_id.clone(), root_children);

        let entries = vec![
            (file0, file0_id),
            (file1, file1_id),
            (file2, file2_id),
            (sub_root, sub_root_id),
            (root, root_id),
        ];
        let mut btree_map = BTreeMap::new();
        for (v, k) in entries.into_iter() {
            btree_map.insert(k, v);
        }
        btree_map
    }

    fn create_device_btree_map_2() -> BTreeMap<DeviceId, Device> {
        // generate device tree (cycle)
        //  - virtual1
        //    - virtual2
        //       - virtual0
        //          - virtual1

        let virtual0_id = "virtual0".to_owned();
        let virtual1_id = "virtual1".to_owned();
        let virtual2_id = "virtual2".to_owned();

        let mut children = BTreeSet::new();
        children.insert(virtual2_id.clone());
        let virtual1 = create_virtual_device(virtual1_id.clone(), children);

        let mut children = BTreeSet::new();
        children.insert(virtual0_id.clone());
        let virtual2 = create_virtual_device(virtual2_id.clone(), children);

        let mut children = BTreeSet::new();
        children.insert(virtual1_id.clone());
        let virtual0 = create_virtual_device(virtual0_id.clone(), children);

        let entries = vec![
            (virtual0, virtual0_id),
            (virtual1, virtual1_id),
            (virtual2, virtual2_id),
        ];
        let mut btree_map = BTreeMap::new();
        for (v, k) in entries.into_iter() {
            btree_map.insert(k, v);
        }
        btree_map
    }

    fn create_device_btree_map_3() -> (BTreeMap<DeviceId, Device>, Device) {
        // generate device tree (not tree)
        // virtual0
        //   - virtual1
        //     - file1
        //   - virtual2
        //     - file1
        let virtual0_id = "virtual0".to_owned();
        let virtual1_id = "virtual1".to_owned();
        let virtual2_id = "virtual2".to_owned();
        let file1_id = "file1".to_owned();

        let file1 = create_file_device(file1_id.clone());

        let mut children = BTreeSet::new();
        children.insert(file1_id.clone());
        let virtual1 = create_virtual_device(virtual1_id.clone(), children.clone());
        let virtual2 = create_virtual_device(virtual2_id.clone(), children);

        let mut children = BTreeSet::new();
        children.insert(virtual1_id.clone());
        children.insert(virtual2_id.clone());
        let virtual0 = create_virtual_device(virtual0_id, children);

        let entries = vec![
            (file1, file1_id),
            (virtual1, virtual1_id),
            (virtual2, virtual2_id),
        ];

        let mut btree_map = BTreeMap::new();
        for (v, k) in entries.into_iter() {
            btree_map.insert(k, v);
        }
        (btree_map, virtual0)
    }

    #[test]
    fn verify_device_works() -> Result<()> {
        let (devices, root_device_id) =
            build_device_tree(&[3, 8, 1, 2], SegmentAllocationPolicy::ScatterIfPossible);
        let result = verify_device_tree(&root_device_id, &devices);
        assert!(result.is_ok());

        // reference illegal device
        let devices = create_device_btree_map_1(root_device_id.clone());
        let result = verify_device_tree(&root_device_id, &devices);
        assert!(result.is_err());

        // cycle graph
        let devices = create_device_btree_map_2();
        let result = verify_device_tree(&"virtual1".to_owned(), &devices);
        assert!(result.is_err());

        // not tree
        let (devices, new_device) = create_device_btree_map_3();
        let result = verify_new_device(&new_device, &devices);
        assert!(result.is_err());

        Ok(())
    }
}
