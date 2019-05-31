//! Raftに発行されたコマンド列を処理する状態機械.
#![allow(clippy::ptr_arg)]
use libfrugalos::entity::object::{Metadata, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use patricia_tree::PatriciaMap;
use std::collections::HashMap;

use {Error, Result};

/// ノードの状態を管理するための状態機械.
#[derive(Debug, Clone)]
pub struct Machine {
    // NOTE:
    // - `PatriciaMap`は、`HashMap`よりもメモリ消費量が少ないので採用
    // - バージョンとデータで分けているのは、大半のケースでは前者のみを利用しているため
    //   二つを分けた方がメモリ消費量が抑えられると期待されるため
    id_to_version: PatriciaMap<ObjectVersion>,
    id_to_data: HashMap<ObjectId, Vec<u8>>,
}
impl Machine {
    pub fn new() -> Self {
        Machine {
            id_to_version: PatriciaMap::new(),
            id_to_data: HashMap::new(),
        }
    }
    pub fn from_snapshot(snapshot: Snapshot) -> Self {
        match snapshot {
            Snapshot::Assoc(snapshot) => {
                let mut id_to_version = PatriciaMap::new();
                let mut id_to_data = HashMap::new();
                for (id, metadata) in snapshot {
                    if !metadata.data.is_empty() {
                        id_to_data.insert(id.clone(), metadata.data);
                    }
                    id_to_version.insert(id, metadata.version);
                }
                Machine {
                    id_to_version,
                    id_to_data,
                }
            }
            Snapshot::Patricia(id_to_version) => Machine {
                id_to_version,
                id_to_data: HashMap::new(),
            },
        }
    }
    pub fn to_snapshot(&self) -> Snapshot {
        if self.id_to_data.is_empty() {
            Snapshot::Patricia(self.id_to_version.clone())
        } else {
            // 典型的にはメタデータバケツの場合にここにくる
            let assoc = self
                .id_to_version
                .iter()
                .map(|(object_id, &version)| {
                    let object_id = String::from_utf8(object_id).unwrap();
                    let data = self.get_data(&object_id);
                    (object_id.clone(), Metadata { version, data })
                })
                .collect();
            Snapshot::Assoc(assoc)
        }
    }
    pub fn len(&self) -> usize {
        self.id_to_version.len()
    }
    pub fn put(
        &mut self,
        object_id: ObjectId,
        metadata: Metadata,
        expect: &Expect,
    ) -> Result<Option<ObjectVersion>> {
        track!(self.check_version(&object_id, &expect))?;
        if metadata.data.is_empty() {
            self.id_to_data.remove(&object_id);
        } else {
            self.id_to_data.insert(object_id.clone(), metadata.data);
        }
        Ok(self.id_to_version.insert(object_id, metadata.version))
    }
    pub fn delete(
        &mut self,
        object_id: &ObjectId,
        expect: &Expect,
    ) -> Result<Option<ObjectVersion>> {
        track!(self.check_version(object_id, &expect))?;
        self.id_to_data.remove(object_id);
        Ok(self.id_to_version.remove(object_id))
    }
    pub fn delete_version(
        &mut self,
        object_version: ObjectVersion,
    ) -> Result<Option<ObjectVersion>> {
        let owner_id: Option<Vec<u8>> = self
            .id_to_version
            .iter()
            .find(|(_, version)| version.0 == object_version.0)
            .map(|(id, _)| id);

        if let Some(owner_id) = owner_id {
            let owner_id: ObjectId = track!(String::from_utf8(owner_id).map_err(Error::from))?;
            self.id_to_data.remove(&owner_id);
            return Ok(self.id_to_version.remove(&owner_id));
        } else {
            return Ok(None);
        }
    }
    pub fn delete_by_prefix(&mut self, object_prefix: &ObjectPrefix) -> Result<Vec<ObjectVersion>> {
        let mut versions = Vec::new();
        for (object_id, version) in self.id_to_version.split_by_prefix(&object_prefix.0) {
            let id = track!(String::from_utf8(object_id).map_err(Error::from))?;
            let _ = self.id_to_data.remove(&id);
            versions.push(version);
        }
        Ok(versions)
    }
    pub fn get(&self, object_id: &ObjectId, expect: &Expect) -> Result<Option<Metadata>> {
        track!(self.check_version(object_id, &expect))?;
        Ok(self.id_to_version.get(object_id).cloned().map(|version| {
            let data = self.get_data(object_id);
            Metadata { version, data }
        }))
    }
    pub fn head(&self, object_id: &ObjectId, expect: &Expect) -> Result<Option<ObjectVersion>> {
        track!(self.check_version(object_id, &expect))?;
        Ok(self.id_to_version.get(object_id).cloned())
    }
    pub fn to_summaries(&self) -> Vec<ObjectSummary> {
        self.id_to_version
            .iter()
            .map(|(id, version)| (String::from_utf8(id).unwrap(), version))
            .map(|(id, &version)| ObjectSummary { id, version })
            .collect()
    }
    pub fn latest_version(&self) -> Option<ObjectSummary> {
        self.id_to_version
            .iter()
            .max_by_key(|(_, version)| version.0)
            .map(|(id, &version)| ObjectSummary {
                id: String::from_utf8(id).expect(
                    "Stringから作ったVec<u8>を復元するので失敗しないはず",
                ),
                version,
            })
    }
    pub fn to_versions(&self) -> Vec<ObjectVersion> {
        self.id_to_version.values().cloned().collect()
    }
    fn check_version(&self, object_id: &ObjectId, expect: &Expect) -> Result<()> {
        expect
            .validate(self.id_to_version.get(object_id).cloned())
            .map_err(Error::from)
    }
    fn get_data(&self, object_id: &ObjectId) -> Vec<u8> {
        self.id_to_data
            .get(object_id)
            .cloned()
            .unwrap_or_else(Vec::new)
    }
}

#[derive(Debug, Clone)]
pub enum Command {
    Put {
        object_id: ObjectId,
        userdata: Vec<u8>,
        expect: Expect,

        // 現在時刻を起点とした秒単位の尺.
        // 絶対時刻ではので、ノード再起動時等に大幅にズレる可能性はあるが、
        // 後ろに伸びる分には実害はないので大丈夫.
        // 絶対時刻だと、複数ノード間の時計が同期していない場合に
        // 微妙な問題があるので、あえて相対時刻にしている.
        put_content_timeout: Seconds,
    },
    Delete {
        object_id: ObjectId,
        expect: Expect,
    },
    DeleteByVersion {
        object_version: ObjectVersion,
    },
    DeleteByRange {
        version_from: ObjectVersion,
        version_to: ObjectVersion,
    },
    DeleteByPrefix {
        prefix: ObjectPrefix,
    },
}

#[derive(Debug)]
pub enum Snapshot {
    Assoc(Vec<(ObjectId, Metadata)>),
    Patricia(PatriciaMap<ObjectVersion>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use trackable::result::TestResult;

    const DEFAULT_OBJECT_VERSION: ObjectVersion = ObjectVersion(2);
    const UNKNOWN_OBJECT_VERSION: ObjectVersion = ObjectVersion(99999);

    /// テスト内で扱うデータ種別
    #[derive(Copy, Clone)]
    enum MetadataKind {
        MUSIC,
        LYRIC,
    }

    fn make_object_id(id: usize, kind: MetadataKind) -> ObjectId {
        #[allow(clippy::identity_conversion)]
        match kind {
            MetadataKind::MUSIC => ObjectId::from(format!("music:metadata:{}", id)),
            MetadataKind::LYRIC => ObjectId::from(format!("lyric:metadata:{}", id)),
        }
    }

    fn make_metadata(id: usize, kind: MetadataKind) -> (ObjectId, Metadata) {
        (
            make_object_id(id, kind),
            Metadata {
                version: DEFAULT_OBJECT_VERSION,
                data: vec![0x01, 0x02],
            },
        )
    }

    /// 個数を指定してオブジェクトを登録する。
    /// id は 0 始まりで、バージョンは固定。
    fn setup_metadata(machine: &mut Machine, metadata_size: usize, kind: MetadataKind) {
        for n in 0..metadata_size {
            let (id, meta) = make_metadata(n, kind);
            machine.put(id, meta, &Expect::None).unwrap();
        }
    }

    /// バージョンを指定してオブジェクトを登録する。
    fn setup_music_metadata_by_versions(machine: &mut Machine, versions: Vec<ObjectVersion>) {
        versions.into_iter().enumerate().for_each(|(n, version)| {
            let id = make_object_id(n, MetadataKind::MUSIC);
            let meta = Metadata {
                version,
                data: vec![0x01, 0x02],
            };
            machine.put(id, meta, &Expect::None).unwrap();
        });
    }

    #[test]
    fn it_puts_object() -> TestResult {
        let mut machine = Machine::new();

        assert_eq!(machine.len(), 0);

        let (id, meta) = make_metadata(1, MetadataKind::MUSIC);

        assert!(machine.put(id, meta, &Expect::None)?.is_none());

        assert_eq!(machine.len(), 1);

        Ok(())
    }

    #[test]
    fn it_cant_put_object_with_incorrect_expect() -> TestResult {
        let mut machine = Machine::new();

        assert_eq!(machine.len(), 0);

        let (id, meta) = make_metadata(1, MetadataKind::MUSIC);

        machine.put(id.clone(), meta.clone(), &Expect::None)?;

        // すでにバージョンが1つ以上ある
        assert!(machine
            .put(id.clone(), meta.clone(), &Expect::None)
            .is_err());

        // バージョンが異なる
        assert!(machine
            .put(
                id.clone(),
                meta.clone(),
                &Expect::IfMatch(vec![UNKNOWN_OBJECT_VERSION])
            )
            .is_err());

        // バージョンが存在している
        assert!(machine
            .put(
                id.clone(),
                meta.clone(),
                &Expect::IfNoneMatch(vec![DEFAULT_OBJECT_VERSION])
            )
            .is_err());

        Ok(())
    }

    #[test]
    fn it_get_matching_version() -> TestResult {
        let mut machine = Machine::new();

        let metadata_size = 3;

        setup_metadata(&mut machine, metadata_size, MetadataKind::MUSIC);

        assert_eq!(machine.len(), metadata_size);

        assert!(machine
            .get(
                &make_object_id(0, MetadataKind::MUSIC),
                &Expect::IfMatch(vec![DEFAULT_OBJECT_VERSION])
            )?
            .is_some());

        Ok(())
    }

    #[test]
    fn it_deletes_object_by_id() -> TestResult {
        let mut machine = Machine::new();
        let metadata_size = 3;

        setup_metadata(&mut machine, metadata_size, MetadataKind::MUSIC);

        assert_eq!(machine.len(), metadata_size);

        assert!(machine
            .delete(&make_object_id(0, MetadataKind::MUSIC), &Expect::Any)?
            .is_some());

        assert_eq!(machine.len(), metadata_size - 1);

        Ok(())
    }

    #[test]
    fn it_doesnt_delete_unknown_object_by_id() -> TestResult {
        let mut machine = Machine::new();
        let metadata_size = 3;

        setup_metadata(&mut machine, metadata_size, MetadataKind::MUSIC);

        assert_eq!(machine.len(), metadata_size);

        assert!(machine
            .delete(
                &make_object_id(metadata_size + 30, MetadataKind::MUSIC),
                &Expect::Any
            )?
            .is_none());

        assert_eq!(machine.len(), metadata_size);

        Ok(())
    }

    #[test]
    fn it_fails_to_delete_object_by_id_with_incorrect_expect() -> TestResult {
        let mut machine = Machine::new();
        let metadata_size = 3;

        setup_metadata(&mut machine, metadata_size, MetadataKind::MUSIC);

        // expect が指定されていない
        assert!(machine
            .delete(
                &make_object_id(0, MetadataKind::MUSIC),
                &Expect::IfMatch(vec![])
            )
            .is_err());

        // 存在しないバージョンを指定している
        assert!(machine
            .delete(
                &make_object_id(0, MetadataKind::MUSIC),
                &Expect::IfMatch(vec![UNKNOWN_OBJECT_VERSION])
            )
            .is_err());

        // 存在するバージョンを指定している
        assert!(machine
            .delete(
                &make_object_id(0, MetadataKind::MUSIC),
                &Expect::IfNoneMatch(vec![DEFAULT_OBJECT_VERSION])
            )
            .is_err());

        Ok(())
    }

    #[test]
    fn it_deletes_matched_objects_by_version() -> TestResult {
        let mut machine = Machine::new();
        let deleted_version = ObjectVersion(1234);
        let versions = vec![DEFAULT_OBJECT_VERSION, deleted_version];

        setup_music_metadata_by_versions(&mut machine, versions.clone());

        assert_eq!(machine.len(), versions.len());

        assert!(machine.delete_version(deleted_version)?.is_some());

        // バージョンが異なるオブジェクトは削除しない
        assert_eq!(machine.len(), versions.len() - 1);

        Ok(())
    }

    #[test]
    fn it_deletes_objects_by_prefix() -> TestResult {
        let mut machine = Machine::new();
        let music_metadata_size = 3;
        let lyric_metadata_size = 1;

        setup_metadata(&mut machine, music_metadata_size, MetadataKind::MUSIC);

        let (id, meta) = make_metadata(1, MetadataKind::LYRIC);

        assert!(machine.put(id.clone(), meta, &Expect::None)?.is_none());

        assert_eq!(machine.len(), music_metadata_size + lyric_metadata_size);

        assert!(machine
            .delete_by_prefix(&ObjectPrefix("music".to_owned()))
            .is_ok());

        // MetadataKind::LYRIC は消えていない
        assert_eq!(machine.len(), lyric_metadata_size);
        assert!(machine.head(&id, &Expect::Any)?.is_some());

        Ok(())
    }

    #[test]
    fn it_doesnt_delete_non_matched_objects_by_prefix() -> TestResult {
        let mut machine = Machine::new();
        let metadata_size = 3;

        setup_metadata(&mut machine, metadata_size, MetadataKind::MUSIC);

        assert_eq!(machine.len(), metadata_size);

        assert!(machine
            .delete_by_prefix(&ObjectPrefix("metadata".to_owned()))
            .is_ok());

        // プレフィックスが一致していないので削除しない
        assert_eq!(machine.len(), metadata_size);

        Ok(())
    }
}
