//! セグメント構成に関係する構造体等。
use byteorder::{BigEndian, ByteOrder};
use cannyls::lump::LumpId;
use frugalos_raft::NodeId;
use libfrugalos::entity::object::ObjectVersion;
use raftlog::cluster::ClusterMembers;
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};

// TODO: LumpIdの名前空間の使い方に関してWikiに記載する
pub(crate) const LUMP_NAMESPACE_CONTENT: u8 = 1;

/// Raftクラスタ(i.e., セグメント)内のメンバ情報。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMember {
    /// ノードID。
    pub node: NodeId,

    /// 使用しているデバイスのID。
    pub device: String,
}
impl ClusterMember {
    pub(crate) fn make_lump_id(&self, version: ObjectVersion) -> LumpId {
        make_lump_id(&self.node, version)
    }
}

/// 対象ノードが指定のバージョン番号を有するオブジェクトを保存する際に使用する`LumpId`を返す。
pub(crate) fn make_lump_id(node: &NodeId, version: ObjectVersion) -> LumpId {
    let mut id = [0; 16];
    // NOTE:
    // `id[0]`は常に`0`になることが保証されている（TODO: もう少し根拠を詳しく).
    // `id[7]`は使用されない.
    (&mut id[0..7]).copy_from_slice(node.local_id.as_slice());
    id[0] = LUMP_NAMESPACE_CONTENT;
    BigEndian::write_u64(&mut id[8..], version.0);
    LumpId::new(BigEndian::read_u128(&id[..]))
}

// FIXME: rename
/// クライアントがセグメントにアクセスする際に使用する構成情報。
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub cluster: ClusterConfig,
    pub storage: Storage,
}
impl ClientConfig {
    /// 対象のセグメントに属しているメンバ一覧を返す。
    pub fn to_raft_cluster_members(&self) -> ClusterMembers {
        self.cluster
            .members
            .iter()
            .map(|m| m.node.to_raft_node_id())
            .collect()
    }
}

/// セグメント(Raftクラスタ)の構成情報。
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub members: Vec<ClusterMember>,
}
impl ClusterConfig {
    /// オブジェクトデータの取得先候補を優先順位が高い順に返す。
    pub fn candidates(&self, version: ObjectVersion) -> impl Iterator<Item = &ClusterMember> {
        let mut hasher = SipHasher::new();
        version.0.hash(&mut hasher);
        let i = hasher.finish() as usize % self.members.len();
        Candidates::new(&self.members, i)
    }
}

#[derive(Debug)]
struct Candidates<'a> {
    members: &'a [ClusterMember],
    current: usize,
    end: usize,
}
impl<'a> Candidates<'a> {
    fn new(members: &'a [ClusterMember], start: usize) -> Self {
        Candidates {
            members,
            current: start,
            end: start + members.len(),
        }
    }
}
impl<'a> Iterator for Candidates<'a> {
    type Item = &'a ClusterMember;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            None
        } else {
            let i = self.current % self.members.len();
            self.current += 1;
            Some(&self.members[i])
        }
    }
}

/// オブジェクトデータの保存先ストレージの構成。
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Storage {
    #[serde(rename = "metadata")]
    Metadata,

    #[serde(rename = "replicated")]
    Replicated(ReplicatedConfig),

    #[serde(rename = "dispersed")]
    Dispersed(DispersedConfig),
}
impl Storage {
    /// メタデータストレージかどうかを判定する。
    pub fn is_metadata(&self) -> bool {
        if let Storage::Metadata = *self {
            true
        } else {
            false
        }
    }
}

/// 複製による冗長化を行うストレージの構成情報。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedConfig {
    /// 障害耐性数。
    ///
    /// `tolerable_faults + 1`が複製数となる。
    pub tolerable_faults: u8,
}

/// ErasureCodingによる冗長化を行うストレージの構成情報。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispersedConfig {
    /// 障害耐性数。
    ///
    /// パリティフラグメントの数でもある。
    pub tolerable_faults: u8,

    /// データおよびパリティを合わせたフラグメントの合計数。
    pub fragments: u8,
}
