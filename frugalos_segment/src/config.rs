//! セグメント構成に関係する構造体等。
use byteorder::{BigEndian, ByteOrder};
use cannyls::lump::LumpId;
use frugalos_raft::NodeId;
use libfrugalos::entity::object::ObjectVersion;
use libfrugalos::time::Seconds;
use raftlog::cluster::ClusterMembers;
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};

// TODO: LumpIdの名前空間の使い方に関してWikiに記載する
pub(crate) const LUMP_NAMESPACE_CONTENT: u8 = 1;

/// Raftクラスタ(i.e., セグメント)内のメンバ情報。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

/// Configuration for `MdsClient`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MdsClientConfig {
    /// Timeout in seconds, which is used to determine an actual `Deadline` on putting a content.
    pub put_content_timeout: Seconds,
}

impl Default for MdsClientConfig {
    fn default() -> Self {
        MdsClientConfig {
            // This default value is a heuristic.
            put_content_timeout: Seconds(60),
        }
    }
}

// FIXME: rename
/// クライアントがセグメントにアクセスする際に使用する構成情報。
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub cluster: ClusterConfig,
    pub storage: Storage,
    pub mds: MdsClientConfig,
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

/// A set of `ClusterMember`s which MAY have a replica of original data.
/// Be sure to create a `Candidates` object via `ClusterConfig::candidates`.
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

/// A set of `ClusterMember`s which MUST have a replica of original data.
/// Use `Participants::dispersed` to compute spares for a dispersed configuration.
#[derive(Debug)]
pub struct Participants<'a> {
    members: &'a [ClusterMember],
}

impl<'a> Participants<'a> {
    /// Creates a new `Participants` from a set of `ClusterMember`s.
    /// This function doesn't validate the given arguments, so
    /// the caller has the responsibility for using a correct configuration.
    pub fn dispersed(members: &'a [ClusterMember], fragments: u8) -> Self {
        let (members, _) = members.split_at(fragments as usize);
        Participants { members }
    }

    /// Returns the position of the given node in this participants.
    /// Returns None if the given node is not a member of the participants.
    pub fn fragment_index(&self, node_id: &NodeId) -> Option<usize> {
        self.members.iter().position(|m| m.node == *node_id)
    }

    /// Returns spares to be replicated.
    /// The given `NodeId` is excluded from the result.
    pub fn spares(&self, local_node: &NodeId) -> Vec<ClusterMember> {
        self.members
            .iter()
            .filter(|m| m.node != *local_node)
            .cloned()
            .collect::<Vec<_>>()
    }

    /// For testing.
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.members.len()
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

impl DispersedConfig {
    /// Returns the sum of data fragments and parity fragments.
    /// Must be positive.
    pub fn fragments(&self) -> u8 {
        self.fragments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frugalos_raft::LocalNodeId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use trackable::result::TestResult;

    /// Makes a cluster member.
    /// `n` is used for the id of a node.
    fn make_member(n: u8) -> ClusterMember {
        let local_id = LocalNodeId::new([0, 0, 0, 0, 0, 0, n]);
        ClusterMember {
            node: NodeId {
                local_id,
                // an arbitrary value is ok
                instance: 0,
                // an arbitrary value is ok
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
            },
            device: n.to_string(),
        }
    }

    /// Makes a cluster which has the given size of members.
    fn make_cluster(size: u8) -> ClusterConfig {
        let mut members = Vec::new();

        for n in 0..size {
            members.push(make_member(n));
        }

        ClusterConfig { members }
    }

    /// Collects all device names from `ClusterConfig`.
    /// This function makes assertion ease in a test.
    /// The ordering of the returned `Vec<String>` is consistent
    /// with the `ClusterConfig::candidates`.
    fn collect_devices(cluster: &ClusterConfig, version: ObjectVersion) -> Vec<String> {
        cluster
            .candidates(version)
            .map(|m| m.device.clone())
            .collect::<Vec<_>>()
    }

    #[test]
    fn cluster_config_works() {
        let cluster = make_cluster(5);
        let candidates = collect_devices(&cluster, ObjectVersion(1));

        assert_eq!(candidates.len(), 5);
        assert_eq!(candidates[0], "3");
        assert_eq!(candidates[1], "4");
        assert_eq!(candidates[2], "0");
        assert_eq!(candidates[3], "1");
        assert_eq!(candidates[4], "2");
    }

    #[test]
    fn participants_works() -> TestResult {
        let cluster_size = 5;
        let fragments = 3;
        let version = ObjectVersion(1);
        let cluster = make_cluster(cluster_size);
        let candidates = cluster
            .candidates(version.clone())
            .cloned()
            .collect::<Vec<_>>();
        let participants = Participants::dispersed(&candidates, fragments);

        let matrix = vec![
            (0, "3", true),
            (1, "4", true),
            (2, "0", true),
            (3, "1", false),
            (4, "2", false),
        ];

        assert_eq!(participants.len(), fragments as usize);

        for (i, device, is_participant) in matrix {
            let member = candidates.get(i as usize).unwrap();

            assert_eq!(member.device, device);
            assert_eq!(
                participants.fragment_index(&member.node).is_some(),
                is_participant
            );
        }

        let matrix = vec![
            (0, vec!["4", "0"]),
            (1, vec!["3", "0"]),
            (2, vec!["3", "4"]),
            (3, vec!["3", "4", "0"]),
        ];

        for (i, expected_spares) in matrix {
            let node_id = candidates.get(i).unwrap().node;

            assert_eq!(
                expected_spares,
                participants
                    .spares(&node_id)
                    .iter()
                    .map(|m| m.device.clone())
                    .collect::<Vec<_>>()
            );
        }

        Ok(())
    }
}
