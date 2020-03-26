use byteorder::{BigEndian, ByteOrder};
use frugalos_raft::{LocalNodeId, NodeId};
use libfrugalos::entity::server::Server;
use std::net::SocketAddr;

/// `frugalos_config` の設定
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosConfigConfig {
    /// リーダー選出待ちキューが長すぎると判断する記述となる閾値。
    #[serde(default = "default_leader_waiters_threshold")]
    pub leader_waiters_threshold: usize,
}

impl Default for FrugalosConfigConfig {
    fn default() -> Self {
        Self {
            leader_waiters_threshold: default_leader_waiters_threshold(),
        }
    }
}

fn default_leader_waiters_threshold() -> usize {
    10000
}

pub fn server_to_frugalos_raft_node(server: &Server) -> NodeId {
    let mut id = [0; 7];

    // 通常データとIDが衝突しないように、接頭辞に`[3]`を設定しておく
    //
    // FIXME: 定数化(+ IDの命名規則をWikiに明文化する)
    id[0] = 3;
    BigEndian::write_u32(&mut id[1..5], server.seqno);
    NodeId {
        local_id: LocalNodeId::new(id),
        addr: SocketAddr::new(server.host, server.port),
        instance: 0,
    }
}
