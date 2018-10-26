use byteorder::{BigEndian, ByteOrder};
use frugalos_raft::{LocalNodeId, NodeId};
use libfrugalos::entity::server::Server;
use std::net::SocketAddr;

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
