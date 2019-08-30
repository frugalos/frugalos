use byteorder::{BigEndian, ByteOrder};
use cannyls::lump::LumpId;
use raftlog::log::LogIndex;
use raftlog::node::NodeId as RaftNodeId;
use raftlog::{Error, ErrorKind, Result};
use std::fmt;
use std::net::SocketAddr;
use std::ops::Range;
use std::str::FromStr;
use std::u32;
use trackable::error::ErrorKindExt;

const LUMP_TYPE_BALLOT: u8 = 0;
const LUMP_TYPE_LOG_ENTRY: u8 = 1;
const LUMP_TYPE_LOG_PREFIX_INDEX: u8 = 2;
const LUMP_TYPE_LOG_PREFIX: u8 = 3;

/// `Service`ローカルな7バイト長のID.
///
/// このIDは、一つの`Service`内で一意である必要がある.
///
/// また、複数ノードが同じ`DeviceHandle`を共有する場合には、
/// それぞれのIDは異なるものである必要がある
/// (これが守られない場合には、名前空間が衝突し、データが破損してしまう危険性がある).
///
/// # Examples
///
/// 16進文字列との相互変換.
///
/// ```
/// use frugalos_raft::LocalNodeId;
///
/// let id = LocalNodeId::new([0, 11, 222, 3, 44, 5, 66]);
/// assert_eq!(id.to_string(), "bde032c0542");
/// assert_eq!("bde032c0542".parse::<LocalNodeId>().unwrap(), id);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LocalNodeId(#[serde(with = "hex_node_id")] [u8; 7]);
impl LocalNodeId {
    /// 新しい`LocalNodeId`インスタンスを生成する.
    pub fn new(id: [u8; 7]) -> Self {
        LocalNodeId(id)
    }

    /// IDをスライス形式に変換して返す.
    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }

    /// `Ballot`用の`LumpId`を返す.
    ///
    /// # LumpIdのレイアウト (Erlang表記)
    ///
    /// ```erlang
    /// <<LocalNodeId:56, (Type=0):8, 0:64>>
    /// ```
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use frugalos_raft::LocalNodeId;
    ///
    /// assert_eq!(LocalNodeId::new([1; 7]).to_ballot_lump_id().as_ref(),
    ///            [1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    /// ```
    pub fn to_ballot_lump_id(self) -> LumpId {
        let mut id = [0; 16];
        (&mut id[0..7]).copy_from_slice(&self.0[..]);
        id[7] = LUMP_TYPE_BALLOT;
        LumpId::new(BigEndian::read_u128(&id[..]))
    }

    /// 指定されたインデックスのログエントリ用の`LumpId`を返す.
    ///
    /// # LumpIdのレイアウト (Erlang表記)
    ///
    /// ```erlang
    /// <<LocalNodeId:56, (Type=1):8, Index:64>>
    /// ```
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use frugalos_raft::LocalNodeId;
    ///
    /// assert_eq!(LocalNodeId::new([2; 7]).to_log_entry_lump_id(3.into()).as_ref(),
    ///            [2, 2, 2, 2, 2, 2, 2, 1, 0, 0, 0, 0, 0, 0, 0, 3]);
    /// ```
    pub fn to_log_entry_lump_id(self, index: LogIndex) -> LumpId {
        let mut id = [0; 16];
        (&mut id[0..7]).copy_from_slice(&self.0[..]);
        id[7] = LUMP_TYPE_LOG_ENTRY;
        BigEndian::write_u64(&mut id[8..], index.as_u64());
        LumpId::new(BigEndian::read_u128(&id[..]))
    }

    /// `LogPrefix`の格納場所を保持するための`LumpId`を返す.
    ///
    /// # LumpIdのレイアウト (Erlang表記)
    ///
    /// ```erlang
    /// <<LocalNodeId:56, (Type=2):8, 0:64>>
    /// ```
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use frugalos_raft::LocalNodeId;
    ///
    /// assert_eq!(LocalNodeId::new([1; 7]).to_log_prefix_index_lump_id().as_ref(),
    ///            [1, 1, 1, 1, 1, 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
    /// ```
    pub fn to_log_prefix_index_lump_id(self) -> LumpId {
        let mut id = [0; 16];
        (&mut id[0..7]).copy_from_slice(&self.0[..]);
        id[7] = LUMP_TYPE_LOG_PREFIX_INDEX;
        LumpId::new(BigEndian::read_u128(&id[..]))
    }

    /// 指定された位置に保存される`LogPrefix`用の`LumpId`を返す.
    ///
    /// # LumpIdのレイアウト (Erlang表記)
    ///
    /// ```erlang
    /// <<LocalNodeId:56, (Type=3):8, Index:64>>
    /// ```
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use frugalos_raft::LocalNodeId;
    ///
    /// assert_eq!(LocalNodeId::new([1; 7]).to_log_prefix_lump_id(4).as_ref(),
    ///            [1, 1, 1, 1, 1, 1, 1, 3, 0, 0, 0, 0, 0, 0, 0, 4]);
    /// ```
    pub fn to_log_prefix_lump_id(self, index: u64) -> LumpId {
        let mut id = [0; 16];
        (&mut id[0..7]).copy_from_slice(&self.0[..]);
        id[7] = LUMP_TYPE_LOG_PREFIX;
        BigEndian::write_u64(&mut id[8..], index);
        LumpId::new(BigEndian::read_u128(&id[..]))
    }

    /// このノードが取り得る `LumpId` の範囲を返す.
    ///
    /// # Examples
    ///
    /// ```
    /// use frugalos_raft::LocalNodeId;
    ///
    /// let mut id = [0; 7];
    /// id[6] = 1;
    /// let range = LocalNodeId::new(id).to_available_lump_id_range();
    /// assert_eq!(range.start.as_u128(), 4722366482869645213696);
    /// assert_eq!(range.end.as_u128(), 9444732965739290427392);
    /// assert_eq!(range.end.as_u128() / range.start.as_u128(), 2);
    /// ```
    pub fn to_available_lump_id_range(self) -> Range<LumpId> {
        let mut id = [0; 16];
        (&mut id[0..7]).copy_from_slice(&self.0[..]);
        let start = LumpId::new(BigEndian::read_u128(&id[..]));
        let local_id_and_type = BigEndian::read_u64(&id[0..8]);
        BigEndian::write_u64(&mut id[0..8], local_id_and_type + (0x01 << 8));
        let end = LumpId::new(BigEndian::read_u128(&id[..]));
        Range { start, end }
    }
}
impl fmt::Debug for LocalNodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LocalNodeId({:?})", hex_node_id::to_hex_string(self.0))
    }
}
impl fmt::Display for LocalNodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex_node_id::to_hex_string(self.0))
    }
}
impl FromStr for LocalNodeId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        track!(hex_node_id::from_hex_str(s).map(LocalNodeId))
    }
}

/// ノードのID.
///
/// # Examples
///
/// 文字列形式との相互変換.
///
/// 文字列形式: `{local_id:16進数}.{instance:16進数}@{addr:IPアドレス表記}`
///
/// ```
/// use frugalos_raft::NodeId;
///
/// let id = NodeId {
///     local_id: "1".parse().unwrap(),
///     instance: 2,
///     addr: "127.0.0.1:80".parse().unwrap()
/// };
/// assert_eq!(id.to_string(), "1.2@127.0.0.1:80");
/// assert_eq!("1.2@127.0.0.1:80".parse::<NodeId>().unwrap(), id);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    /// `addr`が示すマシン内でのノードのID.
    pub local_id: LocalNodeId,

    /// ノードのインスタンスを識別するためのフィールド.
    ///
    /// 例えば、ストレージデータ破損を跨いだノード再起動の際には、
    /// このフィールドの値を変えることで、Raft(i.e., `raftlog`)レベルでは、
    /// 異なるノードとして認識されるので、データ消去に伴う問題を避けることができる.
    pub instance: u32,

    /// ノードが存在するマシンを特定するためのアドレス.
    pub addr: SocketAddr,
}
impl NodeId {
    /// Raft用のノードID形式に変換する.
    pub fn to_raft_node_id(&self) -> RaftNodeId {
        RaftNodeId::new(self.to_string())
    }

    /// Raft用のノードIDを`NodeId`形式に変換する.
    pub fn from_raft_node_id(id: &RaftNodeId) -> Result<Self> {
        id.as_str().parse()
    }
}
impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{:x}@{}", self.local_id, self.instance, self.addr)
    }
}
impl FromStr for NodeId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut tokens = s.splitn(2, '.');
        let local_id = track_assert_some!(tokens.next(), ErrorKind::InvalidInput, "{:?}", s);

        let mut tokens =
            track_assert_some!(tokens.next(), ErrorKind::InvalidInput, "{:?}", s).splitn(2, '@');
        let instance = track_assert_some!(tokens.next(), ErrorKind::InvalidInput, "{:?}", s);
        let addr = track_assert_some!(tokens.next(), ErrorKind::InvalidInput, "{:?}", s);

        let local_id = track!(local_id.parse())?;
        let instance = track!(
            u32::from_str_radix(instance, 16).map_err(|e| ErrorKind::InvalidInput.cause(e))
        )?;
        let addr = track!(addr.parse().map_err(|e| ErrorKind::InvalidInput.cause(e)))?;
        Ok(NodeId {
            local_id,
            instance,
            addr,
        })
    }
}

mod hex_node_id {
    use byteorder::{BigEndian, ByteOrder};
    use raftlog::{Error, ErrorKind};
    use serde::de;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::u64;
    use trackable::error::ErrorKindExt;

    const NODE_ID_SIZE: usize = 7;

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(value: &[u8; NODE_ID_SIZE], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex = to_hex_string(*value);
        hex.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; NODE_ID_SIZE], D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        from_hex_str(&hex).map_err(de::Error::custom)
    }

    pub fn to_hex_string(id: [u8; NODE_ID_SIZE]) -> String {
        let n = BigEndian::read_uint(&id[..], 7);
        format!("{:x}", n)
    }
    pub fn from_hex_str(hex: &str) -> Result<[u8; NODE_ID_SIZE], Error> {
        let n = track!(u64::from_str_radix(hex, 16).map_err(|e| ErrorKind::InvalidInput.cause(e)))?;
        let mut id = [0; NODE_ID_SIZE];
        BigEndian::write_uint(&mut id[..], n, 7);
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_conv() {
        let n = NodeId {
            local_id: "123abc".parse().unwrap(),
            instance: 4,
            addr: "127.0.0.1:80".parse().unwrap(),
        };
        assert_eq!(n.to_string(), "123abc.4@127.0.0.1:80");
        assert_eq!("123abc.4@127.0.0.1:80".parse::<NodeId>().unwrap(), n);
    }
}
