//! 各 frugalos crate に共通する機能・定義を提供する

/// LumpId の先頭 8bit における利用区分について定義
/// 詳細: https://github.com/frugalos/frugalos/wiki/Naming-Rules-of-LumpIds
pub const LUMP_ID_NAMESPACE_RAFTLOG: u8 = 0;
pub const LUMP_ID_NAMESPACE_OBJECT: u8 = 1;
