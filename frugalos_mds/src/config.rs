//! mds の設定を定義しているcrate.

use std::ops::Range;
use std::time::Duration;

/// `frugalos_mds` の設定.
/// 以下の理由で現時点では module 毎に設定を struct で分けていない。
/// - ほとんどの設定が `Node` のためのものであること。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosMdsConfig {
    /// コミットをタイムアウトさせるかどうかを決める閾値。
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_commit_timeout_threshold")]
    pub commit_timeout_threshold: usize,

    /// proposal キューが長すぎる(リーダーが重い)と判断する基準となる閾値。
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_large_proposal_queue_threshold")]
    pub large_proposal_queue_threshold: usize,

    /// リーダー選出待ちキューが長すぎると判断する基準となる閾値。
    #[serde(default = "default_large_leader_waiting_queue_threshold")]
    pub large_leader_waiting_queue_threshold: usize,

    /// リーダー選出待ちをあきらめるまでの閾値。
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_leader_waiting_timeout_threshold")]
    pub leader_waiting_timeout_threshold: usize,

    /// node がポーリングする間隔。
    #[serde(default = "default_node_polling_interval")]
    pub node_polling_interval: Duration,

    /// リーダが重い場合に再選出を行うかどうかを決める閾値。
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_reelection_threshold")]
    pub reelection_threshold: usize,

    /// スナップショットを取る際の閾値(レンジの両端を含む).
    ///
    /// `Node` のローカルログの長さが、この値を超えた場合に、スナップショットの取得が開始される.
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: Range<usize>,
}

impl Default for FrugalosMdsConfig {
    fn default() -> Self {
        Self {
            commit_timeout_threshold: default_commit_timeout_threshold(),
            large_proposal_queue_threshold: default_large_proposal_queue_threshold(),
            large_leader_waiting_queue_threshold: default_large_leader_waiting_queue_threshold(),
            leader_waiting_timeout_threshold: default_leader_waiting_timeout_threshold(),
            node_polling_interval: default_node_polling_interval(),
            reelection_threshold: default_reelection_threshold(),
            snapshot_threshold: default_snapshot_threshold(),
        }
    }
}

fn default_commit_timeout_threshold() -> usize {
    30
}

fn default_large_proposal_queue_threshold() -> usize {
    1024
}

fn default_large_leader_waiting_queue_threshold() -> usize {
    10000
}

fn default_leader_waiting_timeout_threshold() -> usize {
    10
}

fn default_node_polling_interval() -> Duration {
    Duration::from_millis(500)
}

fn default_reelection_threshold() -> usize {
    10
}

fn default_snapshot_threshold() -> Range<usize> {
    Range {
        start: 9_500,
        end: 10_500,
    }
}
