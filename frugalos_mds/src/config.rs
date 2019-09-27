//! mds の設定を定義しているcrate。

use std::ops::Range;
use std::time::Duration;

/// `frugalos_mds` の設定。
///
/// 以下の理由で現時点では module 毎に設定を struct で分けていない。
///
/// - ほとんどの設定が `Node` のためのものであること。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosMdsConfig {
    /// コミットをタイムアウトさせるかどうかを決める閾値。
    ///
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_commit_timeout_threshold")]
    pub commit_timeout_threshold: usize,

    /// proposal キューが長すぎる(リーダーが重い)と判断する基準となる閾値。
    ///
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_large_proposal_queue_threshold")]
    pub large_proposal_queue_threshold: usize,

    /// リーダー選出待ちキューが長すぎると判断する基準となる閾値。
    #[serde(default = "default_large_leader_waiting_queue_threshold")]
    pub large_leader_waiting_queue_threshold: usize,

    /// リーダー選出待ちをあきらめるまでの閾値。
    ///
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_leader_waiting_timeout_threshold")]
    pub leader_waiting_timeout_threshold: usize,

    /// node がポーリングする間隔。
    #[serde(
        rename = "node_polling_interval_millis",
        default = "default_node_polling_interval",
        with = "frugalos_core::serde_ext::duration_millis"
    )]
    pub node_polling_interval: Duration,

    /// リーダが重い場合に再選出を行うかどうかを決める閾値。
    ///
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_reelection_threshold")]
    pub reelection_threshold: usize,

    /// スナップショットを取る際の閾値の下限(この値を含む).
    #[serde(default = "default_snapshot_threshold_min")]
    pub snapshot_threshold_min: usize,

    /// スナップショットを取る際の閾値の上限(この値を含む).
    #[serde(default = "default_snapshot_threshold_max")]
    pub snapshot_threshold_max: usize,

    /// リーダー不在状況でオブジェクトが古くなりすぎているか否かを決める閾値の上限(この値を含む).
    ///
    /// この設定値の1単位は `node_polling_interval` である点に注意。
    #[serde(default = "default_staled_object_threshold")]
    pub staled_object_threshold: usize,
}

impl FrugalosMdsConfig {
    /// スナップショットを取る際の閾値を返す(両端の値を含む).
    ///
    /// `Node` のローカルログの長さが、この値を超えた場合に、スナップショットの取得が開始される.
    pub fn snapshot_threshold(&self) -> Range<usize> {
        Range {
            start: self.snapshot_threshold_min,
            end: self.snapshot_threshold_max,
        }
    }
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
            snapshot_threshold_min: default_snapshot_threshold_min(),
            snapshot_threshold_max: default_snapshot_threshold_max(),
            staled_object_threshold: default_staled_object_threshold(),
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

fn default_snapshot_threshold_min() -> usize {
    9_500
}

fn default_snapshot_threshold_max() -> usize {
    10_500
}

fn default_staled_object_threshold() -> usize {
    50
}
