//! 障害からの復旧手順を提供する crate。
//!
//! 個別の復旧処理(raftのログ削除etc)はその他の crate 内で実装されている。
//!
//! ファイルを使ったリカバリー方式の意図については以下の URL を参照:
//! - https://github.com/frugalos/frugalos/issues/157

use slog::Logger;
use std::fs;
use std::io;
use std::path::Path;

use crate::{Error, Result};

// 未完了の設定ファイル名
// NOTE: 設定ファイルに今後設定値を記述していけるよう YAML にしておく。
const RECOVERY_FILE_NAME: &str = "recovery.yml";

// 完了済みの設定ファイル名
const COMPLETED_RECOVERY_FILE_NAME: &str = "recovery.done";

/// 起動時に必要なリカバリー要求を表す。
pub struct RecoveryRequest;

/// リカバリー処理の準備をする。
///
/// この関数呼び出しの副作用でリカバリー要求のクリアが発生するため、2回目以降の呼び出しは無視される。
/// (リカバリー処理を複数回行う必要性もない。)
///
/// 実際のリカバリー処理の成否によらずリカバリー要求がクリアされてしまうため、やり直しをしたい場合は
/// 再度ファイルの配置をする必要がある。
pub fn prepare_recovery<P: AsRef<Path>>(
    logger: &Logger,
    data_dir: P,
) -> Result<Option<RecoveryRequest>> {
    let src = data_dir.as_ref().join(RECOVERY_FILE_NAME);
    let dest = data_dir.as_ref().join(COMPLETED_RECOVERY_FILE_NAME);

    // atomic に処理したいため、io::ErrorKind::NotFound を許容し Ok 扱いにする。
    match fs::rename(&src, &dest) {
        Ok(()) => {
            info!(
                logger,
                "Rename the recovery file: src={:?}, dest={:?}", src, dest
            );
            // 現時点では要求の有無だけを表現したい、かつ、明瞭性のために型を用意したいためフィールドなしの struct を返す。
            Ok(Some(RecoveryRequest))
        }
        Err(e) => {
            if let io::ErrorKind::NotFound = e.kind() {
                info!(
                    logger,
                    "Skipped recovery process because the recovery file is not found.: src={:?}, dest={:?}", src, dest
                );
                Ok(None)
            } else {
                Err(track!(Error::from(e)))
            }
        }
    }
}
