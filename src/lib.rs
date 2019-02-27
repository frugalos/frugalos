//! Frugal Object Storage.
#![warn(missing_docs)]
#![allow(clippy::new_ret_no_self)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate cannyls;
extern crate cannyls_rpc;
extern crate fibers;
extern crate fibers_http_server;
extern crate fibers_rpc;
extern crate fibers_tasque;
extern crate frugalos_config;
extern crate frugalos_mds;
extern crate frugalos_raft;
extern crate frugalos_segment;
extern crate futures;
extern crate httpcodec;
extern crate jemalloc_ctl;
extern crate libfrugalos;
extern crate num_cpus;
extern crate prometrics;
extern crate raftlog;
extern crate rustracing;
extern crate rustracing_jaeger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate siphasher;
extern crate url;
#[macro_use]
extern crate slog;
#[cfg(test)]
extern crate tempdir;
#[macro_use]
extern crate trackable;

use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

macro_rules! dump {
    ($($e:expr),*) => {
        format!(concat!($(stringify!($e), "={:?}; "),*), $($e),*)
    }
}

pub use error::{Error, ErrorKind};

pub mod daemon;

mod bucket;
mod client;
mod codec;
mod config_server;
mod error;
mod http;
mod rpc_server;
mod server;
mod service;

/// クレート固有の`Result`型。
pub type Result<T> = ::std::result::Result<T, Error>;

/// ファイルに書き出した時のフォーマットを調整する。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct FrugalosConfigWrapper {
    #[serde(rename = "frugalos")]
    config: FrugalosConfig,
}

/// frugalos の設定を表す struct。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosConfig {
    /// データ用ディレクトリのパス。
    pub data_dir: String,
    /// ログをファイルに出力する場合の出力先ファイルパス。
    pub log_file: Option<PathBuf>,
    /// 出力するログレベルの下限。
    pub loglevel: sloggers::types::Severity,
    /// 同時に処理できるログの最大値。
    pub max_concurrent_logs: usize,
    /// デーモン向けの設定。
    pub daemon: FrugalosDaemonConfig,
    /// HTTP server 向けの設定。
    pub http_server: FrugalosHttpServerConfig,
    /// RPC server 向けの設定。
    pub rpc_server: FrugalosRpcServerConfig,
    /// frugalos_segment 向けの設定。
    pub segment: frugalos_segment::FrugalosSegmentConfig,
}

impl FrugalosConfig {
    /// Reads `FrugalosConfig` from a YAML file.
    pub fn from_yaml<P: AsRef<Path>>(path: P) -> Result<FrugalosConfig> {
        let file = File::open(path.as_ref()).map_err(|e| track!(Error::from(e)))?;
        serde_yaml::from_reader::<File, FrugalosConfigWrapper>(file)
            .map(|wrapped| wrapped.config)
            .map_err(|e| track!(Error::from(e)))
    }
}

impl Default for FrugalosConfig {
    fn default() -> Self {
        Self {
            data_dir: Default::default(),
            log_file: None,
            loglevel: sloggers::types::Severity::Info,
            max_concurrent_logs: 4096,
            daemon: Default::default(),
            http_server: Default::default(),
            rpc_server: Default::default(),
            segment: Default::default(),
        }
    }
}

/// `FrugalosDaemon` 向けの設定。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosDaemonConfig {
    /// 実行スレッド数。
    pub executor_threads: usize,
    /// Jaegerのトレースのサンプリング確率。
    pub sampling_rate: f64,
    /// frugalos 停止時に待つ時間。
    pub stop_waiting_time: Duration,
}

impl Default for FrugalosDaemonConfig {
    fn default() -> FrugalosDaemonConfig {
        Self {
            executor_threads: num_cpus::get(),
            sampling_rate: 0.001,
            stop_waiting_time: Duration::from_millis(10),
        }
    }
}

/// HTTP server 向けの設定。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosHttpServerConfig {
    /// bind するアドレス。
    pub bind_addr: SocketAddr,
}

impl Default for FrugalosHttpServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 3000)),
        }
    }
}

/// RPC server 向けの設定。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrugalosRpcServerConfig {
    /// bind するアドレス。
    pub bind_addr: SocketAddr,
    /// RPC の接続タイムアウト時間。
    pub tcp_connect_timeout: Duration,
    /// RPC の書き込みタイムアウト時間。
    pub tcp_write_timeout: Duration,
}

impl FrugalosRpcServerConfig {
    fn channel_options(&self) -> fibers_rpc::channel::ChannelOptions {
        let mut options = fibers_rpc::channel::ChannelOptions::default();
        options.tcp_connect_timeout = self.tcp_connect_timeout;
        options.tcp_write_timeout = self.tcp_write_timeout;
        options
    }
}

impl Default for FrugalosRpcServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 14278)),
            tcp_connect_timeout: Duration::from_millis(5000),
            tcp_write_timeout: Duration::from_millis(5000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libfrugalos::time::Seconds;
    use std::fs::File;
    use std::io::Write;
    use tempdir::TempDir;
    use trackable::result::TestResult;

    #[test]
    fn config_works() -> TestResult {
        let content = r##"
---
frugalos:
  data_dir: "/var/lib/frugalos"
  log_file: ~
  loglevel: critical
  max_concurrent_logs: 30
  daemon:
    executor_threads: 3
    sampling_rate: 0.1
    stop_waiting_time:
      secs: 300
      nanos: 0
  http_server:
    bind_addr: "127.0.0.1:2222"
  rpc_server:
    bind_addr: "127.0.0.1:3333"
    tcp_connect_timeout:
      secs: 8
      nanos: 0
    tcp_write_timeout:
      secs: 10
      nanos: 0
  segment:
    mds_client:
      put_content_timeout: 32"##;
        let dir = track_any_err!(TempDir::new("frugalos_test"))?;
        let filepath = dir.path().join("frugalos.yml");
        let mut file = track_any_err!(File::create(filepath.clone()))?;

        track_any_err!(file.write(content.as_bytes()))?;

        let actual = track!(FrugalosConfig::from_yaml(filepath))?;
        let mut expected = FrugalosConfig::default();
        expected.data_dir = "/var/lib/frugalos".to_owned();
        expected.max_concurrent_logs = 30;
        expected.loglevel = sloggers::types::Severity::Critical;
        expected.daemon.sampling_rate = 0.1;
        expected.daemon.executor_threads = 3;
        expected.daemon.stop_waiting_time = Duration::from_secs(300);
        expected.http_server.bind_addr = SocketAddr::from(([127, 0, 0, 1], 2222));
        expected.rpc_server.bind_addr = SocketAddr::from(([127, 0, 0, 1], 3333));
        expected.rpc_server.tcp_connect_timeout = Duration::from_secs(8);
        expected.rpc_server.tcp_write_timeout = Duration::from_secs(10);
        expected.segment.mds_client.put_content_timeout = Seconds(32);

        assert_eq!(expected, actual);

        Ok(())
    }
}
