//! frugalosプロセス(デーモン)を起動したり操作するための機能を提供するモジュール。
// FIXME: 実際にはデーモンプロセスではないので、名前は変更する
use fibers::executor::ThreadPoolExecutorHandle;
use fibers::sync::mpsc;
use fibers::sync::oneshot;
use fibers::{Executor, Spawn, ThreadPoolExecutor};
use fibers_http_server::metrics::{MetricsHandler, WithMetrics};
use fibers_http_server::{Server as HttpServer, ServerBuilder as HttpServerBuilder};
use fibers_rpc;
use fibers_rpc::channel::ChannelOptions;
use fibers_rpc::client::{ClientService as RpcService, ClientServiceBuilder as RpcServiceBuilder};
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_config;
use frugalos_raft;
use futures::{Async, Future, Poll, Stream};
use libfrugalos;
use num_cpus;
use prometrics;
use rustracing::sampler::{PassiveSampler, ProbabilisticSampler, Sampler};
use rustracing_jaeger;
use rustracing_jaeger::span::SpanContextState;
use slog::{self, Drain, Logger};
use std::mem;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use config_server::ConfigServer;
use frugalos_segment;
use rpc_server::RpcServer;
use server::{spawn_report_spans_thread, Server};
use service;
use {Error, ErrorKind, Result};

/// デーモンのビルダ。
pub struct FrugalosDaemonBuilder {
    logger: Logger,

    /// 実行スレッド数。
    pub executor_threads: usize,

    /// Jaegerのトレースのサンプリング確率。
    pub sampling_rate: f64,

    /// RPC 通信のオプション。
    pub rpc_client_channel_options: ChannelOptions,

    /// `MdsClient` に与える Configuration。
    pub mds_client_config: frugalos_segment::config::MdsClientConfig,
}
impl FrugalosDaemonBuilder {
    /// 新しい`FrugalosDaemonBuilder`インスタンスを生成する。
    pub fn new(logger: Logger) -> Self {
        FrugalosDaemonBuilder {
            logger,
            executor_threads: num_cpus::get(),
            sampling_rate: 0.001,
            rpc_client_channel_options: Default::default(),
            mds_client_config: Default::default(),
        }
    }

    /// デーモンを起動する。
    pub fn finish<P: AsRef<Path>>(
        &self,
        data_dir: P,
        http_addr: SocketAddr,
    ) -> Result<FrugalosDaemon> {
        FrugalosDaemon::new(self, data_dir, http_addr)
    }
}

/// A configuration used when frugalos starts.
pub struct FrugalosRunConfig {
    /// waiting time when frugalos stops.
    pub stop_waiting_time: Duration,
}

impl Default for FrugalosRunConfig {
    fn default() -> Self {
        Self {
            stop_waiting_time: Default::default(),
        }
    }
}

/// Frugalosの各種機能を提供するためのデーモン。
pub struct FrugalosDaemon {
    logger: Logger,
    service: service::Service<ThreadPoolExecutorHandle>,
    http_server_builder: HttpServerBuilder,
    rpc_server_builder: RpcServerBuilder,
    rpc_service: RpcService,
    executor: ThreadPoolExecutor,
    command_rx: mpsc::Receiver<DaemonCommand>,
}
impl FrugalosDaemon {
    fn new<P: AsRef<Path>>(
        builder: &FrugalosDaemonBuilder,
        data_dir: P,
        http_addr: SocketAddr,
    ) -> Result<Self> {
        let logger = Logger::root(
            slog::Duplicate::new(builder.logger.clone(), track!(LogMetrics::new())?).fuse(),
            o!(),
        );

        let server = track!(frugalos_config::cluster::load_local_server_info(&data_dir))?;

        let rpc_addr = server.addr();
        let mut http_server_builder = HttpServerBuilder::new(http_addr);
        http_server_builder.logger(logger.clone());

        let mut rpc_server_builder = RpcServerBuilder::new(rpc_addr);
        rpc_server_builder.logger(logger.clone());

        let executor = track!(
            ThreadPoolExecutor::with_thread_count(builder.executor_threads).map_err(Error::from)
        )?;
        let rpc_service = RpcServiceBuilder::new()
            .logger(logger.clone())
            .channel_options(builder.rpc_client_channel_options.clone())
            .finish(executor.handle());

        let raft_service = frugalos_raft::Service::new(logger.clone(), &mut rpc_server_builder);
        let config_service = track!(frugalos_config::Service::new(
            logger.clone(),
            data_dir,
            &mut rpc_server_builder,
            rpc_service.handle(),
            raft_service.handle(),
            executor.handle(),
        ))?;
        let service = track!(service::Service::new(
            logger.clone(),
            executor.handle(),
            raft_service,
            config_service,
            &mut rpc_server_builder,
            rpc_service.handle(),
            builder.mds_client_config.clone(),
        ))?;

        let sampler = Sampler::<SpanContextState>::or(
            PassiveSampler,
            track!(ProbabilisticSampler::new(builder.sampling_rate)
                .map_err(|e| ErrorKind::InvalidInput.takes_over(e)))?,
        );
        let (tracer, span_rx) = rustracing_jaeger::Tracer::new(sampler);
        spawn_report_spans_thread(span_rx);

        let (command_tx, command_rx) = mpsc::channel();

        let client = service.client();
        RpcServer::register(
            client.clone(),
            FrugalosDaemonHandle { command_tx },
            &mut rpc_server_builder,
        );

        let server = Server::new(logger.clone(), client, tracer);
        track!(server.register(&mut http_server_builder))?;

        track!(http_server_builder.add_handler(WithMetrics::new(MetricsHandler)))?;

        let config_server = ConfigServer::new(rpc_service.handle(), rpc_addr);
        track!(config_server.register(&mut http_server_builder))?;

        Ok(FrugalosDaemon {
            logger: logger.clone(),
            service,
            http_server_builder,
            rpc_server_builder,
            rpc_service,
            executor,
            command_rx,
        })
    }

    fn register_prometheus_metrics(&self) -> Result<()> {
        prometrics::default_registry()
            .register(prometrics::metrics::ProcessMetricsCollector::new());
        let mut version = track!(prometrics::metrics::GaugeBuilder::new("build")
            .namespace("frugalos")
            .label("version", env!("CARGO_PKG_VERSION"))
            .initial_value(1.0)
            .default_registry()
            .finish())?;
        if let Some(commit) = Command::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
        {
            if !commit.is_empty() {
                track!(version.labels_mut().insert("revision", commit.trim()))?;
            }
        }
        mem::forget(version); // FIXME

        Ok(())
    }
}
impl FrugalosDaemon {
    /// 各種サーバを起動して、処理を実行する。
    ///
    /// この呼び出しはブロッキングするので注意。
    pub fn run(mut self, config: FrugalosRunConfig) -> Result<()> {
        track!(self.register_prometheus_metrics())?;

        let runner = DaemonRunner {
            logger: self.logger.clone(),
            config,
            service: self.service,
            rpc_server: self.rpc_server_builder.finish(self.executor.handle()),
            http_server: self.http_server_builder.finish(self.executor.handle()),
            rpc_service: self.rpc_service,
            command_rx: self.command_rx,
            stop_notifications: Vec::new(),
            do_stop: false,
        };

        let monitor = self.executor.handle().spawn_monitor(runner);
        let result = track!(self.executor.run_fiber(monitor).map_err(Error::from))?;
        track!(result.map_err(Error::from))
    }
}

struct DaemonRunner {
    logger: Logger,
    config: FrugalosRunConfig,
    service: service::Service<ThreadPoolExecutorHandle>,
    http_server: HttpServer,
    rpc_server: fibers_rpc::server::Server<ThreadPoolExecutorHandle>,
    rpc_service: fibers_rpc::client::ClientService,
    command_rx: mpsc::Receiver<DaemonCommand>,
    stop_notifications: Vec<oneshot::Monitored<(), Error>>,
    do_stop: bool,
}
impl DaemonRunner {
    fn handle_command(&mut self, command: DaemonCommand) {
        match command {
            DaemonCommand::StopDaemon { reply } => {
                info!(
                    self.logger,
                    "Stops HTTP Server and waits for a while({:?})", self.config.stop_waiting_time
                );
                self.http_server.stop(self.config.stop_waiting_time);
                self.service.stop();
                self.stop_notifications.push(reply);
            }
            DaemonCommand::TakeSnapshot => {
                self.service.take_snapshot();
            }
        }
    }
}
impl Future for DaemonRunner {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if track!(self.http_server.poll())?.is_ready() && self.do_stop {
            return Ok(Async::Ready(()));
        }
        track!(self.rpc_server.poll())?;
        track!(self.rpc_service.poll())?;
        self.do_stop = self.do_stop || track!(self.service.poll())?.is_ready();
        if self.do_stop {
            for reply in self.stop_notifications.drain(..) {
                reply.exit(Ok(()));
            }
            return Ok(Async::NotReady);
        }
        while let Async::Ready(Some(command)) = self.command_rx.poll().expect("Never fails") {
            self.handle_command(command);
        }
        Ok(Async::NotReady)
    }
}

/// デーモンを操作するためのハンドル。
#[derive(Debug, Clone)]
pub struct FrugalosDaemonHandle {
    command_tx: mpsc::Sender<DaemonCommand>,
}
impl FrugalosDaemonHandle {
    /// 停止する。
    pub fn stop(&self) -> impl Future<Item = (), Error = Error> {
        let (reply_tx, reply_rx) = oneshot::monitor();
        let command = DaemonCommand::StopDaemon { reply: reply_tx };
        let _ = self.command_tx.send(command);
        StopDaemon(reply_rx)
    }

    /// スナップショット取得を依頼する。
    pub fn take_snapshot(&self) {
        let command = DaemonCommand::TakeSnapshot;
        let _ = self.command_tx.send(command);
    }
}

#[derive(Debug)]
enum DaemonCommand {
    StopDaemon {
        reply: oneshot::Monitored<(), Error>,
    },
    TakeSnapshot,
}

#[derive(Debug)]
pub(crate) struct StopDaemon(oneshot::Monitor<(), Error>);
impl Future for StopDaemon {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self
            .0
            .poll()
            .map_err(|e| e.unwrap_or_else(|| ErrorKind::Other
                .cause("Monitoring channel disconnected")
                .into())))
    }
}

struct LogMetrics {
    // FIXME
    debugs: prometrics::metrics::Counter,
    infos: prometrics::metrics::Counter,
    warnings: prometrics::metrics::Counter,
    errors: prometrics::metrics::Counter,
    criticals: prometrics::metrics::Counter,
}
impl LogMetrics {
    pub fn new() -> Result<Self> {
        fn counter(level: &str) -> Result<prometrics::metrics::Counter> {
            let counter = track!(prometrics::metrics::CounterBuilder::new("records_total")
                .namespace("log")
                .label("level", level)
                .default_registry()
                .finish())?;
            Ok(counter)
        }
        Ok(LogMetrics {
            debugs: track!(counter("debug"))?,
            infos: track!(counter("info"))?,
            warnings: track!(counter("warning"))?,
            errors: track!(counter("error"))?,
            criticals: track!(counter("critical"))?,
        })
    }
}
impl Drain for LogMetrics {
    type Ok = ();
    type Err = ();
    fn log(
        &self,
        record: &slog::Record,
        _values: &slog::OwnedKVList,
    ) -> ::std::result::Result<Self::Ok, Self::Err> {
        use slog::Level;
        match record.level() {
            Level::Trace => {}
            Level::Debug => self.debugs.increment(),
            Level::Info => self.infos.increment(),
            Level::Warning => self.warnings.increment(),
            Level::Error => self.errors.increment(),
            Level::Critical => self.criticals.increment(),
        }
        Ok(())
    }
}

/// 指定されたアドレスを使用しているfrugalosプロセスを停止する。
pub fn stop(logger: &Logger, rpc_addr: SocketAddr) -> Result<()> {
    info!(logger, "Starts stopping the frugalos server");

    let mut executor = track!(ThreadPoolExecutor::with_thread_count(1).map_err(Error::from))?;
    let rpc_service = RpcServiceBuilder::new()
        .logger(logger.clone())
        .finish(executor.handle());
    let rpc_service_handle = rpc_service.handle();
    executor.spawn(rpc_service.map_err(|e| panic!("{}", e)));

    let client = libfrugalos::client::frugalos::Client::new(rpc_addr, rpc_service_handle);
    let fiber = executor.spawn_monitor(client.stop());
    track!(executor
        .run_fiber(fiber)
        .unwrap()
        .map_err(|e| e.unwrap_or_else(|| panic!("monitoring channel disconnected"))))?;

    info!(logger, "The frugalos server has stopped");
    Ok(())
}

/// 指定されたアドレスを使用しているfrugalosプロセスでスナップショットを取得する。
pub fn take_snapshot(logger: &Logger, rpc_addr: SocketAddr) -> Result<()> {
    info!(logger, "Starts taking snapshot");

    let mut executor = track!(ThreadPoolExecutor::with_thread_count(1).map_err(Error::from))?;
    let rpc_service = RpcServiceBuilder::new()
        .logger(logger.clone())
        .finish(executor.handle());
    let rpc_service_handle = rpc_service.handle();
    executor.spawn(rpc_service.map_err(|e| panic!("{}", e)));

    let client = libfrugalos::client::frugalos::Client::new(rpc_addr, rpc_service_handle);
    let fiber = executor.spawn_monitor(client.take_snapshot());
    track!(executor
        .run_fiber(fiber)
        .unwrap()
        .map_err(|e| e.unwrap_or_else(|| panic!("monitoring channel disconnected"))))?;

    info!(logger, "The frugalos server has taken snapshot");
    Ok(())
}
