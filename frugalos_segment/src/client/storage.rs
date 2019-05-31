#![allow(clippy::needless_pass_by_value)]
use adler32;
use byteorder::{BigEndian, ByteOrder};
use cannyls::deadline::Deadline;
use cannyls::lump::LumpData;
use cannyls_rpc::Client as CannyLsClient;
use cannyls_rpc::DeviceId;
use ecpool::liberasurecode::LibErasureCoderBuilder;
use ecpool::ErasureCoderPool;
use fibers::time::timer;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use frugalos_core::tracer::SpanExt;
use frugalos_raft::NodeId;
use futures::future;
use futures::{self, Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::span::{Span, SpanHandle};
use slog::Logger;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use config::{
    CannyLsClientConfig, ClientConfig, ClusterConfig, ClusterMember, DispersedClientConfig,
    DispersedConfig, Participants, ReplicatedClientConfig, ReplicatedConfig,
};
use metrics::{DispersedClientMetrics, PutAllMetrics, ReplicatedClientMetrics};
use util::Phase;
use {Error, ErrorKind, ObjectValue, Result};

type BoxFuture<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

#[derive(Clone)]
pub enum StorageClient {
    Metadata,
    Replicated(ReplicatedClient),
    Dispersed(DispersedClient),
}
impl StorageClient {
    pub fn new(
        logger: Logger,
        config: ClientConfig,
        rpc_service: RpcServiceHandle,
        ec: Option<ErasureCoder>,
    ) -> Result<Self> {
        use config::Storage;
        match config.storage {
            Storage::Metadata => Ok(StorageClient::Metadata),
            Storage::Replicated(c) => {
                let metrics = track!(ReplicatedClientMetrics::new())?;
                Ok(StorageClient::Replicated(ReplicatedClient::new(
                    metrics,
                    config.cluster,
                    c,
                    config.replicated_client,
                    rpc_service,
                )))
            }
            Storage::Dispersed(c) => {
                let metrics = track!(DispersedClientMetrics::new())?;
                Ok(StorageClient::Dispersed(DispersedClient::new(
                    logger,
                    metrics,
                    config.cluster,
                    c,
                    config.dispersed_client,
                    rpc_service,
                    ec,
                )))
            }
        }
    }
    pub fn is_metadata(&self) -> bool {
        if let StorageClient::Metadata = *self {
            true
        } else {
            false
        }
    }
    pub fn get_fragment(self, local_node: NodeId, version: ObjectVersion) -> GetFragment {
        match self {
            StorageClient::Metadata => GetFragment::Failed(futures::failed(
                ErrorKind::Other.cause("unreachable").into(),
            )),
            StorageClient::Replicated(c) => {
                GetFragment::Replicated(c.get_fragment(local_node, version))
            }
            StorageClient::Dispersed(c) => {
                GetFragment::Dispersed(c.get_fragment(local_node, version))
            }
        }
    }
    pub fn get(
        self,
        object: ObjectValue,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<Vec<u8>> {
        match self {
            StorageClient::Metadata => Box::new(futures::finished(object.content)),
            StorageClient::Replicated(c) => c.get(object.version, deadline),
            StorageClient::Dispersed(c) => c.get(object.version, deadline, parent),
        }
    }
    pub fn put(
        self,
        version: ObjectVersion,
        content: Vec<u8>,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<()> {
        match self {
            StorageClient::Metadata => Box::new(futures::finished(())),
            StorageClient::Replicated(c) => c.put(version, content, deadline),
            StorageClient::Dispersed(c) => c.put(version, content, deadline, parent),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicatedClient {
    metrics: ReplicatedClientMetrics,
    cluster: Arc<ClusterConfig>,
    config: ReplicatedConfig,
    client_config: ReplicatedClientConfig,
    rpc_service: RpcServiceHandle,
}
impl ReplicatedClient {
    pub fn new(
        metrics: ReplicatedClientMetrics,
        cluster: ClusterConfig,
        config: ReplicatedConfig,
        client_config: ReplicatedClientConfig,
        rpc_service: RpcServiceHandle,
    ) -> Self {
        ReplicatedClient {
            metrics,
            cluster: Arc::new(cluster),
            config,
            client_config,
            rpc_service,
        }
    }
    pub fn get_fragment(
        self,
        _local_node: NodeId,
        version: ObjectVersion,
    ) -> GetReplicatedFragment {
        // TODO: `_local_node`は問い合わせ候補から外す(必ず失敗するので)
        let future = self.get(version, Deadline::Infinity);
        GetReplicatedFragment(future)
    }
    pub fn get(self, version: ObjectVersion, deadline: Deadline) -> BoxFuture<Vec<u8>> {
        let replica = self.config.tolerable_faults as usize + 1;
        let mut candidates = self
            .cluster
            .candidates(version)
            .take(replica)
            .cloned()
            .collect::<Vec<_>>();
        candidates.reverse();
        let future = ReplicatedGet {
            version,
            deadline,
            cannyls_config: self.client_config.cannyls.clone(),
            candidates,
            rpc_service: self.rpc_service,
            future: Box::new(futures::finished(None)),
        };
        Box::new(future)
    }
    pub fn put(
        self,
        version: ObjectVersion,
        mut content: Vec<u8>,
        deadline: Deadline,
    ) -> BoxFuture<()> {
        let rpc_service = self.rpc_service;
        let replica = self.config.tolerable_faults as usize + 1;
        append_checksum(&mut content);

        let data = match track!(LumpData::new(content)) {
            Ok(data) => data,
            Err(error) => return Box::new(futures::failed(Error::from(error))),
        };
        let cannyls_config = self.client_config.cannyls.clone();

        let futures = self
            .cluster
            .candidates(version)
            .take(replica)
            .map(move |m| {
                let client = CannyLsClient::new(m.node.addr, rpc_service.clone());
                let mut request = client.request();
                request.rpc_options(cannyls_config.rpc_options());

                let device_id = DeviceId::new(m.device.clone());
                let lump_id = m.make_lump_id(version);
                let future: BoxFuture<_> = Box::new(
                    request
                        .deadline(deadline)
                        .max_queue_len(cannyls_config.device_max_queue_len)
                        .put_lump(device_id, lump_id, data.clone())
                        .map(|_is_new| ())
                        .map_err(|e| track!(Error::from(e))),
                );
                future
            });
        let put_all = match track!(PutAll::new(self.metrics.put_all.clone(), futures, 1)) {
            Ok(put_all) => put_all,
            Err(error) => return Box::new(futures::failed(error)),
        };
        Box::new(put_all)
    }
}

pub struct ReplicatedGet {
    version: ObjectVersion,
    deadline: Deadline,
    cannyls_config: CannyLsClientConfig,
    candidates: Vec<ClusterMember>,
    future: BoxFuture<Option<Vec<u8>>>,
    rpc_service: RpcServiceHandle,
}
impl Future for ReplicatedGet {
    type Item = Vec<u8>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.future.poll() {
                Err(e) => {
                    if self.candidates.is_empty() {
                        return Err(track!(e));
                    }
                    self.future = Box::new(futures::finished(None))
                }
                Ok(Async::Ready(None)) => {
                    let m = track!(self
                        .candidates
                        .pop()
                        .ok_or_else(|| ErrorKind::Corrupted.error(),))?;
                    let client = CannyLsClient::new(m.node.addr, self.rpc_service.clone());
                    let mut request = client.request();
                    request.rpc_options(self.cannyls_config.rpc_options());

                    let lump_id = m.make_lump_id(self.version);
                    let future = request
                        .deadline(self.deadline)
                        .get_lump(DeviceId::new(m.device), lump_id);
                    self.future = Box::new(future.map_err(|e| track!(Error::from(e))));
                }
                Ok(Async::Ready(Some(mut content))) => {
                    if let Err(e) = track!(verify_and_remove_checksum(&mut content)) {
                        if self.candidates.is_empty() {
                            return Err(track!(e));
                        }
                        self.future = Box::new(futures::finished(None));
                    } else {
                        return Ok(Async::Ready(content));
                    }
                }
                Ok(Async::NotReady) => break,
            }
        }
        Ok(Async::NotReady)
    }
}

pub struct PutAll {
    metrics: PutAllMetrics,
    future: future::SelectAll<BoxFuture<()>>,
    ok_count: usize,
    required_ok_count: usize,
}
impl PutAll {
    pub fn new<I>(metrics: PutAllMetrics, futures: I, required_ok_count: usize) -> Result<Self>
    where
        I: Iterator<Item = BoxFuture<()>>,
    {
        let (_, upper_bound) = futures.size_hint();
        let len = track!(upper_bound.ok_or_else(
            || ErrorKind::Invalid.cause("The upper bound of the given futures is unknown.")
        ))?;
        if len < required_ok_count {
            let e = ErrorKind::Invalid.cause(format!("The length of the given futures is too short:  required_ok_count={}, futures.len={}", required_ok_count, len));
            return Err(track!(Error::from(e)));
        }
        let future = future::select_all(futures);
        Ok(PutAll {
            metrics,
            future,
            ok_count: 0,
            required_ok_count,
        })
    }
}
impl Future for PutAll {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let remainings = match self.future.poll() {
                Err((e, _, remainings)) => {
                    self.metrics.lost_fragments_total.increment();
                    if remainings.len() + self.ok_count < self.required_ok_count {
                        self.metrics.failures_total.increment();
                        return Err(track!(e));
                    }
                    remainings
                }
                Ok(Async::Ready(((), _, remainings))) => {
                    self.ok_count += 1;
                    if self.ok_count >= self.required_ok_count {
                        // TODO: パラメータ化
                        return Ok(Async::Ready(()));
                    }
                    remainings
                }
                Ok(Async::NotReady) => break,
            };
            if remainings.is_empty() {
                return Ok(Async::Ready(()));
            }
            self.future = future::select_all(remainings);
        }
        Ok(Async::NotReady)
    }
}

/// ErasureCodingのエンコーダ・デコーダの型。
pub type ErasureCoder = ErasureCoderPool<LibErasureCoderBuilder>;

/// `ErasureCoder`を構築するための補助関数。
pub fn build_ec(data_fragments: usize, parity_fragments: usize) -> ErasureCoder {
    let data_fragments = NonZeroUsize::new(data_fragments).expect("TODO: handle error");
    let parity_fragments = NonZeroUsize::new(parity_fragments).expect("TODO: hanlde error");
    let builder = LibErasureCoderBuilder::new(data_fragments, parity_fragments);
    ErasureCoderPool::new(builder)
}

#[derive(Clone)]
pub struct DispersedClient {
    logger: Logger,
    metrics: DispersedClientMetrics,
    cluster: Arc<ClusterConfig>,
    config: DispersedConfig,
    client_config: DispersedClientConfig,
    data_fragments: usize,
    ec: ErasureCoder,
    rpc_service: RpcServiceHandle,
}
impl DispersedClient {
    pub fn new(
        logger: Logger,
        metrics: DispersedClientMetrics,
        cluster: ClusterConfig,
        config: DispersedConfig,
        client_config: DispersedClientConfig,
        rpc_service: RpcServiceHandle,
        ec: Option<ErasureCoder>,
    ) -> Self {
        let parity_fragments = config.tolerable_faults as usize;
        let data_fragments = config.fragments as usize - parity_fragments;
        let ec = ec.unwrap_or_else(|| build_ec(data_fragments, parity_fragments));
        DispersedClient {
            logger,
            metrics,
            cluster: Arc::new(cluster),
            config,
            client_config,
            ec,
            data_fragments,
            rpc_service,
        }
    }
    pub fn get_fragment(self, local_node: NodeId, version: ObjectVersion) -> GetDispersedFragment {
        let candidates = self
            .cluster
            .candidates(version)
            .cloned()
            .collect::<Vec<_>>();
        let participants = Participants::dispersed(&candidates, self.config.fragments());
        let missing_index = participants.fragment_index(&local_node);
        let mut spares = participants.spares(&local_node);
        spares.reverse();

        // let spares = self.cluster
        //     .members
        //     .iter()
        //     .filter(|m| m.node != local_node)
        //     .cloned()
        //     .collect::<Vec<_>>();
        debug!(
            self.logger,
            "get_fragment: version={:?}, missing_index={:?}, spares={:?}",
            version,
            missing_index,
            spares
        );

        // rand::thread_rng().shuffle(&mut spares);
        let dummy: BoxFuture<_> = Box::new(futures::finished(None));
        let future = CollectFragments {
            logger: self.logger.clone(),
            futures: vec![dummy],
            fragments: Vec::new(),
            data_fragments: self.data_fragments,
            spares,
            version,
            deadline: Deadline::Infinity,
            cannyls_config: self.client_config.cannyls.clone(),
            rpc_service: self.rpc_service,
            parent: Span::inactive().handle(), // TODO
            timeout: None,
            next_timeout_duration: self.client_config.get_timeout,
        };
        GetDispersedFragment {
            phase: Phase::A(future),
            ec: self.ec.clone(),
            missing_index,
        }
    }
    pub fn get(
        self,
        version: ObjectVersion,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<Vec<u8>> {
        let mut spares = self
            .cluster
            .candidates(version)
            .cloned()
            .collect::<Vec<_>>();
        spares.reverse();
        // rand::thread_rng().shuffle(&mut spares);
        let dummy: BoxFuture<_> = Box::new(futures::finished(None));

        let span = parent.child("get_content", |span| {
            span.tag(StdTag::component(module_path!()))
                .tag(Tag::new("object.version", version.0 as i64))
                .tag(Tag::new("storage.type", "dispersed"))
                .start()
        });
        let future = CollectFragments {
            logger: self.logger.clone(),
            futures: vec![dummy],
            fragments: Vec::new(),
            data_fragments: self.data_fragments,
            spares,
            version,
            deadline,
            cannyls_config: self.client_config.cannyls.clone(),
            rpc_service: self.rpc_service,
            parent: span.handle(),
            timeout: Some(timer::timeout(self.client_config.get_timeout)),
            next_timeout_duration: self.client_config.get_timeout,
        };
        Box::new(DispersedGet {
            phase: Phase::A(future),
            ec: self.ec.clone(),
            span,
        })
    }
    pub fn put(
        self,
        version: ObjectVersion,
        content: Vec<u8>,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> BoxFuture<()> {
        let span = parent.child("put_content", |span| {
            span.tag(StdTag::component(module_path!()))
                .tag(Tag::new("object.version", version.0 as i64))
                .tag(Tag::new("storage.type", "dispersed"))
                .start()
        });

        let mut child = span.child("ec_encode", |span| {
            span.tag(StdTag::component(module_path!())).start()
        });
        let future = self
            .ec
            .encode(content)
            .map_err(|e| track!(Error::from(e)))
            .then(move |result| {
                if let Err(ref e) = result {
                    child.set_tag(StdTag::error);
                    child.log(|log| {
                        log.error().message(e.to_string());
                    });
                }
                result
            });
        Box::new(DispersedPut {
            // NOTE: 他のメトリクスを追加するタイミングで `DispersedPut` 用の metrics に変更する
            metrics: self.metrics.put_all,
            cluster: self.cluster.clone(),
            version,
            deadline,
            cannyls_config: self.client_config.cannyls.clone(),
            data_fragments: self.data_fragments,
            rpc_service: self.rpc_service,
            phase: Phase::A(Box::new(future)),
            parent: span,
        })
    }
}

pub struct DispersedPut {
    metrics: PutAllMetrics,
    cluster: Arc<ClusterConfig>,
    version: ObjectVersion,
    deadline: Deadline,
    cannyls_config: CannyLsClientConfig,
    data_fragments: usize,
    rpc_service: RpcServiceHandle,
    phase: Phase<BoxFuture<Vec<Vec<u8>>>, PutAll>,
    parent: Span,
}
impl Future for DispersedPut {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(Error::from))? {
            let next = match phase {
                Phase::A(fragments) => {
                    let parent = self.parent.handle();
                    let version = self.version;
                    let deadline = self.deadline;
                    let cannyls_config = self.cannyls_config.clone();
                    let rpc_service = self.rpc_service.clone();
                    let futures = self
                        .cluster
                        .candidates(self.version)
                        .zip(fragments.into_iter())
                        .map(move |(m, mut content)| {
                            append_checksum(&mut content);
                            let client = CannyLsClient::new(m.node.addr, rpc_service.clone());
                            let mut request = client.request();
                            request.rpc_options(cannyls_config.rpc_options());

                            let device_id = m.device.clone();
                            let lump_id = m.make_lump_id(version);
                            let data = match track!(LumpData::new(content)) {
                                Ok(data) => data,
                                Err(error) => {
                                    let future: BoxFuture<_> =
                                        Box::new(futures::failed(Error::from(error)));
                                    return future;
                                }
                            };

                            let mut span = parent.child("put_fragment", |span| {
                                span.tag(StdTag::component(module_path!()))
                                    .tag(StdTag::span_kind("client"))
                                    .tag(StdTag::peer_ip(m.node.addr.ip()))
                                    .tag(StdTag::peer_port(m.node.addr.port()))
                                    .tag(Tag::new("node", m.node.local_id.to_string()))
                                    .tag(Tag::new("device.id", device_id.clone()))
                                    .tag(Tag::new("lump.id", lump_id.to_string()))
                                    .tag(Tag::new("lump.bytes", data.as_bytes().len() as i64))
                                    .start()
                            });
                            let future: BoxFuture<_> = Box::new(
                                request
                                    .deadline(deadline)
                                    .max_queue_len(cannyls_config.device_max_queue_len)
                                    .put_lump(DeviceId::new(device_id), lump_id, data)
                                    .map(|_is_new| ())
                                    .map_err(|e| track!(Error::from(e)))
                                    .then(move |result| {
                                        if let Err(ref e) = result {
                                            span.log_error(e);
                                        }
                                        result
                                    }),
                            );
                            future
                        });
                    Phase::B(track!(PutAll::new(
                        self.metrics.clone(),
                        futures,
                        self.data_fragments
                    ))?)
                }
                Phase::B(()) => {
                    return Ok(Async::Ready(()));
                }
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

pub struct DispersedGet {
    phase: Phase<CollectFragments, BoxFuture<Vec<u8>>>,
    ec: ErasureCoderPool<LibErasureCoderBuilder>,
    span: Span,
}
impl Future for DispersedGet {
    type Item = Vec<u8>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(Error::from))? {
            let next = match phase {
                Phase::A(fragments) => {
                    let mut child = self.span.child("ec_decode", |span| {
                        span.tag(StdTag::component(module_path!()))
                            .tag(Tag::new(
                                "fragments.bytes",
                                fragments.iter().map(Vec::len).sum::<usize>() as i64,
                            ))
                            .start()
                    });
                    let future: BoxFuture<_> = Box::new(
                        self.ec
                            .decode(fragments)
                            .map_err(|e| track!(Error::from(e)))
                            .then(move |result| {
                                if let Err(ref e) = result {
                                    child.set_tag(StdTag::error);
                                    child.log(|log| {
                                        log.error().message(e.to_string());
                                    });
                                }
                                result
                            }),
                    );
                    Phase::B(future)
                }
                Phase::B(content) => return Ok(Async::Ready(content)),
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

pub struct CollectFragments {
    logger: Logger,
    futures: Vec<BoxFuture<Option<Vec<u8>>>>,
    fragments: Vec<Vec<u8>>,
    data_fragments: usize,
    spares: Vec<ClusterMember>,
    version: ObjectVersion,
    deadline: Deadline,
    cannyls_config: CannyLsClientConfig,
    rpc_service: RpcServiceHandle,
    parent: SpanHandle,

    // フラグメント群の取得のタイムアウト時間
    //
    // 基本的にはディスクI/O負荷を抑えるために、
    // 必要最小限のフラグメントのみを取得しようと試みるが、
    // タイムアウト時間に達した場合には、
    // 極度に遅延しているディスクがあるものと判断して、
    // 取得候補を増やすことになる。
    //
    // TODO: リクエスト数が多い場合にはRPCレベルでの遅延防止処理がある旨を記述する
    //
    // TODO: `deadline`を考慮した値を使うようにする
    timeout: Option<timer::Timeout>,

    /// How long to wait before aborting the next get operation.
    next_timeout_duration: Duration,
}
impl CollectFragments {
    fn fill_shortage_from_spare(&mut self, mut force: bool) -> Result<()> {
        while force || self.futures.len() + self.fragments.len() < self.data_fragments {
            force = false;

            let m = track!(self
                           .spares
                           .pop()
                           .ok_or_else(|| {
                               let cause = format!(
                                   "There are no enough fragments (Detail: futures.len({}) + fragments.len({}) < data_fragments({}))",
                                   self.futures.len(),
                                   self.fragments.len(),
                                   self.data_fragments
                               );
                               Error::from(ErrorKind::Corrupted.cause(cause))
                           }))?;

            let client = CannyLsClient::new(m.node.addr, self.rpc_service.clone());
            let lump_id = m.make_lump_id(self.version);
            debug!(
                self.logger,
                "[CollectFragments({},{},{}/{})] candidate={:?}, lump_id={:?}",
                self.spares.len(),
                self.futures.len(),
                self.fragments.len(),
                self.data_fragments,
                m.node,
                lump_id
            );
            let mut span = self.parent.child("collect_fragment", |span| {
                span.tag(StdTag::component(module_path!()))
                    .tag(StdTag::span_kind("client"))
                    .tag(StdTag::peer_ip(m.node.addr.ip()))
                    .tag(StdTag::peer_port(m.node.addr.port()))
                    .tag(Tag::new("device", m.device.clone()))
                    .tag(Tag::new("lump", format!("{:?}", lump_id)))
                    .start()
            });

            let mut request = client.request();
            request.rpc_options(self.cannyls_config.rpc_options());

            let future = request
                .deadline(self.deadline)
                .get_lump(DeviceId::new(m.device), lump_id)
                .then(move |result| {
                    if let Err(ref e) = result {
                        span.log_error(e);
                    }
                    result
                });
            let future: BoxFuture<_> = Box::new(future.map_err(|e| track!(Error::from(e))));
            self.futures.push(future);
        }
        Ok(())
    }
}
impl Future for CollectFragments {
    type Item = Vec<Vec<u8>>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let mut i = 0;
            while i < self.futures.len() {
                match track!(self.futures[i].poll()) {
                    Err(e) => {
                        self.futures.swap_remove(i);
                        debug!(self.logger, "[CollectFragments] Error: {}", e);
                        track!(self.fill_shortage_from_spare(false), "Last error: {}", e)?;
                    }
                    Ok(Async::NotReady) => {
                        i += 1;
                    }
                    Ok(Async::Ready(fragment)) => {
                        self.futures.swap_remove(i);
                        if let Some(mut fragment) = fragment {
                            if let Err(e) = track!(verify_and_remove_checksum(&mut fragment)) {
                                // TODO: Add protection for log overflow
                                warn!(self.logger, "[CollectFragments] Corrupted fragment: {}", e);
                                track!(self.fill_shortage_from_spare(false))?;
                            } else {
                                self.fragments.push(fragment);
                            }
                        } else {
                            debug!(self.logger, "[CollectFragments] NotFound");
                            track!(self.fill_shortage_from_spare(false))?;
                        }
                    }
                }
            }
            if self.fragments.len() == self.data_fragments {
                return Ok(Async::Ready(mem::replace(&mut self.fragments, Vec::new())));
            }
            if let Ok(Async::Ready(Some(()))) = self.timeout.poll() {
                // TODO: ログは出さなくする(かわりにprometheusを使う)
                info!(
                    self.logger,
                    "Collecting fragments timeout expired: add new candidate. next_timeout={:?}",
                    self.next_timeout_duration
                );
                self.timeout = None;
                if !self.spares.is_empty() {
                    if let Err(e) = track!(self.fill_shortage_from_spare(true)) {
                        warn!(self.logger, "{}", e);
                    } else {
                        self.timeout = Some(timer::timeout(self.next_timeout_duration));
                        continue;
                    }
                }
            }
            break;
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug, PartialEq, Eq)]
/// This enum represents the result of `GetFragment`.
pub enum MaybeFragment {
    /// Successfully get a content.
    Fragment(Vec<u8>),

    /// It's not responsible for storing a fragment.
    NotParticipant,
}

#[allow(clippy::large_enum_variant)]
pub enum GetFragment {
    Failed(future::Failed<Vec<u8>, Error>),
    Replicated(GetReplicatedFragment),
    Dispersed(GetDispersedFragment),
}
impl Future for GetFragment {
    type Item = MaybeFragment;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            GetFragment::Failed(ref mut f) => {
                track!(f.poll().map(|content| content.map(MaybeFragment::Fragment)))
            }
            GetFragment::Replicated(ref mut f) => {
                track!(f.poll().map(|content| content.map(MaybeFragment::Fragment)))
            }
            GetFragment::Dispersed(ref mut f) => track!(f.poll()),
        }
    }
}

pub struct GetReplicatedFragment(BoxFuture<Vec<u8>>);
impl Future for GetReplicatedFragment {
    type Item = Vec<u8>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        track!(self.0.poll())
    }
}

/// Reconstructs original data from dispersed fragments even if
/// a focusing node loses its data fragment.
pub struct GetDispersedFragment {
    /// The processing order of futures
    phase: Phase<CollectFragments, BoxFuture<Vec<u8>>>,

    /// A thread pool of encoders(by erasure code)
    ec: ErasureCoderPool<LibErasureCoderBuilder>,

    /// The index of a focusing node.
    /// None represents that there is no missing index.
    missing_index: Option<usize>,
}
impl Future for GetDispersedFragment {
    type Item = MaybeFragment;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.missing_index.is_none() {
            return Ok(Async::Ready(MaybeFragment::NotParticipant));
        }

        let missing_index = self.missing_index.expect("never fails");

        while let Async::Ready(phase) = track!(self.phase.poll().map_err(Error::from))? {
            let next = match phase {
                Phase::A(fragments) => {
                    let future = self.ec.reconstruct(missing_index, fragments);
                    let future: BoxFuture<_> = Box::new(future.map_err(|e| track!(Error::from(e))));
                    Phase::B(future)
                }
                Phase::B(fragment) => return Ok(Async::Ready(MaybeFragment::Fragment(fragment))),
            };
            self.phase = next;
        }
        Ok(Async::NotReady)
    }
}

pub(crate) fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = adler32::adler32(&bytes[..]).expect("Never fails");
    let mut trailer = [0; 5]; // TODO: フォーマットを文書化
    BigEndian::write_u32(&mut trailer[..], checksum);
    bytes.extend_from_slice(&trailer[..]);
}

fn verify_and_remove_checksum(bytes: &mut Vec<u8>) -> Result<()> {
    track_assert!(bytes.len() >= 5, ErrorKind::Invalid);
    let split_pos = bytes.len() - 5;

    let checksum = adler32::adler32(&bytes[..split_pos]).expect("Never fails");
    let expected = BigEndian::read_u32(&bytes[split_pos..]);
    track_assert_eq!(checksum, expected, ErrorKind::Invalid);

    bytes.truncate(split_pos);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    fn candidate_position(
        config: &ClusterConfig,
        member: ClusterMember,
        version: ObjectVersion,
    ) -> usize {
        config
            .candidates(version)
            .position(|candidate| *candidate == member)
            .unwrap()
    }

    #[test]
    fn put_all_new_works() -> TestResult {
        let metrics = track!(PutAllMetrics::new("test_client"))?;
        let futures: Vec<BoxFuture<_>> = vec![];
        assert!(PutAll::new(metrics.clone(), futures.into_iter(), 2).is_err());

        let futures: Vec<BoxFuture<_>> = vec![Box::new(futures::future::ok(()))];
        assert!(PutAll::new(metrics.clone(), futures.into_iter(), 2).is_err());

        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::ok(())),
            Box::new(futures::future::ok(())),
        ];
        let put = track!(PutAll::new(metrics.clone(), futures.into_iter(), 2))?;
        assert!(wait(put).is_ok());

        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::ok(())),
            Box::new(futures::future::ok(())),
            Box::new(futures::future::ok(())),
        ];
        let put = track!(PutAll::new(metrics.clone(), futures.into_iter(), 2))?;
        assert!(wait(put).is_ok());

        Ok(())
    }

    #[test]
    fn put_all_fails_correctly() -> TestResult {
        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::ok(())),
            Box::new(futures::future::err(ErrorKind::Other.into())),
        ];
        let metrics = track!(PutAllMetrics::new("test_client"))?;
        let put = track!(PutAll::new(metrics, futures.into_iter(), 2))?;
        assert!(wait(put).is_err());
        Ok(())
    }

    #[test]
    fn put_all_fails_even_if_last_operation_succeeds() -> TestResult {
        let futures: Vec<BoxFuture<_>> = vec![
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::err(ErrorKind::Other.into())),
            Box::new(futures::future::ok(())),
        ];
        let metrics = track!(PutAllMetrics::new("test_client"))?;
        let put = track!(PutAll::new(metrics, futures.into_iter(), 2))?;
        assert!(wait(put).is_err());
        Ok(())
    }

    #[test]
    fn it_puts_data_correctly() -> TestResult {
        let data_fragments = 4;
        let parity_fragments = 1;
        let cluster_size = 5;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (_members, client) = setup_system(&mut system, cluster_size)?;
        let storage_client = client.storage;
        let version = ObjectVersion(1);
        let expected = vec![0x03];

        wait(storage_client.clone().put(
            version,
            expected.clone(),
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;
        let actual = wait(storage_client.clone().get(
            ObjectValue {
                version,
                content: expected.clone(),
            },
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn get_fragment_works() -> TestResult {
        // fragments = 5 (data_fragments = 4, parity_fragments = 1)
        let data_fragments = 4;
        let parity_fragments = 1;
        let cluster_size = 6;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (members, client) = setup_system(&mut system, cluster_size)?;
        let storage_client = client.storage;
        let (node_id, device_id, _) = members[0].clone();
        let version = ObjectVersion(4);
        let expected = vec![0x02];

        // This assersion means that
        //  the node `node_id` is a member of participants that put a data to a dispersed device.
        assert!(
            candidate_position(
                system.cluster_config(),
                ClusterMember {
                    node: node_id,
                    device: device_id.clone()
                },
                version
            ) < system.fragments() as usize
        );

        wait(storage_client.clone().put(
            version,
            expected.clone(),
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;

        let result = wait(storage_client.clone().get_fragment(node_id, version))?;

        if let MaybeFragment::Fragment(content) = result {
            assert!(!content.is_empty());
            return Ok(());
        }

        Err(ErrorKind::Other
            .cause("Cannot get a fragment".to_owned())
            .into())
    }

    #[test]
    fn get_fragment_returns_not_participant() -> TestResult {
        // fragments = 5 (data_fragments = 4, parity_fragments = 1)
        let data_fragments = 4;
        let parity_fragments = 1;
        let cluster_size = 6;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (members, client) = setup_system(&mut system, cluster_size)?;
        let storage_client = client.storage;
        let (node_id, device_id, _) = members[0].clone();

        let version = ObjectVersion(6);
        let expected = vec![0x02];

        // This assersion means that
        //  the node `node_id` is not a member of participants that put a data to a dispersed device
        //  with high probability.
        assert!(
            candidate_position(
                system.cluster_config(),
                ClusterMember {
                    node: node_id,
                    device: device_id.clone()
                },
                version
            ) >= system.fragments() as usize
        );

        wait(storage_client.clone().put(
            version,
            expected.clone(),
            Deadline::Infinity,
            Span::inactive().handle(),
        ))?;

        let result = wait(storage_client.clone().get_fragment(node_id, version))?;

        assert_eq!(result, MaybeFragment::NotParticipant);

        Ok(())
    }
}
