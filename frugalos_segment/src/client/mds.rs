//! A client implementation for MDS.
//!
//! # Overview
//!
//! MDS へのリクエストを発行するためのクライアント実装を提供する。
//!
//! 柔軟に挙動をカスタマイズ可能にするために、リクエストを実行する際に以下の要素を
//! 組み合わせられるようにしたい。
//!
//! 1. リクエストをタイムアウトさせる際の挙動(タイムアウト時間の設定 etc)
//! 2. リクエストを投げる対象のノードの選び方(単一ノード/複数ノード etc)させる挙動
//! 3. 2 で実行される各リクエストの内容
//!
//! # Behaviour
//!
//! ## タイムアウト
//!
//! リクエストのタイムアウトの挙動には以下の種類が存在する:
//!
//! 1. タイムアウトしない(conservative)
//! 2. 投機的なリクエストにより、一定時間経過でタイムアウトさせる(speculative)
//!
//! 投機的リクエストにより一時的に利用不可能な Raft ノードが存在する状況でも可用性を
//! 維持できる。なぜなら、一定時間内に応答がなかった場合に別のノードへ同じリクエスト
//! を投げ直す実装になっているため。投機的リクエストを選択した場合、1 つの操作要求に
//! おいて、各ノードは最低 1 回はリクエストを受けるため、N 台のノードのうち 1 台でも
//! 正常に動作している場合はリクエストが正常に処理される。
//!
//! 投機的リクエストは exponential backoff のアルゴリズムにより、なるべくリクエストを
//! 失敗させないように配慮されている。
//!
//! ## リクエストの対象ノード選択
//!
//! リクエストを投げる対象ノードは以下の選ばれ方がある:
//!
//! 1. (リーダーが不明の場合は)クラスタ内のノードからランダムに選択する
//! 2. (リーダーが不明の場合は)クラスタ内のノードを順に過不足なく1度ずつ選択する
//!
//! 1, 2 どちらでもリーダーが判明している場合はリーダーを選ぶ。
//!
//! 1 は初期の実装であり、乱択することでリクエスト量が均一にならされるメリットがある。
//! 一方で、理屈上同一のノードを連続して選択し続け、ノードを変更すれば失敗しないケース
//! でもエラーを返してしまう可能性がある。その確率は、各ノードを `1 / N` の確率で選択する、
//! 真にランダムな乱数生成器が存在すると仮定して、`(1 / N)^m (m = リトライ回数)`となる。
//!
//! 他方、2 は発生しうるエラーを減らすが、リーダーが不在時に単一ノードにリクエストが
//! 集中しやすいデメリットがある。現状の実装では、2 を選ぶと自動的にタイムアウトの項の 2 が
//! 合わせて選択される。
//!
//!
//! # Role(for developers)
//!
//! ## `struct Request`
//!
//! `Request` はクライアントからの 1 つの操作要求において成功/失敗が決定するまでの
//! lifetime に対応している。リトライ処理が組込まれているため、実際に MDS へ発行
//! されるリクエスト回数と `Request` が生成される回数は一致しないことがある。
//!
//! 責務として、MDS への各リクエストで必要になる状態を管理する。状態には例えば、
//! リクエストのリトライ可能回数や実行中の MDS に対するリクエストなどがある。
//!
//! ## `trait RequestOnce`
//!
//! `Request` が実際に MDS へリクエストを発行する際に `Future` を生成するために利用
//! される。`Request` とは違い、それ自身がリクエストの状態を管理するためのものではない。

use cannyls::deadline::Deadline;
use fibers::time::timer;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use frugalos_core::tracer::SpanExt;
use frugalos_mds::{Error as MdsError, ErrorKind as MdsErrorKind};
use frugalos_raft::{LocalNodeId, NodeId};
use futures::future::Either;
use futures::{Async, Future, Poll};
use libfrugalos::client::mds::Client as RaftMdsClient;
use libfrugalos::consistency::ReadConsistency;
use libfrugalos::entity::node::RemoteNodeId;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, Metadata, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use rand::{self, thread_rng, Rng};
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::span::{Span, SpanHandle};
use slog::Logger;
use std::collections::hash_set::HashSet;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use trackable::error::ErrorKindExt;

use config::{ClusterConfig, MdsClientConfig, MdsRequestPolicy};
use metrics::MdsClientMetrics;
use {Error, ErrorKind, ObjectValue, Result};

// TODO HEAD/GET 以外の参照系リクエストで `ReadConsistency` をサポートする
#[derive(Debug, Clone)]
pub struct MdsClient {
    logger: Logger,
    rpc_service: RpcServiceHandle,
    inner: Arc<Mutex<Inner>>,
    client_config: MdsClientConfig,
    metrics: MdsClientMetrics,
}
impl MdsClient {
    pub fn new(
        logger: Logger,
        rpc_service: RpcServiceHandle,
        cluster_config: ClusterConfig,
        client_config: MdsClientConfig,
        metrics: MdsClientMetrics,
    ) -> Self {
        // TODO: 以下のassertionは復活させたい
        // assert!(!config.members.is_empty());
        MdsClient {
            logger,
            rpc_service,
            inner: Arc::new(Mutex::new(Inner::new(cluster_config))),
            client_config,
            metrics,
        }
    }

    pub fn latest(&self) -> impl Future<Item = Option<ObjectSummary>, Error = Error> {
        let parent = Span::inactive().handle();
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(client.latest_version().map_err(MdsError::from))
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn list(&self) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        debug!(self.logger, "Starts LIST");
        let parent = Span::inactive().handle();
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            // TODO: supports consistency levels
            Box::new(
                client
                    .list_objects(ReadConsistency::Consistent)
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn list_by_prefix(
        &self,
        prefix: ObjectPrefix,
        parent: SpanHandle,
    ) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        debug!(self.logger, "Starts LIST BY PREFIX");
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(
                client
                    .list_objects_by_prefix(prefix.clone())
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn get(
        &self,
        id: ObjectId,
        consistency: ReadConsistency,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectValue>, Error = Error> {
        debug!(self.logger, "Starts GET: id={:?}", id);
        let member_size = self.member_size();
        if let Err(e) = validate_consistency(consistency.clone(), member_size) {
            return Either::A(futures::future::err(track!(e)));
        }
        let concurrency = match consistency {
            ReadConsistency::Quorum => Some(self.majority_size()),
            ReadConsistency::Subset(n) => Some(n),
            _ => None,
        };
        let request = if let Some(concurrency) = concurrency {
            RequestOnce2::Parallel(ParallelRequestOnce::new(
                RequestKind::Get,
                concurrency,
                move |clients| {
                    let futures: Vec<_> = clients
                        .into_iter()
                        .map(|(client, mut span)| {
                            let future: BoxFuture<_> = Box::new(
                                client
                                    .get_object(id.clone(), Expect::Any, consistency.clone())
                                    .map(to_object_value)
                                    .map_err(move |e| {
                                        span.log_error(&e);
                                        track!(MdsError::from(e))
                                    }),
                            );
                            future
                        })
                        .collect();
                    Box::new(GetLatestObject::new(futures))
                },
            ))
        } else {
            RequestOnce2::Single(SingleRequestOnce::new(RequestKind::Get, move |client| {
                let future = client
                    .get_object(id.clone(), Expect::Any, consistency.clone())
                    .map(to_object_value)
                    .map_err(MdsError::from);
                Box::new(future)
            }))
        };
        Either::B(Request::new(self.clone(), parent, request))
    }

    pub fn head(
        &self,
        id: ObjectId,
        consistency: ReadConsistency,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        debug!(self.logger, "Starts HEAD: id={:?}", id);
        let member_size = self.member_size();
        if let Err(e) = validate_consistency(consistency.clone(), member_size) {
            return Either::A(futures::future::err(track!(e)));
        }
        let concurrency = match consistency {
            ReadConsistency::Quorum => Some(self.majority_size()),
            ReadConsistency::Subset(n) => Some(n),
            _ => None,
        };
        let request = if let Some(concurrency) = concurrency {
            RequestOnce2::Parallel(ParallelRequestOnce::new(
                RequestKind::Head,
                concurrency,
                move |clients| {
                    let futures: Vec<_> = clients
                        .into_iter()
                        .map(|(client, mut span)| {
                            let future: BoxFuture<_> = Box::new(
                                client
                                    .head_object(id.clone(), Expect::Any, consistency.clone())
                                    .map_err(move |e| {
                                        span.log_error(&e);
                                        track!(MdsError::from(e))
                                    }),
                            );
                            future
                        })
                        .collect();
                    Box::new(GetLatestObject::new(futures))
                },
            ))
        } else {
            RequestOnce2::Single(SingleRequestOnce::new(RequestKind::Head, move |client| {
                Box::new(
                    client
                        .head_object(id.clone(), Expect::Any, consistency.clone())
                        .map_err(MdsError::from),
                )
            }))
        };
        Either::B(Request::new(self.clone(), parent, request))
    }

    pub fn delete(
        &self,
        id: ObjectId,
        expect: Expect,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        debug!(self.logger, "Starts DELETE: id={:?}", id);
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_object(id.clone(), expect.clone())
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn delete_by_version(
        &self,
        version: ObjectVersion,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        debug!(self.logger, "Starts DELETE: version={:?}", version);
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_object_by_version(version)
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn delete_by_range(
        &self,
        targets: Range<ObjectVersion>,
        parent: SpanHandle,
    ) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        debug!(
            self.logger,
            "Starts DELETE: versions if {:?} <= it < {:?}", targets.start, targets.end
        );
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_by_range(targets.clone())
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn delete_by_prefix(
        &self,
        prefix: ObjectPrefix,
        parent: SpanHandle,
    ) -> impl Future<Item = DeleteObjectsByPrefixSummary, Error = Error> {
        debug!(self.logger, "Starts DELETE: prefix={:?}", prefix);
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_by_prefix(prefix.clone())
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    pub fn put(
        &self,
        id: ObjectId,
        content: Vec<u8>,
        expect: Expect,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> impl Future<Item = (ObjectVersion, bool), Error = Error> {
        debug!(self.logger, "Starts PUT: id={:?}", id);
        let put_content_timeout = Seconds(if let Deadline::Within(d) = deadline {
            d.as_secs() + self.client_config.put_content_timeout.0
        } else {
            self.client_config.put_content_timeout.0
        });
        let request = SingleRequestOnce::new(RequestKind::Other, move |client| {
            Box::new(
                client
                    .put_object(
                        id.clone(),
                        content.clone(),
                        expect.clone(),
                        put_content_timeout.into(),
                    )
                    .map(|(leader, (version, old))| (leader, (version, old.is_none())))
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    /// セグメント内に保持されているオブジェクトの数を返す.
    pub fn object_count(&self) -> impl Future<Item = u64, Error = Error> {
        let parent = Span::inactive().handle();
        let request = SingleRequestOnce::new(RequestKind::Other, |client| {
            // TODO: supports consistency levels
            Box::new(
                client
                    .object_count(ReadConsistency::Consistent)
                    .map_err(MdsError::from),
            )
        });
        Request::new(self.clone(), parent, request)
    }

    fn timeout(&self, kind: RequestKind) -> RequestTimeout {
        match self.request_policy(&kind) {
            // for backward compatibility
            MdsRequestPolicy::Conservative => RequestTimeout::Never,
            MdsRequestPolicy::Speculative { timeout, .. } => RequestTimeout::Speculative {
                timer: timer::timeout(*timeout),
            },
        }
    }
    fn request_policy(&self, kind: &RequestKind) -> &MdsRequestPolicy {
        match kind {
            RequestKind::Get => &self.client_config.get_request_policy,
            RequestKind::Head => &self.client_config.head_request_policy,
            RequestKind::Other => &self.client_config.default_request_policy,
        }
    }
    fn majority_size(&self) -> usize {
        ((self.member_size() as f64 / 2.0).ceil()) as usize
    }
    fn member_size(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| panic!("{}", e))
            .config
            .members
            .len()
    }
    fn clear_leader(&self) {
        self.inner.lock().unwrap_or_else(|e| panic!("{}", e)).leader = None;
    }
    fn set_leader(&self, leader: LocalNodeId) {
        // TODO: debugレベルにする
        info!(self.logger, "Set leader: {:?}", leader);

        let mut inner = self.inner.lock().unwrap_or_else(|e| panic!("{}", e));
        let leader = inner
            .config
            .members
            .iter()
            .map(|m| m.node)
            .find(|node| node.local_id == leader)
            .expect("Never fails");
        inner.leader = Some(leader);
    }
    fn next_peer(&self, policy: &MdsRequestPolicy, candidate: usize) -> NodeId {
        // NOTE: ピアの選択とタイムアウトの方法は直交しているので、組み合わせられるようにするか検討する。
        match policy {
            MdsRequestPolicy::Conservative => self.leader(),
            MdsRequestPolicy::Speculative { .. } => self.leader_or_candidate(candidate),
        }
    }
    /// 次にリクエストを投げる対象となる MDS ノード群を返す。
    ///
    /// 最新のデータを取得できる確率を上げるためリーダーが判明している時は常にリーダーを候補に追加する。
    /// 対象ノードの集合を呼び出し毎に変更しないと、ノード障害が発生している場合にリトライが常に失敗して
    /// しまうため、呼び出し側はリトライする毎に `i` によって指定される、ノード選択の開始インデックスを
    /// 増加(ないしは減少)させていくのが望ましい。
    fn next_peers(&self, mut i: usize, required_peers: usize) -> Result<HashSet<NodeId>> {
        let inner = track!(self
            .inner
            .lock()
            .map_err(|e| Error::from(ErrorKind::Other.cause(format!("{}", e)))))?;
        let member_total = inner.config.members.len();
        track_assert!(
            required_peers <= member_total,
            ErrorKind::Invalid,
            "This cluster has {} members but {} peers are required",
            member_total,
            required_peers
        );
        let mut peers = HashSet::new();
        if let Some(leader) = inner.leader {
            peers.insert(leader);
        }
        // NOTE: 上記 `track_assert!` によって無限ループしないことが保証されている
        while peers.len() < required_peers {
            peers.insert(inner.config.members[i % member_total].node);
            i += 1;
        }
        Ok(peers)
    }
    fn leader(&self) -> NodeId {
        let mut inner = self.inner.lock().unwrap_or_else(|e| panic!("{}", e));
        if inner.leader.is_none() {
            inner.leader = rand::thread_rng()
                .choose(&inner.config.members)
                .map(|m| m.node);
        }
        inner.leader.unwrap_or_else(|| unreachable!())
    }
    fn leader_or_candidate(&self, member: usize) -> NodeId {
        let inner = self.inner.lock().unwrap_or_else(|e| panic!("{}", e));
        if inner.leader.is_none() {
            return inner
                .config
                .members
                .get(member % inner.config.members.len())
                .map(|m| m.node)
                .unwrap_or_else(|| unreachable!());
        }
        inner.leader.unwrap_or_else(|| unreachable!())
    }
}

type BoxFuture<V> =
    Box<dyn Future<Item = (Option<RemoteNodeId>, V), Error = MdsError> + Send + 'static>;

fn to_object_value(
    response: (Option<RemoteNodeId>, Option<Metadata>),
) -> (Option<RemoteNodeId>, Option<ObjectValue>) {
    (
        response.0,
        response.1.map(|metadata| ObjectValue {
            version: metadata.version,
            content: metadata.data,
        }),
    )
}

fn validate_consistency(consistency: ReadConsistency, member_size: usize) -> Result<()> {
    if member_size == 0 {
        return track!(Err(ErrorKind::Invalid
            .cause("The size of cluster member must be bigger than 0")
            .into()));
    }
    match consistency {
        ReadConsistency::Subset(n) => {
            if n == 0 || member_size < n {
                track!(Err(ErrorKind::Invalid
                    .cause(format!("subset must be 0 < n <= {}", member_size))
                    .into()))
            } else {
                Ok(())
            }
        }
        ReadConsistency::Quorum | ReadConsistency::Stale | ReadConsistency::Consistent => Ok(()),
    }
}

fn make_request_span(parent: &SpanHandle, peer: &NodeId) -> Span {
    parent.child("mds_request", |span| {
        span.tag(StdTag::component(module_path!()))
            .tag(StdTag::span_kind("client"))
            .tag(StdTag::peer_ip(peer.addr.ip()))
            .tag(StdTag::peer_port(peer.addr.port()))
            .tag(Tag::new("peer.node", peer.local_id.to_string()))
            .start()
    })
}

#[derive(Debug)]
pub struct Inner {
    config: ClusterConfig,
    leader: Option<NodeId>,
}
impl Inner {
    pub fn new(config: ClusterConfig) -> Self {
        // TODO: 以下のassertionは復活させたい
        // assert!(!config.members.is_empty());
        Inner {
            config,
            leader: None,
        }
    }
}

/// Types of a request for MDS.
#[derive(Clone, Copy)]
pub enum RequestKind {
    Head,
    Get,
    Other,
}

/// Timeout method for requests.
pub enum RequestTimeout {
    /// Never times out.
    Never,

    /// The request for MDS times out at the specified time.
    ///
    /// The timeout time for each request increases to exponential according to the value specified
    /// in the configuration and the number of failures.
    /// Also, an algorithm for selecting a leader candidate when the leader is indeterminate becomes
    /// round robin.
    Speculative { timer: timer::Timeout },
}

impl Future for RequestTimeout {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            RequestTimeout::Never => Ok(Async::NotReady),
            RequestTimeout::Speculative { timer } => track!(timer.poll().map_err(Error::from)),
        }
    }
}

// TODO: supports timeout for other request kinds
#[allow(clippy::type_complexity)]
pub struct Request<T: RequestOnce> {
    client: MdsClient,
    max_retry: usize,
    retries_total: usize,
    request: T,
    parent: SpanHandle,
    peers: Vec<NodeId>,
    timeout: RequestTimeout,
    timeout_total: usize,
    started_at: Instant,
    future: Option<BoxFuture<T::Item>>,
}
impl<T> Request<T>
where
    T: RequestOnce,
    T::Item: Send + 'static,
{
    pub fn new(client: MdsClient, parent: SpanHandle, request: T) -> Self {
        let max_retry = client.member_size();
        let timeout = client.timeout(request.kind());
        Request {
            client,
            max_retry,
            retries_total: 0,
            timeout_total: 0,
            request,
            parent,
            peers: Vec::new(),
            timeout,
            started_at: Instant::now(),
            future: None,
        }
    }
    fn request_once(&mut self) -> Result<()> {
        track_assert_ne!(self.max_retry, 0, ErrorKind::Busy);
        self.max_retry -= 1;
        let (peers, future) = track!(self.request.request_once(&self.client, &self.parent))?;
        self.peers = peers;
        self.timeout = self.client.timeout(self.request.kind());
        self.future = Some(future);
        Ok(())
    }
    fn aggregate_metrics(&mut self) {
        let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
        self.client
            .metrics
            .request_duration_seconds
            .observe(elapsed);
        self.client
            .metrics
            .request_timeout_total
            .observe(self.timeout_total as f64);
        self.client
            .metrics
            .request_retries_total
            .observe(self.retries_total as f64);
    }
}
impl<T> Future for Request<T>
where
    T: RequestOnce,
    T::Item: Send + 'static,
{
    type Item = T::Item;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // It is possible to reduce processing time by making a request time out.
        // For example, there is a node where leader election has been completed but the leader has not been updated yet.
        while let Async::Ready(()) = track!(self.timeout.poll())? {
            self.timeout_total += 1;
            warn!(
                self.client.logger,
                "Request timeout: peers={:?}, max_retry={}", self.peers, self.max_retry
            );
            self.client.clear_leader();
            if self.max_retry == 0 {
                self.client
                    .metrics
                    .request_max_retry_reached_total
                    .increment();
                self.aggregate_metrics();
                track_panic!(ErrorKind::Busy, "max retry reached: peers={:?}", self.peers);
            }
            // NOTE: `request_once` はリトライ以外にも初回のリクエストでも呼ばれるため、
            // `request_once` の外で計測する必要がある.
            self.retries_total += 1;
            track!(self.request_once())?;
        }
        match self.future.poll() {
            Err(e) => {
                debug!(
                    self.client.logger,
                    "Error: peers={:?}, reason={}", self.peers, e
                );
                if let MdsErrorKind::Unexpected(current) = *e.kind() {
                    self.aggregate_metrics();
                    return Err(
                        track!(ErrorKind::UnexpectedVersion { current }.takes_over(e)).into(),
                    );
                } else {
                    self.client.clear_leader();
                }
                if self.max_retry == 0 {
                    self.client
                        .metrics
                        .request_max_retry_reached_total
                        .increment();
                    self.aggregate_metrics();
                    return Err(
                        track!(ErrorKind::Busy.takes_over(e), "peers={:?}", self.peers).into(),
                    );
                }
                self.retries_total += 1;
                track!(self.request_once())?;
                debug!(self.client.logger, "Tries next peers: {:?}", self.peers);
                self.poll()
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(None)) => {
                track!(self.request_once())?;
                self.poll()
            }
            Ok(Async::Ready(Some((new_leader, v)))) => {
                if let Some(leader) = new_leader {
                    let (_addr, local_node_id) = leader;
                    self.client.set_leader(track!(local_node_id.parse())?);
                }
                self.aggregate_metrics();
                Ok(Async::Ready(v))
            }
        }
    }
}

/// MDS ノードに投げるリクエストを生成する。
pub trait RequestOnce {
    /// リクエストに対するレスポンスの型定義。
    type Item;

    /// リクエストの種別を返す。
    fn kind(&self) -> RequestKind;

    /// 実行したいリクエストと対象ノード群を返す。
    fn request_once(
        &mut self,
        client: &MdsClient,
        parent: &SpanHandle,
    ) -> Result<(Vec<NodeId>, BoxFuture<Self::Item>)>;
}

enum RequestOnce2<F, G> {
    Single(SingleRequestOnce<F>),
    Parallel(ParallelRequestOnce<G>),
}
impl<F, G, V> RequestOnce for RequestOnce2<F, G>
where
    F: Fn(RaftMdsClient) -> BoxFuture<V>,
    G: Fn(Vec<(RaftMdsClient, Span)>) -> BoxFuture<V>,
    V: Send + 'static,
{
    type Item = V;
    fn kind(&self) -> RequestKind {
        match self {
            RequestOnce2::Single(r) => r.kind(),
            RequestOnce2::Parallel(r) => r.kind(),
        }
    }
    fn request_once(
        &mut self,
        client: &MdsClient,
        parent: &SpanHandle,
    ) -> Result<(Vec<NodeId>, BoxFuture<Self::Item>)> {
        match self {
            RequestOnce2::Single(r) => r.request_once(client, parent),
            RequestOnce2::Parallel(r) => r.request_once(client, parent),
        }
    }
}

/// 複数の MDS に並行してリクエストを投げるための `Future` が生成できる。
///
/// - 最初に応答が返ってきたレスポンスを採用する
/// - 返ってきたレスポンスの中で最新のレスポンスを採用する
///
/// といった要求を実現するために利用される。
struct ParallelRequestOnce<F> {
    kind: RequestKind,
    required_peers: usize,
    from_peer: usize,
    f: F,
}
impl<F, V> ParallelRequestOnce<F>
where
    F: Fn(Vec<(RaftMdsClient, Span)>) -> BoxFuture<V>,
    V: Send + 'static,
{
    fn new(kind: RequestKind, required_peers: usize, f: F) -> Self {
        let from_peer = thread_rng().gen();
        Self {
            kind,
            required_peers,
            from_peer,
            f,
        }
    }
}
impl<F, V> RequestOnce for ParallelRequestOnce<F>
where
    F: Fn(Vec<(RaftMdsClient, Span)>) -> BoxFuture<V>,
    V: Send + 'static,
{
    type Item = V;
    fn kind(&self) -> RequestKind {
        self.kind
    }
    fn request_once(
        &mut self,
        client: &MdsClient,
        parent: &SpanHandle,
    ) -> Result<(Vec<NodeId>, BoxFuture<Self::Item>)> {
        self.from_peer += 1;
        let peers = track!(client.next_peers(self.from_peer, self.required_peers))?;
        let mut clients = Vec::new();
        for peer in &peers {
            let client = RaftMdsClient::new(
                (peer.addr, peer.local_id.to_string()),
                client.rpc_service.clone(),
            );
            let span = make_request_span(parent, peer);
            clients.push((client, span));
        }
        let future = (self.f)(clients);
        Ok((peers.into_iter().collect(), Box::new(future)))
    }
}

/// 単一の MDS に並行してリクエストを投げるための `Future` を生成する。
///
/// `ParallelRequestOnce` でも機能的には代用可能ではあるが、更新系のリクエストなど
/// においても常に `ParallelRequestOnce` を使うと利用側のコードが煩雑になるため
/// 単一ノード専用の実装が用意されている。
struct SingleRequestOnce<F> {
    kind: RequestKind,
    from_peer: usize,
    f: F,
}
impl<F, V> SingleRequestOnce<F>
where
    F: Fn(RaftMdsClient) -> BoxFuture<V>,
    V: Send + 'static,
{
    fn new(kind: RequestKind, f: F) -> Self {
        let from_peer = thread_rng().gen();
        Self { kind, from_peer, f }
    }
}
impl<F, V> RequestOnce for SingleRequestOnce<F>
where
    F: Fn(RaftMdsClient) -> BoxFuture<V>,
    V: Send + 'static,
{
    type Item = V;
    fn kind(&self) -> RequestKind {
        self.kind
    }
    fn request_once(
        &mut self,
        client: &MdsClient,
        parent: &SpanHandle,
    ) -> Result<(Vec<NodeId>, BoxFuture<Self::Item>)> {
        self.from_peer += 1;
        let request_policy = client.request_policy(&self.kind);
        let peer = client.next_peer(request_policy, self.from_peer);
        let mut span = make_request_span(parent, &peer);
        let client = RaftMdsClient::new(
            (peer.addr, peer.local_id.to_string()),
            client.rpc_service.clone(),
        );
        let future = (self.f)(client);
        let future = future.then(move |result| {
            if let Err(ref e) = result {
                // NOTE: NotLeaderの場合はエラーではない
                span.log_error(e);
            }
            track!(result)
        });
        Ok((vec![peer], Box::new(future)))
    }
}

/// `ObjectVersion` を取得できる型で実装するべきトレイト。
///
/// HEAD と GET で `GetLatestObject` を共用するために利用される。
trait ContainObjectVersion {
    fn object_version(&self) -> ObjectVersion;
}
impl ContainObjectVersion for ObjectVersion {
    fn object_version(&self) -> ObjectVersion {
        *self
    }
}
impl ContainObjectVersion for ObjectValue {
    fn object_version(&self) -> ObjectVersion {
        self.version
    }
}
impl<A, B: ContainObjectVersion> ContainObjectVersion for (A, B) {
    fn object_version(&self) -> ObjectVersion {
        self.1.object_version()
    }
}

#[inline]
fn select_latest<I: Iterator>(values: I) -> Option<I::Item>
where
    I::Item: ContainObjectVersion,
{
    values.max_by_key(ContainObjectVersion::object_version)
}

/// 複数ノードに同時に参照リクエストを投げ、最新の `ObjectVersion` を返してきたレスポンスを採用する。
///
/// 可用性を優先するため、最新ではないオブジェクトを返すことを許容している。
struct GetLatestObject<V> {
    /// 存在しないオブジェクトを参照した回数。
    ///
    /// 最新の情報を知らないノードと最新の情報を知っているノードで結果割れた場合に多数決で結果を
    /// 決めるために利用される。
    not_found_count: usize,

    /// 実行中の `Future`。
    futures: Vec<BoxFuture<Option<V>>>,

    /// ノードが返してきた結果のためのバッファ。
    ///
    /// `not_found_count` と `values` を比較した上で最終的な結果が決まる。
    values: Vec<(Option<RemoteNodeId>, V)>,
}
impl<V> GetLatestObject<V> {
    fn new(futures: Vec<BoxFuture<Option<V>>>) -> Self {
        Self {
            futures,
            not_found_count: 0,
            values: Vec::new(),
        }
    }
}
impl<V> Future for GetLatestObject<V>
where
    V: Debug + ContainObjectVersion,
{
    type Item = (Option<RemoteNodeId>, Option<V>);
    type Error = MdsError;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut i = 0;
        while i < self.futures.len() {
            match track!(self.futures[i].poll()) {
                Err(e) => {
                    self.futures.swap_remove(i);
                    return track!(Err(e));
                }
                Ok(Async::NotReady) => {
                    i += 1;
                }
                Ok(Async::Ready(response)) => {
                    self.futures.swap_remove(i);
                    if let (leader, Some(value)) = response {
                        self.values.push((leader, value));
                    } else {
                        self.not_found_count += 1;
                    }
                }
            }
        }
        if self.futures.is_empty() {
            let values = self.values.drain(..);
            if self.not_found_count > values.len() {
                return Ok(Async::Ready((None, None)));
            }
            if let Some((leader, value)) = select_latest(values) {
                return Ok(Async::Ready((leader, Some(value))));
            }
            let e = MdsErrorKind::Other.cause("No MDS response");
            return track!(Err(e.into()));
        }
        Ok(Async::NotReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_consistency_works() {
        assert!(validate_consistency(ReadConsistency::Consistent, 3).is_ok());
        assert!(validate_consistency(ReadConsistency::Consistent, 0).is_err());
        assert!(validate_consistency(ReadConsistency::Stale, 2).is_ok());
        assert!(validate_consistency(ReadConsistency::Stale, 0).is_err());
        assert!(validate_consistency(ReadConsistency::Quorum, 1).is_ok());
        assert!(validate_consistency(ReadConsistency::Quorum, 0).is_err());
        assert!(validate_consistency(ReadConsistency::Subset(4), 12).is_ok());
        assert!(validate_consistency(ReadConsistency::Subset(2), 2).is_ok());
        assert!(validate_consistency(ReadConsistency::Subset(2), 1).is_err());
        assert!(validate_consistency(ReadConsistency::Subset(0), 1).is_err());
    }
}
