use cannyls::deadline::Deadline;
use fibers::time::timer;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use frugalos_core::tracer::SpanExt;
use frugalos_mds::{Error as MdsError, ErrorKind as MdsErrorKind};
use frugalos_raft::{LocalNodeId, NodeId};
use futures::{Async, Future, Poll};
use libfrugalos::client::mds::Client as RaftMdsClient;
use libfrugalos::entity::node::RemoteNodeId;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use rand::{self, Rng};
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::span::{Span, SpanHandle};
use slog::Logger;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use trackable::error::ErrorKindExt;

use config::{ClusterConfig, MdsClientConfig, MdsRequestPolicy};
use {Error, ErrorKind, ObjectValue, Result};

#[derive(Debug, Clone)]
pub struct MdsClient {
    logger: Logger,
    rpc_service: RpcServiceHandle,
    inner: Arc<Mutex<Inner>>,
    client_config: MdsClientConfig,
}
impl MdsClient {
    pub fn new(
        logger: Logger,
        rpc_service: RpcServiceHandle,
        cluster_config: ClusterConfig,
        client_config: MdsClientConfig,
    ) -> Self {
        // TODO: 以下のassertionは復活させたい
        // assert!(!config.members.is_empty());
        MdsClient {
            logger,
            rpc_service,
            inner: Arc::new(Mutex::new(Inner::new(cluster_config))),
            client_config,
        }
    }

    pub fn latest(&self) -> impl Future<Item = Option<ObjectSummary>, Error = Error> {
        let parent = Span::inactive().handle();
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(client.latest_version().map_err(MdsError::from))
        })
    }

    pub fn list(&self) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        debug!(self.logger, "Starts LIST");
        let parent = Span::inactive().handle();
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(client.list_objects().map_err(MdsError::from))
        })
    }

    pub fn get(
        &self,
        id: ObjectId,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectValue>, Error = Error> {
        debug!(self.logger, "Starts GET: id={:?}", id);
        Request::new(self.clone(), parent, RequestKind::Get, move |client| {
            let future = client
                .get_object(id.clone(), Expect::Any)
                .map(|(leader, v)| {
                    (
                        leader,
                        v.map(|metadata| ObjectValue {
                            version: metadata.version,
                            content: metadata.data,
                        }),
                    )
                });
            Box::new(future.map_err(MdsError::from))
        })
    }

    pub fn head(
        &self,
        id: ObjectId,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        debug!(self.logger, "Starts HEAD: id={:?}", id);
        Request::new(self.clone(), parent, RequestKind::Head, move |client| {
            Box::new(
                client
                    .head_object(id.clone(), Expect::Any)
                    .map_err(MdsError::from),
            )
        })
    }

    pub fn delete(
        &self,
        id: ObjectId,
        expect: Expect,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        debug!(self.logger, "Starts DELETE: id={:?}", id);
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_object(id.clone(), expect.clone())
                    .map_err(MdsError::from),
            )
        })
    }

    pub fn delete_by_version(
        &self,
        version: ObjectVersion,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        debug!(self.logger, "Starts DELETE: version={:?}", version);
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_object_by_version(version)
                    .map_err(MdsError::from),
            )
        })
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
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_by_range(targets.clone())
                    .map_err(MdsError::from),
            )
        })
    }

    pub fn delete_by_prefix(
        &self,
        prefix: ObjectPrefix,
        parent: SpanHandle,
    ) -> impl Future<Item = DeleteObjectsByPrefixSummary, Error = Error> {
        debug!(self.logger, "Starts DELETE: prefix={:?}", prefix);
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(
                client
                    .delete_by_prefix(prefix.clone())
                    .map_err(MdsError::from),
            )
        })
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
        Request::new(self.clone(), parent, RequestKind::Other, move |client| {
            Box::new(
                client
                    .put_object(
                        id.clone(),
                        content.clone(),
                        expect.clone(),
                        put_content_timeout.into(),
                    )
                    .map(|(leader, (versoin, old))| (leader, (versoin, old.is_none())))
                    .map_err(MdsError::from),
            )
        })
    }

    /// セグメント内に保持されているオブジェクトの数を返す.
    pub fn object_count(&self) -> impl Future<Item = u64, Error = Error> {
        let parent = Span::inactive().handle();
        Request::new(self.clone(), parent, RequestKind::Other, |client| {
            Box::new(client.object_count().map_err(MdsError::from))
        })
    }

    fn timeout(&self, kind: RequestKind, max_retry: usize) -> RequestTimeout {
        match self.request_policy(&kind) {
            // for backward compatibility
            MdsRequestPolicy::Conservative => RequestTimeout::Never,
            MdsRequestPolicy::Speculative { timeout, .. } => {
                let factor = 2u32.pow((self.max_retry().saturating_sub(max_retry)) as u32);
                RequestTimeout::Speculative {
                    timer: timer::timeout(*timeout * factor),
                }
            }
        }
    }
    fn request_policy(&self, kind: &RequestKind) -> &MdsRequestPolicy {
        match kind {
            RequestKind::Get => &self.client_config.get_request_policy,
            RequestKind::Head => &self.client_config.head_request_policy,
            RequestKind::Other => &self.client_config.default_request_policy,
        }
    }
    fn max_retry(&self) -> usize {
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
        // NOTE: リーダー選択とタイムアウトの方法は直交しているので、組み合わせられるようにするか検討する。
        match policy {
            MdsRequestPolicy::Conservative => self.leader(),
            MdsRequestPolicy::Speculative { .. } => self.leader_or_candidate(candidate),
        }
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
                .get(member)
                .map(|m| m.node)
                .unwrap_or_else(|| unreachable!());
        }
        inner.leader.unwrap_or_else(|| unreachable!())
    }
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
pub struct Request<F, V> {
    client: MdsClient,
    kind: RequestKind,
    max_retry: usize,
    request: F,
    parent: SpanHandle,
    peer: Option<NodeId>,
    timeout: RequestTimeout,
    future:
        Option<Box<Future<Item = (Option<RemoteNodeId>, V), Error = MdsError> + Send + 'static>>,
}
impl<F, V> Request<F, V>
where
    V: Send + 'static,
    F: Fn(
        RaftMdsClient,
    ) -> Box<Future<Item = (Option<RemoteNodeId>, V), Error = MdsError> + Send + 'static>,
{
    pub fn new(client: MdsClient, parent: SpanHandle, kind: RequestKind, request: F) -> Self {
        let max_retry = client.max_retry();
        let timeout = client.timeout(kind, max_retry);
        Request {
            client,
            kind,
            max_retry,
            request,
            parent,
            peer: None,
            timeout,
            future: None,
        }
    }
    fn request_once(&mut self) -> Result<()> {
        track_assert_ne!(self.max_retry, 0, ErrorKind::Busy);
        self.max_retry -= 1;

        let request_policy = self.client.request_policy(&self.kind);
        let peer = self.client.next_peer(request_policy, self.max_retry);
        let mut span = self.parent.child("mds_request", |span| {
            span.tag(StdTag::component(module_path!()))
                .tag(StdTag::span_kind("client"))
                .tag(StdTag::peer_ip(peer.addr.ip()))
                .tag(StdTag::peer_port(peer.addr.port()))
                .tag(Tag::new("peer.node", peer.local_id.to_string()))
                .start()
        });
        let client = RaftMdsClient::new(
            (peer.addr, peer.local_id.to_string()),
            self.client.rpc_service.clone(),
        );
        let future = (self.request)(client);
        let future = future.then(move |result| {
            if let Err(ref e) = result {
                // NOTE: NotLeaderの場合はエラーではない
                span.log_error(e);
            }
            result
        });
        self.timeout = self.client.timeout(self.kind, self.max_retry);
        self.peer = Some(peer);
        self.future = Some(Box::new(future));
        Ok(())
    }
}
impl<F, V> Future for Request<F, V>
where
    F: Fn(
        RaftMdsClient,
    ) -> Box<Future<Item = (Option<RemoteNodeId>, V), Error = MdsError> + Send + 'static>,
    V: Send + 'static,
{
    type Item = V;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // It is possible to reduce processing time by making a request time out.
        // For example, there is a node where leader election has been completed but the leader has not been updated yet.
        while let Async::Ready(()) = track!(self.timeout.poll())? {
            warn!(
                self.client.logger,
                "Request timeout: node={:?}, max_retry={}", self.peer, self.max_retry
            );
            self.client.clear_leader();
            if self.max_retry == 0 {
                track_panic!(ErrorKind::Busy, "max retry reached: node={:?}", self.peer);
            }
            track!(self.request_once())?;
        }
        match self.future.poll() {
            Err(e) => {
                debug!(
                    self.client.logger,
                    "Error: node={:?}, reason={}", self.peer, e
                );
                if let MdsErrorKind::Unexpected(current) = *e.kind() {
                    return Err(
                        track!(ErrorKind::UnexpectedVersion { current }.takes_over(e)).into(),
                    );
                } else {
                    self.client.clear_leader();
                }
                if self.max_retry == 0 {
                    return Err(
                        track!(ErrorKind::Busy.takes_over(e), "node={:?}", self.peer).into(),
                    );
                }
                track!(self.request_once())?;
                debug!(self.client.logger, "Tries next node: {:?}", self.peer);
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
                Ok(Async::Ready(v))
            }
        }
    }
}
