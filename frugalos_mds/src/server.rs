use fibers_rpc::server::{
    HandleCall, HandleCast, NoReply, Reply, ServerBuilder as RpcServerBuilder,
};
use frugalos_core::tracer::{SpanExt, ThreadLocalTracer};
use frugalos_raft::LocalNodeId;
use futures::Future;
use libfrugalos::schema::mds as rpc;
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::span::Span;
use std::time::Instant;
use trackable::error::ErrorKindExt;

use error::to_rpc_error;
use node::NodeHandle;
use {Error, ErrorKind, Result, ServiceHandle};

macro_rules! rpc_try {
    ($expr:expr) => {
        match $expr {
            Err(e) => return Reply::done(Err(to_rpc_error(track!(e)))),
            Ok(v) => v,
        }
    };
}

macro_rules! rpc_cast_try {
    ($expr:expr) => {
        match $expr {
            Err(_) => return NoReply::done(),
            Ok(v) => v,
        }
    };
}

fn with_trace<T>(
    mut span: Span,
) -> impl FnOnce(Result<T>) -> std::result::Result<T, libfrugalos::Error> {
    |result| {
        result.map_err(move |e| {
            span.log_error(&e);
            track!(to_rpc_error(e))
        })
    }
}

#[derive(Debug, Clone)]
pub struct Server {
    service: ServiceHandle,
    tracer: ThreadLocalTracer,
}
impl Server {
    pub fn register(
        service: ServiceHandle,
        builder: &mut RpcServerBuilder,
        tracer: ThreadLocalTracer,
    ) {
        let this = Server { service, tracer };
        builder.add_cast_handler::<rpc::RecommendToLeaderRpc, _>(this.clone());
        builder.add_call_handler::<rpc::GetLeaderRpc, _>(this.clone());
        builder.add_call_handler::<rpc::ListObjectsRpc, _>(this.clone());
        builder.add_call_handler::<rpc::GetObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::HeadObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::PutObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::GetLatestVersionRpc, _>(this.clone());
        builder.add_call_handler::<rpc::GetObjectCountRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectByVersionRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectsByRangeRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectsByPrefixRpc, _>(this.clone());
    }

    fn get_node(&self, node: LocalNodeId) -> Result<NodeHandle> {
        track!(
            self.service
                .get_node(node)
                .ok_or_else(|| ErrorKind::Other.cause("No such node").into()),
            "node={:?}",
            node
        )
    }

    fn start_span(&self, operation: &'static str) -> Span {
        self.tracer.span(|t| {
            t.span(operation)
                .tag(StdTag::component(module_path!()))
                .start()
        })
    }
}
impl HandleCast<rpc::RecommendToLeaderRpc> for Server {
    fn handle_cast(&self, node_id: String) -> NoReply {
        // FIXME: log errors
        let node_id = rpc_cast_try!(node_id.parse());
        let node = rpc_cast_try!(self.get_node(node_id));
        node.start_reelection();
        NoReply::done()
    }
}
impl HandleCall<rpc::GetLeaderRpc> for Server {
    fn handle_call(&self, node_id: String) -> Reply<rpc::GetLeaderRpc> {
        let node_id = rpc_try!(node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        let mut span = self.start_span("mds_get_leader_rpc");
        span.set_tag(|| Tag::new("node_id", node_id.to_string()));
        Reply::future(
            node.get_leader(Instant::now())
                .then(with_trace(span))
                .then(Ok),
        )
    }
}
impl HandleCall<rpc::ListObjectsRpc> for Server {
    fn handle_call(&self, node_id: String) -> Reply<rpc::ListObjectsRpc> {
        let node_id = rpc_try!(node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(node.list_objects().map_err(to_rpc_error).then(Ok))
    }
}

impl HandleCall<rpc::GetLatestVersionRpc> for Server {
    fn handle_call(&self, node_id: String) -> Reply<rpc::GetLatestVersionRpc> {
        let node_id = rpc_try!(node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(node.latest_version().map_err(to_rpc_error).then(Ok))
    }
}

impl HandleCall<rpc::GetObjectCountRpc> for Server {
    fn handle_call(&self, node_id: String) -> Reply<rpc::GetObjectCountRpc> {
        let node_id = rpc_try!(node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(node.object_count().map_err(to_rpc_error).then(Ok))
    }
}

impl HandleCall<rpc::GetObjectRpc> for Server {
    fn handle_call(&self, request: rpc::ObjectRequest) -> Reply<rpc::GetObjectRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.get_object(request.object_id, request.expect, Instant::now())
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<rpc::HeadObjectRpc> for Server {
    fn handle_call(&self, request: rpc::ObjectRequest) -> Reply<rpc::HeadObjectRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.head_object(request.object_id, request.expect)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<rpc::PutObjectRpc> for Server {
    fn handle_call(&self, request: rpc::PutObjectRequest) -> Reply<rpc::PutObjectRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.put_object(
                request.object_id,
                request.metadata,
                request.expect,
                request.put_content_timeout.into(),
                Instant::now(),
            )
            .map_err(to_rpc_error)
            .then(Ok),
        )
    }
}
impl HandleCall<rpc::DeleteObjectRpc> for Server {
    fn handle_call(&self, request: rpc::ObjectRequest) -> Reply<rpc::DeleteObjectRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.delete_object(request.object_id, request.expect, Instant::now())
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
impl HandleCall<rpc::DeleteObjectByVersionRpc> for Server {
    fn handle_call(&self, request: rpc::VersionRequest) -> Reply<rpc::DeleteObjectByVersionRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.delete_version(request.object_version)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}

impl HandleCall<rpc::DeleteObjectsByRangeRpc> for Server {
    fn handle_call(&self, request: rpc::RangeRequest) -> Reply<rpc::DeleteObjectsByRangeRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.delete_by_range(request.targets)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}

impl HandleCall<rpc::DeleteObjectsByPrefixRpc> for Server {
    fn handle_call(&self, request: rpc::PrefixRequest) -> Reply<rpc::DeleteObjectsByPrefixRpc> {
        let node_id = rpc_try!(request.node_id.parse().map_err(Error::from));
        let node = rpc_try!(self.get_node(node_id));
        Reply::future(
            node.delete_by_prefix(request.prefix)
                .map_err(to_rpc_error)
                .then(Ok),
        )
    }
}
