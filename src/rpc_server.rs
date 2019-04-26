use cannyls;
use fibers_rpc::server::{HandleCall, Reply, ServerBuilder as RpcServerBuilder};
use frugalos_core::tracer::{SpanExt, ThreadLocalTracer};
use futures::Future;
use libfrugalos;
use libfrugalos::schema::frugalos as rpc;
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::span::Span;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use client::FrugalosClient;
use {Error, ErrorKind};

use daemon::FrugalosDaemonHandle;

#[derive(Debug, Clone)]
pub struct RpcServer {
    client: FrugalosClient,
    daemon: FrugalosDaemonHandle,
    tracer: ThreadLocalTracer,
}
impl RpcServer {
    pub fn register(
        client: FrugalosClient,
        daemon: FrugalosDaemonHandle,
        builder: &mut RpcServerBuilder,
        tracer: ThreadLocalTracer,
    ) {
        let this = RpcServer {
            client,
            daemon,
            tracer,
        };
        builder.add_call_handler::<rpc::DeleteObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::GetObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::HeadObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::PutObjectRpc, _>(this.clone());
        builder.add_call_handler::<rpc::ListObjectsRpc, _>(this.clone());
        builder.add_call_handler::<rpc::StopRpc, _>(this.clone());
        builder.add_call_handler::<rpc::TakeSnapshotRpc, _>(this.clone());

        builder.add_call_handler::<rpc::GetLatestVersionRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectByVersionRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectsByRangeRpc, _>(this.clone());
        builder.add_call_handler::<rpc::DeleteObjectsByPrefixRpc, _>(this.clone());
    }

    fn span_from_object_request(
        &self,
        operation: &'static str,
        request: &rpc::ObjectRequest,
    ) -> Span {
        // TODO リクエストからの span を引き継ぐ
        let mut span = self.tracer.span(|t| t.span(operation).start());
        let bucket_id = request.bucket_id.clone();
        let object_id = request.object_id.clone();
        span.set_tag(|| StdTag::component(module_path!()));
        span.set_tag(|| Tag::new("bucket.id", object_id));
        span.set_tag(|| Tag::new("object.id", bucket_id));
        span
    }
}
impl HandleCall<rpc::DeleteObjectRpc> for RpcServer {
    fn handle_call(&self, request: rpc::ObjectRequest) -> Reply<rpc::DeleteObjectRpc> {
        let mut span = self.span_from_object_request("delete_object_rpc", &request);
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .expect(request.expect)
            .span(&span)
            .delete(request.object_id);
        Reply::future(
            future
                .then(move |result| {
                    result
                        .map(|version| {
                            version.map(|version| {
                                span.set_tag(|| Tag::new("object.version", version.0.to_string()));
                                version
                            })
                        })
                        .map_err(|e| {
                            span.log_error(&e);
                            into_rpc_error(e)
                        })
                })
                .then(Ok),
        )
    }
}
impl HandleCall<rpc::DeleteObjectByVersionRpc> for RpcServer {
    fn handle_call(&self, request: rpc::VersionRequest) -> Reply<rpc::DeleteObjectByVersionRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .delete_by_version(request.segment as usize, request.object_version);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}
impl HandleCall<rpc::DeleteObjectsByRangeRpc> for RpcServer {
    fn handle_call(&self, request: rpc::RangeRequest) -> Reply<rpc::DeleteObjectsByRangeRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .delete_by_range(request.segment as usize, request.targets);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}
impl HandleCall<rpc::DeleteObjectsByPrefixRpc> for RpcServer {
    fn handle_call(&self, request: rpc::PrefixRequest) -> Reply<rpc::DeleteObjectsByPrefixRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .delete_by_prefix(request.prefix);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}
impl HandleCall<rpc::GetObjectRpc> for RpcServer {
    fn handle_call(&self, request: rpc::ObjectRequest) -> Reply<rpc::GetObjectRpc> {
        let mut span = self.span_from_object_request("get_object_rpc", &request);
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .expect(request.expect)
            .span(&span)
            .get(request.object_id);
        Reply::future(
            future
                .then(move |result| {
                    result
                        .map(|o| {
                            o.map(|o| {
                                span.set_tag(|| {
                                    Tag::new("object.version", o.version.0.to_string())
                                });
                                (o.version, o.content)
                            })
                        })
                        .map_err(|e| {
                            span.log_error(&e);
                            into_rpc_error(e)
                        })
                })
                .then(Ok),
        )
    }
}
impl HandleCall<rpc::HeadObjectRpc> for RpcServer {
    fn handle_call(&self, request: rpc::ObjectRequest) -> Reply<rpc::HeadObjectRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .expect(request.expect)
            .head(request.object_id);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}
impl HandleCall<rpc::PutObjectRpc> for RpcServer {
    fn handle_call(&self, request: rpc::PutObjectRequest) -> Reply<rpc::PutObjectRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .deadline(into_cannyls_deadline(request.deadline))
            .expect(request.expect)
            .put(request.object_id, request.content);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}
impl HandleCall<rpc::ListObjectsRpc> for RpcServer {
    fn handle_call(&self, request: rpc::SegmentRequest) -> Reply<rpc::ListObjectsRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .list(request.segment as usize);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}

impl HandleCall<rpc::GetLatestVersionRpc> for RpcServer {
    fn handle_call(&self, request: rpc::SegmentRequest) -> Reply<rpc::GetLatestVersionRpc> {
        let future = self
            .client
            .request(request.bucket_id)
            .latest(request.segment as usize);
        Reply::future(future.map_err(into_rpc_error).then(Ok))
    }
}

impl HandleCall<rpc::StopRpc> for RpcServer {
    fn handle_call(&self, (): ()) -> Reply<rpc::StopRpc> {
        Reply::future(self.daemon.stop().map_err(into_rpc_error2).then(Ok))
    }
}
impl HandleCall<rpc::TakeSnapshotRpc> for RpcServer {
    fn handle_call(&self, (): ()) -> Reply<rpc::TakeSnapshotRpc> {
        // TODO: cast?
        self.daemon.take_snapshot();
        Reply::done(Ok(()))
    }
}

fn into_rpc_error(e: Error) -> libfrugalos::Error {
    let kind = match *e.kind() {
        ErrorKind::InvalidInput => libfrugalos::ErrorKind::InvalidInput,
        ErrorKind::NotFound => libfrugalos::ErrorKind::Other,
        ErrorKind::Unexpected(v) => libfrugalos::ErrorKind::Unexpected(v),
        ErrorKind::Other => libfrugalos::ErrorKind::Other,
    };
    kind.takes_over(e).into()
}

// TODO
fn into_rpc_error2(e: ::Error) -> libfrugalos::Error {
    let kind = match *e.kind() {
        ::ErrorKind::InvalidInput => libfrugalos::ErrorKind::InvalidInput,
        _ => libfrugalos::ErrorKind::Other,
    };
    kind.takes_over(e).into()
}

fn into_cannyls_deadline(d: Duration) -> cannyls::deadline::Deadline {
    cannyls::deadline::Deadline::Within(d)
}
