use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
use bytecodec::json_codec::JsonEncoder;
use bytecodec::null::NullDecoder;
use cannyls::deadline::Deadline;
use fibers_http_server::metrics::WithMetrics;
use fibers_http_server::{
    HandleRequest, Reply, Req, Res, ServerBuilder as HttpServerBuilder, Status,
};
use frugalos_core::tracer::ThreadLocalTracer;
use frugalos_segment::SegmentStatistics;
use futures::future::Either;
use futures::{self, Future, Stream};
use httpcodec::{BodyDecoder, BodyEncoder, HeadBodyEncoder, Header};
use libfrugalos::consistency::ReadConsistency;
use libfrugalos::entity::bucket::BucketKind;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use prometrics::metrics::{Counter, CounterBuilder, MetricBuilder};
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::reporter::JaegerCompactReporter;
use rustracing_jaeger::span::{SpanContext, SpanReceiver};
use slog::Logger;
use std::cmp;
use std::collections::HashMap;
use std::str;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use trackable::error::ErrorKindExt;
use url::Url;

use client::FrugalosClient;
use codec::{AsyncEncoder, ObjectResultEncoder};
use http::{
    make_json_response, make_object_response, not_found, BucketStatistics, DeleteFragmentResponse,
    HttpResult, ObjectResponse, TraceHeader,
};
use many_objects::put_many_objects;
use {Error, ErrorKind, FrugalosConfig, Result};

// TODO: 冗長化設定等を反映した正確な上限を使用する
const MAX_PUT_OBJECT_SIZE: usize = 50 * 1024 * 1024;

macro_rules! try_badarg {
    ($e:expr) => {
        match track!($e) {
            Err(e) => {
                return Box::new(futures::finished(Res::new(
                    Status::BadRequest,
                    HttpResult::Err(e),
                )));
            }
            Ok(v) => v,
        }
    };
}

macro_rules! try_badarg_option {
    ($e:expr) => {
        match $e {
            None => {
                let error = ErrorKind::InvalidInput.cause("not designated");
                return Box::new(futures::finished(Res::new(
                    Status::BadRequest,
                    HttpResult::Err(error.into()),
                )));
            }
            Some(v) => v,
        }
    };
}

/// オブジェクト操作のメトリクス
///
/// bucket_kind 毎に label を設定する
#[derive(Clone)]
struct ObjectRequestMetrics {
    get_total: Counter,
    put_total: Counter,
    delete_total: Counter,
}
impl ObjectRequestMetrics {
    pub fn new(bucket_kind: &str) -> Self {
        let get_total = CounterBuilder::new("object_requests_total")
            .namespace("frugalos")
            .subsystem("http")
            .label("bucket_kind", bucket_kind)
            .label("method", "GET")
            .default_registry()
            .finish()
            .expect("metric should be well-formed");
        let put_total = CounterBuilder::new("object_requests_total")
            .namespace("frugalos")
            .subsystem("http")
            .label("bucket_kind", bucket_kind)
            .label("method", "PUT")
            .default_registry()
            .finish()
            .expect("metric should be well-formed");
        let delete_total = CounterBuilder::new("object_requests_total")
            .namespace("frugalos")
            .subsystem("http")
            .label("bucket_kind", bucket_kind)
            .label("method", "DELETE")
            .default_registry()
            .finish()
            .expect("metric should be well-formed");
        ObjectRequestMetrics {
            get_total,
            put_total,
            delete_total,
        }
    }
    pub fn increment(&self, method: &str) {
        match method {
            "GET" => self.get_total.increment(),
            "PUT" => self.put_total.increment(),
            "DELETE" => self.delete_total.increment(),
            _ => (),
        }
    }
}

#[derive(Clone)]
struct Metrics {
    object_requests: HashMap<u32, ObjectRequestMetrics>,
}
impl Metrics {
    pub fn new() -> Self {
        let mut object_requests = HashMap::new();
        object_requests.insert(
            BucketKind::Metadata as u32,
            ObjectRequestMetrics::new("metadata"),
        );
        object_requests.insert(
            BucketKind::Dispersed as u32,
            ObjectRequestMetrics::new("dispersed"),
        );
        object_requests.insert(
            BucketKind::Replicated as u32,
            ObjectRequestMetrics::new("replicated"),
        );
        Metrics { object_requests }
    }
    pub fn increment_object_requests(&self, method: &str, bucket_kind: BucketKind) {
        if let Some(m) = self.object_requests.get(&(bucket_kind as u32)) {
            m.increment(method)
        }
    }
}

#[derive(Clone)]
pub struct Server {
    logger: Logger,
    config: FrugalosConfig,
    client: FrugalosClient,
    tracer: ThreadLocalTracer,
    metrics: Metrics,

    // TODO: remove
    large_object_count: Arc<AtomicUsize>,
}
impl Server {
    pub fn new(
        logger: Logger,
        config: FrugalosConfig,
        client: FrugalosClient,
        tracer: ThreadLocalTracer,
    ) -> Self {
        let metrics = Metrics::new();
        Server {
            logger,
            config,
            client,
            tracer,
            metrics,
            large_object_count: Arc::default(),
        }
    }
    pub fn register(self, builder: &mut HttpServerBuilder) -> Result<()> {
        track!(builder.add_handler(ListSegments(self.clone())))?;
        self.register_once_with_metrics(ListObjects(self.clone()), builder)?;
        self.register_once_with_metrics(ListObjectsByPrefix(self.clone()), builder)?;
        self.register_once_with_metrics(GetObject(self.clone()), builder)?;
        self.register_once_with_metrics(HeadObject(self.clone()), builder)?;
        self.register_once_with_metrics(HeadFragments(self.clone()), builder)?;
        self.register_once_with_metrics(DeleteObject(self.clone()), builder)?;
        self.register_once_with_metrics(DeleteObjectByPrefix(self.clone()), builder)?;
        self.register_once_with_metrics(DeleteFragment(self.clone()), builder)?;
        self.register_once_with_metrics(PutObject(self.clone()), builder)?;
        self.register_once_with_metrics(PutManyObject(self.clone()), builder)?;
        self.register_once_with_metrics(GetBucketStatistics(self.clone()), builder)?;
        track!(builder.add_handler(JemallocStats))?;
        track!(builder.add_handler(CurrentConfigurations(self.config)))?;
        Ok(())
    }

    /// Add one handler with metrics
    fn register_once_with_metrics<H>(
        &self,
        handle_request: H,
        builder: &mut HttpServerBuilder,
    ) -> Result<()>
    where
        H: HandleRequest,
        H::Decoder: Default,
        H::Encoder: Default,
    {
        // Config に書かれたバケツの設定を読む
        let bucket_config = self
            .config
            .fibers_http_server
            .request_duration_bucket_config
            .clone()
            .into();
        track!(
            builder.add_handler(WithMetrics::with_metrics_and_bucket_config(
                handle_request,
                MetricBuilder::new(),
                bucket_config
            ))
        )?;
        Ok(())
    }
}

struct ListSegments(Server);
impl HandleRequest for ListSegments {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets/*/segments";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<Segment>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let response = if let Some(segments) = self.0.client.segment_count(&bucket_id) {
            let segments = (0..segments).map(|id| Segment { id: id as u16 }).collect();
            make_json_response(Status::Ok, Ok(segments))
        } else {
            make_json_response(Status::NotFound, Err(not_found()))
        };
        Box::new(futures::finished(response))
    }
}

struct ListObjects(Server);
impl HandleRequest for ListObjects {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets/*/segments/*/objects";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<ObjectSummary>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<AsyncEncoder<JsonEncoder<Self::ResBody>>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let segment_num = try_badarg!(get_segment_num(req.url()));
        let future = self
            .0
            .client
            .request(bucket_id)
            .list(segment_num as usize)
            .then(|result| {
                let response = match track!(result) {
                    Ok(list) => make_json_response(Status::Ok, Ok(list)),
                    Err(ref e) if *e.kind() == ErrorKind::NotFound => {
                        make_json_response(Status::NotFound, Err(not_found()))
                    }
                    Err(e) => make_json_response(Status::InternalServerError, Err(e)),
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct ListObjectsByPrefix(Server);
impl HandleRequest for ListObjectsByPrefix {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets/*/object_prefixes/*";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<ObjectSummary>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<AsyncEncoder<JsonEncoder<Self::ResBody>>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_prefix = get_object_prefix(req.url());
        let deadline = try_badarg!(get_deadline(&req.url()));

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self.0.tracer.span(|t| {
            t.span("list_objects_by_prefix")
                .child_of(&client_span)
                .start()
        });
        span.set_tag(|| StdTag::http_method("GET"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object_prefix", object_prefix.clone()));

        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .span(&span)
            .list_by_prefix(ObjectPrefix(object_prefix))
            .then(|result| {
                let response = match track!(result) {
                    Ok(list) => make_json_response(Status::Ok, Ok(list)),
                    Err(ref e) if *e.kind() == ErrorKind::NotFound => {
                        make_json_response(Status::NotFound, Err(not_found()))
                    }
                    Err(e) => make_json_response(Status::InternalServerError, Err(e)),
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct GetBucketStatistics(Server);
impl HandleRequest for GetBucketStatistics {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets/*/stats";

    type ReqBody = ();
    type ResBody = HttpResult<BucketStatistics>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());

        let segments = if let Some(segments) = self.0.client.segment_count(&bucket_id) {
            segments
        } else {
            return Box::new(futures::finished(make_json_response(
                Status::NotFound,
                Err(not_found()),
            )));
        };

        let client = self.0.client.clone();
        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("get_bucket").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("GET"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));

        let futures: Vec<_> = (0..segments)
            .map(|segment_id| {
                client
                    .request(bucket_id.clone())
                    .span(&span)
                    .segment_stats(segment_id)
                    .map_err(|e| track!(e))
            })
            .collect();
        let usage = futures::future::join_all(futures).map(|x| {
            x.into_iter().fold(
                (0, 0),
                |(sum_real, sum_approximation), stats: SegmentStatistics| {
                    (
                        sum_real + stats.storage_usage_bytes_real,
                        sum_approximation + stats.storage_usage_bytes_approximation,
                    )
                },
            )
        });

        let bucket_id = get_bucket_id(req.url());
        let client = self.0.client.clone();
        let effectiveness_ratio = client.effectiveness_ratio(&bucket_id).expect("Never fail");
        let redundance_ratio = client.redundance_ratio(&bucket_id).expect("Never fail");
        let objects = futures::stream::iter_ok(0..segments)
            .and_then(move |segment| {
                let request = client.request(bucket_id.clone());
                request
                    .object_count(segment as usize)
                    .map_err(|e| track!(e))
            })
            .fold(0, |total, objects| -> Result<_> { Ok(total + objects) });
        let future = objects
            .join(usage)
            .then(move |result| match track!(result) {
                Err(e) => Ok(make_json_response(Status::InternalServerError, Err(e))),
                Ok((objects, (usage_real, usage_approximation))) => {
                    let usage_sum_overall = usage_real + usage_approximation;
                    let usage_sum_overall_f64 = usage_sum_overall as f64;
                    let usage_sum_effectiveness =
                        (usage_sum_overall_f64 * effectiveness_ratio) as u64;
                    let usage_sum_redundance = (usage_sum_overall_f64 * redundance_ratio) as u64;
                    let usage_real_effectiveness = cmp::min(usage_sum_effectiveness, usage_real);
                    let usage_real_redundance = usage_real.saturating_sub(usage_real_effectiveness);
                    let usage_approximation_effectiveness =
                        usage_sum_effectiveness.saturating_sub(usage_real_effectiveness);
                    let usage_approximation_redundance =
                        usage_sum_redundance.saturating_sub(usage_real_redundance);
                    let stats = BucketStatistics {
                        objects,
                        storage_usage_bytes_sum_overall: usage_sum_overall,
                        storage_usage_bytes_sum_effectiveness: usage_sum_effectiveness,
                        storage_usage_bytes_sum_redundance: usage_sum_redundance,
                        storage_usage_bytes_real_overall: usage_real,
                        storage_usage_bytes_real_effectiveness: usage_real_effectiveness,
                        storage_usage_bytes_real_redundance: usage_real_redundance,
                        storage_usage_bytes_approximation_overall: usage_approximation,
                        storage_usage_bytes_approximation_effectiveness:
                            usage_approximation_effectiveness,
                        storage_usage_bytes_approximation_redundance:
                            usage_approximation_redundance,
                    };

                    Ok(make_json_response(Status::Ok, Ok(stats)))
                }
            });
        Box::new(future)
    }
}

struct GetObject(Server);
impl HandleRequest for GetObject {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/buckets/*/objects/*";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<u8>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<ObjectResultEncoder>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id = get_object_id(req.url());

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("get_object").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("GET"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id", object_id.clone()));
        // TODO: deadline and expect

        if let Some(bucket_kind) = self.0.client.bucket_kind(&bucket_id) {
            self.0.metrics.increment_object_requests("GET", bucket_kind);
        }

        let logger = self.0.logger.clone();
        let expect = try_badarg!(get_expect(&req.header()));
        let deadline = try_badarg!(get_deadline(&req.url()));
        let consistency = try_badarg!(get_consistency(&req.url()));
        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .expect(expect)
            .span(&span)
            .get(object_id, consistency)
            .then(move |result| {
                let response = match track!(result) {
                    Ok(None) => {
                        span.set_tag(|| StdTag::http_status_code(404));
                        make_object_response(Status::NotFound, None, Err(not_found()))
                    }
                    Ok(Some(object)) => {
                        span.set_tag(|| Tag::new("object.size", object.content.len() as i64));
                        span.set_tag(|| Tag::new("object.version", object.version.0 as i64));
                        span.set_tag(|| StdTag::http_status_code(200));
                        make_object_response(Status::Ok, Some(object.version), Ok(object.content))
                    }
                    // NOTE:
                    // オブジェクトが存在しない場合と、バケツが存在しない(まだ起動処理中かもしれない)は分ける
                    //
                    // Err(ref e) if *e.kind() == frugalos::ErrorKind::NotFound => {
                    //     span.set_tag(|| StdTag::http_status_code(404));
                    //     make_object_response(Status::NotFound, None, Err(not_found()))
                    // }
                    Err(e) => {
                        warn!(
                            logger,
                            "Cannot get object (bucket={:?}, object={:?}): {}",
                            get_bucket_id(req.url()),
                            get_object_id(req.url()),
                            e
                        );
                        span.set_tag(|| StdTag::http_status_code(500));
                        make_object_response(Status::InternalServerError, None, Err(e))
                    }
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct HeadObject(Server);
impl HandleRequest for HeadObject {
    const METHOD: &'static str = "HEAD";
    const PATH: &'static str = "/v1/buckets/*/objects/*";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<u8>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = HeadBodyEncoder<BodyEncoder<ObjectResultEncoder>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id = get_object_id(req.url());

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("head_object").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("HEAD"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id", object_id.clone()));
        // TODO: deadline and expect

        let logger = self.0.logger.clone();
        let expect = try_badarg!(get_expect(&req.header()));
        let deadline = try_badarg!(get_deadline(&req.url()));
        let consistency = try_badarg!(get_consistency(&req.url()));
        let check_storage = try_badarg!(get_check_storage(&req.url()));
        let future = if check_storage {
            Either::A(
                self.0
                    .client
                    .request(bucket_id)
                    .deadline(deadline)
                    .expect(expect)
                    .span(&span)
                    .head_storage(object_id, consistency),
            )
        } else {
            Either::B(
                self.0
                    .client
                    .request(bucket_id)
                    .deadline(deadline)
                    .expect(expect)
                    .span(&span)
                    .head(object_id, consistency),
            )
        };
        let future = future.then(move |result| {
            let response = match track!(result) {
                Ok(None) => {
                    span.set_tag(|| StdTag::http_status_code(404));
                    ObjectResponse::new(Status::NotFound, Err(not_found())).into_response()
                }
                Ok(Some(version)) => {
                    span.set_tag(|| Tag::new("object.version", version.0 as i64));
                    span.set_tag(|| StdTag::http_status_code(200));
                    ObjectResponse::new(Status::Ok, Ok(Vec::new()))
                        .version(Some(version))
                        .into_response()
                }
                // Err(ref e) if *e.kind() == frugalos::ErrorKind::NotFound => {
                //     span.set_tag(|| StdTag::http_status_code(404));
                //     make_object_response(Status::NotFound, None, Err(not_found()))
                // }
                Err(e) => {
                    warn!(
                        logger,
                        "Cannot get object (bucket={:?}, object={:?}): {}",
                        get_bucket_id(req.url()),
                        get_object_id(req.url()),
                        e
                    );
                    span.set_tag(|| StdTag::http_status_code(500));
                    ObjectResponse::new(Status::InternalServerError, Err(e)).into_response()
                }
            };
            Ok(response)
        });
        Box::new(future)
    }
}

struct HeadFragments(Server);
impl HandleRequest for HeadFragments {
    const METHOD: &'static str = "HEAD";
    const PATH: &'static str = "/v1/buckets/*/objects/*/fragments";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<u8>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = HeadBodyEncoder<BodyEncoder<ObjectResultEncoder>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id = get_object_id(req.url());

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("head_fragments").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("HEAD"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id", object_id.clone()));

        let logger = self.0.logger.clone();
        let expect = try_badarg!(get_expect(&req.header()));
        let deadline = try_badarg!(get_deadline(&req.url()));
        let consistency = try_badarg!(get_consistency(&req.url()));
        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .expect(expect)
            .span(&span)
            .count_fragments(object_id, consistency)
            .then(move |result| {
                let response = match track!(result) {
                    Ok(None) => {
                        span.set_tag(|| StdTag::http_status_code(404));
                        ObjectResponse::new(Status::NotFound, Err(not_found())).into_response()
                    }
                    Ok(Some(summary)) => {
                        span.set_tag(|| Tag::new("fragments.is_corrupted", summary.is_corrupted));
                        span.set_tag(|| {
                            Tag::new("fragments.found_total", summary.found_total as i64)
                        });
                        span.set_tag(|| {
                            Tag::new("fragments.lost_total", summary.lost_total as i64)
                        });
                        span.set_tag(|| StdTag::http_status_code(200));
                        ObjectResponse::new(Status::Ok, Ok(Vec::new()))
                            .fragments(Some(summary))
                            .into_response()
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "Cannot get fragments (bucket={:?}, object={:?}): {}",
                            get_bucket_id(req.url()),
                            get_object_id(req.url()),
                            e
                        );
                        span.set_tag(|| StdTag::http_status_code(500));
                        ObjectResponse::new(Status::InternalServerError, Err(e)).into_response()
                    }
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct DeleteObject(Server);
impl HandleRequest for DeleteObject {
    const METHOD: &'static str = "DELETE";
    const PATH: &'static str = "/v1/buckets/*/objects/*";

    type ReqBody = ();
    type ResBody = HttpResult<Vec<u8>>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<ObjectResultEncoder>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id = get_object_id(req.url());

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("delete_object").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("DELETE"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id", object_id.clone()));
        // TODO: deadline and expect

        if let Some(bucket_kind) = self.0.client.bucket_kind(&bucket_id) {
            self.0
                .metrics
                .increment_object_requests("DELETE", bucket_kind);
        }

        let logger = self.0.logger.clone();
        let expect = try_badarg!(get_expect(&req.header()));
        let deadline = try_badarg!(get_deadline(&req.url()));

        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .expect(expect)
            .span(&span)
            .delete(object_id)
            .then(move |result| {
                let response = match track!(result) {
                    Ok(None) => {
                        span.set_tag(|| StdTag::http_status_code(404));
                        make_object_response(Status::NotFound, None, Err(not_found()))
                    }
                    Ok(Some(version)) => {
                        span.set_tag(|| Tag::new("object.version", version.0 as i64));
                        span.set_tag(|| StdTag::http_status_code(200));
                        make_object_response(Status::Ok, Some(version), Ok(Vec::new()))
                    }
                    // Err(ref e) if *e.kind() == frugalos::ErrorKind::NotFound => {
                    //     span.set_tag(|| StdTag::http_status_code(404));
                    //     make_object_response(Status::NotFound, None, Err(not_found()))
                    // }
                    Err(e) => {
                        if let ErrorKind::Unexpected(version) = *e.kind() {
                            span.set_tag(|| StdTag::http_status_code(412));
                            make_object_response(Status::PreconditionFailed, version, Err(e))
                        } else {
                            warn!(
                                logger,
                                "Cannot delete object (bucket={:?}, object={:?}): {}",
                                get_bucket_id(req.url()),
                                get_object_id(req.url()),
                                e
                            );
                            span.set_tag(|| StdTag::http_status_code(500));
                            make_object_response(Status::InternalServerError, None, Err(e))
                        }
                    }
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct DeleteObjectByPrefix(Server);
impl HandleRequest for DeleteObjectByPrefix {
    const METHOD: &'static str = "DELETE";
    const PATH: &'static str = "/v1/buckets/*/object_prefixes/*";

    type ReqBody = ();
    type ResBody = HttpResult<DeleteObjectsByPrefixSummary>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<AsyncEncoder<JsonEncoder<Self::ResBody>>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_prefix = get_object_prefix(req.url());

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self.0.tracer.span(|t| {
            t.span("delete_object_by_prefix")
                .child_of(&client_span)
                .start()
        });
        span.set_tag(|| StdTag::http_method("DELETE"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object_prefix", object_prefix.clone()));

        let logger = self.0.logger.clone();
        let deadline = try_badarg!(get_deadline(&req.url()));
        let future = self
            .0
            .client
            .request(bucket_id.clone())
            .deadline(deadline)
            .span(&span)
            .delete_by_prefix(ObjectPrefix(object_prefix.clone()))
            .then(move |result| {
                let response = match track!(result) {
                    Ok(summary) => {
                        span.set_tag(|| StdTag::http_status_code(200));
                        span.set_tag(|| Tag::new("total", summary.total.to_string()));
                        make_json_response(Status::Ok, Ok(summary))
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "Cannot delete object (bucket={:?}, object_prefix={:?}): {}",
                            bucket_id,
                            object_prefix,
                            e
                        );
                        span.set_tag(|| StdTag::http_status_code(500));
                        make_json_response(Status::InternalServerError, Err(e))
                    }
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct DeleteFragment(Server);
impl HandleRequest for DeleteFragment {
    const METHOD: &'static str = "DELETE";
    const PATH: &'static str = "/v1/buckets/*/objects/*/fragments/*";

    type ReqBody = ();
    type ResBody = HttpResult<DeleteFragmentResponse>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id = get_object_id(req.url());
        let fragment_index = try_badarg_option!(try_badarg!(get_fragment_index(req.url())));

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("delete_fragment").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("DELETE"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id", object_id.clone()));

        let logger = self.0.logger.clone();
        let deadline = try_badarg!(get_deadline(&req.url()));

        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .span(&span)
            .delete_fragment(object_id, fragment_index)
            .then(move |result| {
                let (status, body) = match track!(result) {
                    Ok(None) => {
                        span.set_tag(|| StdTag::http_status_code(404));
                        (Status::NotFound, Err(not_found()))
                    }
                    Ok(Some((version, result))) => {
                        span.set_tag(|| Tag::new("object.version", version.0 as i64));
                        span.set_tag(|| StdTag::http_status_code(200));
                        if let Some((deleted, device_id, lump_id)) = result {
                            (
                                Status::Ok,
                                Ok(DeleteFragmentResponse::new(
                                    version, deleted, device_id, lump_id,
                                )),
                            )
                        } else {
                            (Status::NotFound, Err(not_found()))
                        }
                    }
                    Err(e) => {
                        if let ErrorKind::InvalidInput = *e.kind() {
                            span.set_tag(|| StdTag::http_status_code(400));
                            (Status::BadRequest, Err(e))
                        } else if let ErrorKind::Unexpected(_version) = *e.kind() {
                            span.set_tag(|| StdTag::http_status_code(412));
                            (Status::PreconditionFailed, Err(e))
                        } else {
                            warn!(
                                logger,
                                "Cannot delete object fragment (bucket={:?}, object={:?}): {}",
                                get_bucket_id(req.url()),
                                get_object_id(req.url()),
                                e
                            );
                            span.set_tag(|| StdTag::http_status_code(500));
                            (Status::InternalServerError, Err(e))
                        }
                    }
                };
                Ok(make_json_response(status, body))
            });
        Box::new(future)
    }
}

struct PutObject(Server);
impl HandleRequest for PutObject {
    const METHOD: &'static str = "PUT";
    const PATH: &'static str = "/v1/buckets/*/objects/*";

    type ReqBody = Vec<u8>;
    type ResBody = HttpResult<Vec<u8>>;
    type Decoder = BodyDecoder<RemainingBytesDecoder>;
    type Encoder = BodyEncoder<ObjectResultEncoder>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request_head(&self, req: &Req<()>) -> Option<Res<Self::ResBody>> {
        // TODO: decoderにもチェックを入れる
        let n: Option<Option<usize>> = req.header().parse_field("content-length").ok();
        if let Some(Some(n)) = n {
            if n > MAX_PUT_OBJECT_SIZE {
                let count = self.0.large_object_count.fetch_add(1, Ordering::SeqCst);
                warn!(
                    self.0.logger,
                    "Too large body size ({} bytes): {}",
                    n,
                    req.url()
                );
                if count != 0 {
                    // 最初だけはオブジェクトをダンプしたいので即座にエラーにはしない
                    return Some(make_object_response(
                        Status::BadRequest,
                        None,
                        Err(track!(ErrorKind::InvalidInput.cause("Too large body size")).into()),
                    ));
                }
            }
        }
        None
    }

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id = get_object_id(req.url());
        let (req, content) = req.take_body();
        if content.len() > MAX_PUT_OBJECT_SIZE {
            warn!(
                self.0.logger,
                "Too large body size ({} bytes; dumped): {}",
                content.len(),
                req.url()
            );
            return Box::new(futures::finished(make_object_response(
                Status::BadRequest,
                None,
                Err(track!(ErrorKind::InvalidInput.cause("Too large body size")).into()),
            )));
        }

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("put_object").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("PUT"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id", object_id.clone()));
        span.set_tag(|| Tag::new("object.size", content.len().to_string()));

        if let Some(bucket_kind) = self.0.client.bucket_kind(&bucket_id) {
            self.0.metrics.increment_object_requests("PUT", bucket_kind);
        }

        // TODO: deadline and expect
        let logger = self.0.logger.clone();
        let expect = try_badarg!(get_expect(&req.header()));
        let deadline = try_badarg!(get_deadline(&req.url()));
        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .expect(expect)
            .span(&span)
            .put(object_id, content)
            .then(move |result| {
                let response = match track!(result) {
                    Ok((version, created)) => {
                        let status = if created { Status::Created } else { Status::Ok };
                        span.set_tag(|| Tag::new("object.version", version.0 as i64));
                        span.set_tag(|| StdTag::http_status_code(status.code()));
                        make_object_response(status, Some(version), Ok(Vec::new()))
                    }
                    Err(e) => {
                        if let ErrorKind::Unexpected(version) = *e.kind() {
                            span.set_tag(|| StdTag::http_status_code(412));
                            make_object_response(Status::PreconditionFailed, version, Err(e))
                        } else {
                            warn!(
                                logger,
                                "Cannot put object (bucket={:?}, object={:?}): {}",
                                get_bucket_id(req.url()),
                                get_object_id(req.url()),
                                e
                            );
                            span.set_tag(|| StdTag::http_status_code(500));
                            make_object_response(Status::InternalServerError, None, Err(e))
                        }
                    }
                };
                Ok(response)
            });
        Box::new(future)
    }
}

struct PutManyObject(Server);
impl HandleRequest for PutManyObject {
    const METHOD: &'static str = "PUT";
    const PATH: &'static str = "/v1/buckets/*/many_objects/*";

    type ReqBody = Vec<u8>;
    type ResBody = HttpResult<Vec<u8>>;
    type Decoder = BodyDecoder<RemainingBytesDecoder>;
    type Encoder = BodyEncoder<ObjectResultEncoder>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request_head(&self, req: &Req<()>) -> Option<Res<Self::ResBody>> {
        // TODO: decoderにもチェックを入れる
        let n: Option<Option<usize>> = req.header().parse_field("content-length").ok();
        if let Some(Some(n)) = n {
            if n > MAX_PUT_OBJECT_SIZE {
                warn!(
                    self.0.logger,
                    "Too large body size ({} bytes): {}",
                    n,
                    req.url()
                );
            }
        }
        None
    }

    fn handle_request(&self, req: Req<Self::ReqBody>) -> Self::Reply {
        let bucket_id = get_bucket_id(req.url());
        let object_id_prefix = get_object_id(req.url());
        let object_start_index =
            try_badarg_option!(try_badarg!(get_usize_option(req.url(), "start")));
        let object_count = try_badarg_option!(try_badarg!(get_usize_option(req.url(), "count")));
        let concurrency = try_badarg!(get_usize_option(req.url(), "concurrency")).unwrap_or(10);
        let (req, content) = req.take_body();
        if content.len() > MAX_PUT_OBJECT_SIZE {
            warn!(
                self.0.logger,
                "Too large body size ({} bytes; dumped): {}",
                content.len(),
                req.url()
            );
            return Box::new(futures::finished(make_object_response(
                Status::BadRequest,
                None,
                Err(track!(ErrorKind::InvalidInput.cause("Too large body size")).into()),
            )));
        }

        let client_span = SpanContext::extract_from_http_header(&TraceHeader(req.header()))
            .ok()
            .and_then(|c| c);
        let mut span = self
            .0
            .tracer
            .span(|t| t.span("put_many_objects").child_of(&client_span).start());
        span.set_tag(|| StdTag::http_method("PUT"));
        span.set_tag(|| Tag::new("bucket.id", bucket_id.clone()));
        span.set_tag(|| Tag::new("object.id_prefix", object_id_prefix.clone()));
        span.set_tag(|| Tag::new("object.size", content.len().to_string()));
        span.set_tag(|| Tag::new("object.start", object_start_index as i64));
        span.set_tag(|| Tag::new("object.count", object_count as i64));

        // TODO: deadline and expect
        let future = put_many_objects(
            self.0.client.clone(),
            span,
            self.0.logger.clone(),
            bucket_id,
            object_id_prefix,
            object_start_index,
            object_count,
            concurrency,
            content,
        );
        // Errors are suppressed. Always return 201 Created.
        let response = Res::new(Status::Created, HttpResult::Ok(Vec::new()));

        let future = future.map(|_| response);
        Box::new(future)
    }
}

struct JemallocStats;
impl HandleRequest for JemallocStats {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/jemalloc/stats";

    type ReqBody = ();
    type ResBody = Vec<u8>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<BytesEncoder<Vec<u8>>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, _req: Req<Self::ReqBody>) -> Self::Reply {
        // many statistics are cached and only updated when the epoch is advanced.
        let _ = jemalloc_ctl::epoch();

        let mut buf = Vec::new();
        let _ = jemalloc_ctl::stats_print::stats_print(&mut buf, Default::default());
        Box::new(futures::finished(Res::new(Status::Ok, buf)))
    }
}

/// 現在稼働しているFrugalosプロセスのconfigurationを保持し
/// HTTP GETリクエストに応じるための構造体。
pub struct CurrentConfigurations(FrugalosConfig);
impl HandleRequest for CurrentConfigurations {
    const METHOD: &'static str = "GET";
    const PATH: &'static str = "/v1/frugalos/configurations";

    type ReqBody = ();
    type ResBody = HttpResult<FrugalosConfig>;
    type Decoder = BodyDecoder<NullDecoder>;
    type Encoder = BodyEncoder<JsonEncoder<Self::ResBody>>;
    type Reply = Reply<Self::ResBody>;

    fn handle_request(&self, _req: Req<Self::ReqBody>) -> Self::Reply {
        let response = make_json_response(Status::Ok, Ok(self.0.clone()));
        Box::new(futures::finished(response))
    }
}

pub fn spawn_report_spans_thread(rx: SpanReceiver) {
    let reporter = track_try_unwrap!(JaegerCompactReporter::new("frugalos"));
    thread::spawn(move || {
        while let Ok(span) = rx.recv() {
            let _ = reporter.report(&[span]);
        }
    });
}

#[derive(Debug, Clone, Copy, Serialize)]
struct Segment {
    id: u16,
}

fn get_bucket_id(url: &Url) -> String {
    url.path_segments()
        .expect("Never fails")
        .nth(2)
        .expect("Never fails")
        .to_string()
}

fn get_object_id(url: &Url) -> String {
    url.path_segments()
        .expect("Never fails")
        .nth(4)
        .expect("Never fails")
        .to_string()
}

fn get_object_prefix(url: &Url) -> String {
    url.path_segments()
        .expect("Never fails")
        .nth(4)
        .expect("Never fails")
        .to_string()
}

fn get_segment_num(url: &Url) -> Result<u16> {
    let n = track!(url
        .path_segments()
        .expect("Never fails")
        .nth(4)
        .expect("Never fails")
        .parse()
        .map_err(Error::from))?;
    Ok(n)
}

fn get_expect(header: &Header) -> Result<Expect> {
    for field in header.fields() {
        if field.name().eq_ignore_ascii_case("if-match") {
            if field.value() == "*" {
                return Ok(Expect::Any);
            } else {
                let versions = track!(parse_etag_values(field.value()))?;
                return Ok(Expect::IfMatch(versions));
            }
        } else if field.name().eq_ignore_ascii_case("if-none-match") {
            if field.value() == "*" {
                return Ok(Expect::None);
            } else {
                let versions = track!(parse_etag_values(field.value()))?;
                return Ok(Expect::IfNoneMatch(versions));
            }
        }
    }
    Ok(Expect::Any)
}

fn parse_etag_values(s: &str) -> Result<Vec<ObjectVersion>> {
    let mut versions = Vec::new();
    for token in s.split(',') {
        let token = token.trim();
        track_assert!(
            token.bytes().next() == Some(b'"') && token.bytes().last() == Some(b'"'),
            ErrorKind::InvalidInput,
            "Malformed ETag value: {:?}",
            token
        );
        let bytes = token.as_bytes();
        let hex = unsafe { str::from_utf8_unchecked(&bytes[1..bytes.len() - 1]) };
        let version = track!(u64::from_str_radix(hex, 16).map_err(Error::from))?;
        versions.push(ObjectVersion(version));
    }
    Ok(versions)
}

fn get_deadline(url: &Url) -> Result<Deadline> {
    for (k, v) in url.query_pairs() {
        if k == "deadline" {
            let n: u64 = track!(v.parse().map_err(Error::from))?;
            return Ok(Deadline::Within(Duration::from_millis(n)));
        }
    }
    Ok(Deadline::Within(Duration::from_secs(5)))
}

fn get_subset(url: &Url) -> Result<usize> {
    for (k, v) in url.query_pairs() {
        if k == "subset" {
            return track!(v.parse::<usize>().map_err(Error::from));
        }
    }
    Ok(1)
}

fn get_consistency(url: &Url) -> Result<ReadConsistency> {
    for (k, v) in url.query_pairs() {
        if k == "consistency" {
            let consistency = match v.as_ref() {
                "consistent" => Ok(ReadConsistency::Consistent),
                "stale" => Ok(ReadConsistency::Stale),
                "quorum" => Ok(ReadConsistency::Quorum),
                "subset" => {
                    let n = track!(get_subset(url))?;
                    Ok(ReadConsistency::Subset(n))
                }
                _ => Err(ErrorKind::InvalidInput
                    .cause(format!("Undefined consistency level: {}", v))
                    .into()),
            };
            return consistency;
        }
    }
    Ok(Default::default())
}

fn get_check_storage(url: &Url) -> Result<bool> {
    for (k, v) in url.query_pairs() {
        if k == "check_storage" {
            let b: bool = track!(v.parse().map_err(Error::from))?;
            return Ok(b);
        }
    }
    Ok(false)
}

fn get_usize_option(url: &Url, option_name: &str) -> Result<Option<usize>> {
    for (k, v) in url.query_pairs() {
        if k == option_name {
            return track!(v.parse::<usize>().map_err(Error::from).map(Some));
        }
    }
    Ok(None)
}

fn get_fragment_index(url: &Url) -> Result<Option<usize>> {
    let fragment_index = url.path_segments().expect("Never fails").nth(6);
    if let Some(v) = fragment_index {
        return track!(v.parse::<usize>().map_err(Error::from).map(Some));
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trackable::result::TestResult;

    #[test]
    fn get_subset_works() -> TestResult {
        let url = Url::from_str("http://example.com/").unwrap();
        let subset = track!(get_subset(&url))?;
        assert_eq!(1, subset);
        let url = Url::from_str("http://example.com/?subset=2").unwrap();
        let subset = track!(get_subset(&url))?;
        assert_eq!(2, subset);
        let url = Url::from_str("http://example.com/?subset=-1").unwrap();
        let subset = get_subset(&url);
        assert!(subset.is_err());
        Ok(())
    }

    #[test]
    fn get_consistency_works() -> TestResult {
        let url = Url::from_str("http://example.com/?consistency=consistent").unwrap();
        let consistency = track!(get_consistency(&url))?;
        assert_eq!(ReadConsistency::Consistent, consistency);
        let url = Url::from_str("http://example.com/?consistency=stale").unwrap();
        let consistency = track!(get_consistency(&url))?;
        assert_eq!(ReadConsistency::Stale, consistency);
        let url = Url::from_str("http://example.com/?consistency=subset").unwrap();
        let consistency = track!(get_consistency(&url))?;
        assert_eq!(ReadConsistency::Subset(1), consistency);
        let url = Url::from_str("http://example.com/?consistency=quorum").unwrap();
        let consistency = track!(get_consistency(&url))?;
        assert_eq!(ReadConsistency::Quorum, consistency);
        let url = Url::from_str("http://example.com/").unwrap();
        let consistency = track!(get_consistency(&url))?;
        assert_eq!(ReadConsistency::Consistent, consistency);
        Ok(())
    }
}
