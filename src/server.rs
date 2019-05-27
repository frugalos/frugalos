use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
use bytecodec::json_codec::JsonEncoder;
use bytecodec::null::NullDecoder;
use cannyls::deadline::Deadline;
use fibers_http_server::metrics::WithMetrics;
use fibers_http_server::{
    HandleRequest, Reply, Req, Res, ServerBuilder as HttpServerBuilder, Status,
};
use frugalos_core::tracer::ThreadLocalTracer;
use futures::{self, Future, Stream};
use httpcodec::{BodyDecoder, BodyEncoder, HeadBodyEncoder, Header};
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use rustracing::tag::{StdTag, Tag};
use rustracing_jaeger::reporter::JaegerCompactReporter;
use rustracing_jaeger::span::{SpanContext, SpanReceiver};
use slog::Logger;
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
    make_json_response, make_object_response, not_found, BucketStatistics, HttpResult, TraceHeader,
};
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

#[derive(Clone)]
pub struct Server {
    logger: Logger,
    config: FrugalosConfig,
    client: FrugalosClient,
    tracer: ThreadLocalTracer,

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
        Server {
            logger,
            config,
            client,
            tracer,
            large_object_count: Arc::default(),
        }
    }
    pub fn register(self, builder: &mut HttpServerBuilder) -> Result<()> {
        track!(builder.add_handler(ListSegments(self.clone())))?;
        track!(builder.add_handler(WithMetrics::new(ListObjects(self.clone()))))?;
        track!(builder.add_handler(WithMetrics::new(GetObject(self.clone()))))?;
        track!(builder.add_handler(WithMetrics::new(HeadObject(self.clone()))))?;
        track!(builder.add_handler(WithMetrics::new(DeleteObject(self.clone()))))?;
        track!(builder.add_handler(WithMetrics::new(DeleteObjectByPrefix(self.clone()))))?;
        track!(builder.add_handler(WithMetrics::new(PutObject(self.clone()))))?;
        track!(builder.add_handler(WithMetrics::new(GetBucketStatistics(self.clone()))))?;
        track!(builder.add_handler(JemallocStats))?;
        track!(builder.add_handler(CurrentConfigurations(self.config)))?;
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
        let future = futures::stream::iter_ok(0..segments)
            .and_then(move |segment| {
                let request = client.request(bucket_id.clone());
                request
                    .object_count(segment as usize)
                    .map_err(|e| track!(e))
            })
            .fold(0, |total, objects| -> Result<_> { Ok(total + objects) })
            .then(|result| match track!(result) {
                Err(e) => Ok(make_json_response(Status::InternalServerError, Err(e))),
                Ok(objects) => {
                    let stats = BucketStatistics { objects };
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
            .get(object_id)
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
        let future = self
            .0
            .client
            .request(bucket_id)
            .deadline(deadline)
            .expect(expect)
            .span(&span)
            .head(object_id)
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
            {
                use std::fs::File;
                use std::io::Write;

                // TODO: 調査用の一時コード
                let _ = File::create("/tmp/frugalos.huge.object.dump")
                    .and_then(|mut f| f.write_all(&content));
            }
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
            token.bytes().nth(0) == Some(b'"') && token.bytes().last() == Some(b'"'),
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
