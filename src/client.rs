#![cfg_attr(feature = "cargo-clippy", allow(ptr_arg))]
#![cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
use atomic_immut::AtomicImmut;
use cannyls::deadline::Deadline;
use frugalos_segment::ObjectValue;
use futures::{self, Future};
use libfrugalos::entity::bucket::BucketId;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use rustracing_jaeger::span::{Span, SpanHandle};
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use bucket::Bucket;
use {Error, ErrorKind};

type BoxFuture<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

#[derive(Clone)]
pub struct FrugalosClient {
    buckets: Arc<AtomicImmut<HashMap<BucketId, Bucket>>>,
}
impl FrugalosClient {
    pub(crate) fn new(buckets: Arc<AtomicImmut<HashMap<BucketId, Bucket>>>) -> Self {
        FrugalosClient { buckets }
    }
    pub fn request(&self, bucket_id: BucketId) -> Request {
        Request::new(self, bucket_id)
    }
    pub fn segment_count(&self, bucket_id: &BucketId) -> Option<u16> {
        self.buckets
            .load()
            .get(bucket_id)
            .map(|b| b.segments().len() as u16)
    }
}
impl fmt::Debug for FrugalosClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FrugalosClient {{ .. }}")
    }
}

macro_rules! try_get_bucket {
    ($buckets:expr, $bucket_id:expr) => {
        if let Some(bucket) = $buckets.get(&$bucket_id) {
            bucket
        } else {
            let e = ErrorKind::NotFound
                .cause(format!("No such bucket: {:?}", $bucket_id))
                .into();
            return Box::new(futures::failed(e));
        }
    };
}

pub struct Request<'a> {
    client: &'a FrugalosClient,
    bucket_id: BucketId,
    deadline: Deadline,
    expect: Expect,
    parent: SpanHandle,
}
impl<'a> Request<'a> {
    pub fn new(client: &'a FrugalosClient, bucket_id: BucketId) -> Self {
        Request {
            client,
            bucket_id,
            deadline: Deadline::Within(Duration::from_millis(5000)),
            expect: Expect::Any,
            parent: Span::inactive().handle(),
        }
    }
    pub fn deadline(&mut self, deadline: Deadline) -> &mut Self {
        self.deadline = deadline;
        self
    }
    pub fn expect(&mut self, expect: Expect) -> &mut Self {
        self.expect = expect;
        self
    }
    pub fn span(&mut self, span: &Span) -> &mut Self {
        self.parent = span.handle();
        self
    }
    pub fn get(&self, object_id: ObjectId) -> BoxFuture<Option<ObjectValue>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        let segment = bucket.get_segment(&object_id);
        let future = segment.get(object_id, self.deadline, self.parent.clone());
        Box::new(future.map_err(|e| track!(Error::from(e))))
    }
    pub fn head(&self, object_id: ObjectId) -> BoxFuture<Option<ObjectVersion>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        let segment = bucket.get_segment(&object_id);
        let future = segment.head(object_id, self.parent.clone());
        Box::new(future.map_err(|e| track!(Error::from(e))))
    }
    pub fn put(&self, object_id: ObjectId, content: Vec<u8>) -> BoxFuture<(ObjectVersion, bool)> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        let segment = bucket.get_segment(&object_id);
        let future = segment.put(
            object_id,
            content,
            self.deadline,
            self.expect.clone(),
            self.parent.clone(),
        );
        Box::new(future.map_err(|e| track!(Error::from(e))))
    }
    pub fn delete(&self, object_id: ObjectId) -> BoxFuture<Option<ObjectVersion>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        let segment = bucket.get_segment(&object_id);
        let future = segment.delete(
            object_id,
            self.deadline,
            self.expect.clone(),
            self.parent.clone(),
        );
        Box::new(future.map_err(|e| track!(Error::from(e))))
    }
    pub fn delete_by_version(
        &self,
        segment: usize,
        object_version: ObjectVersion,
    ) -> BoxFuture<Option<ObjectVersion>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        if segment < bucket.segments().len() {
            let segment = &bucket.segments()[segment];
            let future =
                segment.delete_by_version(object_version, self.deadline, self.parent.clone());
            Box::new(future.map_err(|e| track!(Error::from(e))))
        } else {
            let e = ErrorKind::InvalidInput.cause(format!("Too large segment number: {}", segment));
            Box::new(futures::failed(e.into()))
        }
    }
    pub fn delete_by_range(
        &self,
        segment: usize,
        targets: Range<ObjectVersion>,
    ) -> BoxFuture<Vec<ObjectSummary>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        if segment < bucket.segments().len() {
            let segment = &bucket.segments()[segment];
            let future = segment.delete_by_range(targets, self.deadline, self.parent.clone());
            Box::new(future.map_err(|e| track!(Error::from(e))))
        } else {
            let e = ErrorKind::InvalidInput.cause(format!("Too large segment number: {}", segment));
            Box::new(futures::failed(e.into()))
        }
    }
    /// 削除した ObjectVersion を返そうとすると、場合によっては大量になってしまうため返さない。
    pub fn delete_by_prefix(
        &self,
        prefix: ObjectPrefix,
    ) -> BoxFuture<DeleteObjectsByPrefixSummary> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        let mut futures = Vec::new();

        // どこかのセグメントで削除が失敗した場合に不整合が発生するがひとまず対応はしない。
        for segment in bucket.segments() {
            futures.push(
                segment
                    .delete_by_prefix(prefix.clone(), self.deadline, self.parent.clone())
                    .map_err(|e| track!(Error::from(e))),
            );
        }

        Box::new(futures::future::join_all(futures).map(|summaries| {
            let total = summaries.iter().map(|summary| summary.total).sum();
            DeleteObjectsByPrefixSummary { total }
        }))
    }
    pub fn list(&self, segment: usize) -> BoxFuture<Vec<ObjectSummary>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        if segment < bucket.segments().len() {
            let future = bucket.segments()[segment].list();
            Box::new(future.map_err(|e| track!(Error::from(e))))
        } else {
            let e = ErrorKind::InvalidInput.cause(format!("Too large segment number: {}", segment));
            Box::new(futures::failed(e.into()))
        }
    }
    pub fn latest(&self, segment: usize) -> BoxFuture<Option<ObjectSummary>> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        if segment < bucket.segments().len() {
            let future = bucket.segments()[segment].latest();
            Box::new(future.map_err(|e| track!(Error::from(e))))
        } else {
            let e = ErrorKind::InvalidInput.cause(format!("Too large segment number: {}", segment));
            Box::new(futures::failed(e.into()))
        }
    }
    pub fn object_count(&self, segment: usize) -> BoxFuture<u64> {
        let buckets = self.client.buckets.load();
        let bucket = try_get_bucket!(buckets, self.bucket_id);
        if segment < bucket.segments().len() {
            let future = bucket.segments()[segment].object_count();
            Box::new(future.map_err(|e| track!(Error::from(e))))
        } else {
            let e = ErrorKind::InvalidInput.cause(format!("Too large segment number: {}", segment));
            Box::new(futures::failed(e.into()))
        }
    }
}
