use cannyls::deadline::Deadline;
use cannyls::lump::LumpId;
use cannyls::storage::StorageUsage;
use cannyls_rpc::DeviceId;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use futures::future::Either;
use futures::{self, Future};
use libfrugalos::consistency::ReadConsistency;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, FragmentsSummary, ObjectId, ObjectPrefix, ObjectSummary,
    ObjectVersion,
};
use libfrugalos::expect::Expect;
use rustracing_jaeger::span::SpanHandle;
use slog::Logger;
use std::mem;
use std::ops::Range;
use SegmentStatistics;

use self::ec::ErasureCoder;
use self::mds::MdsClient;
use self::storage::StorageClient;
use config::ClientConfig;
use trackable::error::ErrorKindExt;
use {Error, ErrorKind, ObjectValue, Result};

mod dispersed_storage;
pub mod ec; // to re-export in frugalos_segment/src/lib.rs
mod mds;
mod replicated_storage;
pub mod storage; // TODO: private

/// セグメントにアクセスるために使用するクライアント。
#[derive(Clone)]
pub struct Client {
    logger: Logger,
    mds: MdsClient,
    pub(crate) storage: StorageClient, // TODO: private
}
impl Client {
    /// 新しい`Client`インスタンスを生成する。
    pub fn new(
        logger: Logger,
        rpc_service: RpcServiceHandle,
        config: ClientConfig,
        ec: Option<ErasureCoder>,
    ) -> Result<Self> {
        let mds = MdsClient::new(
            logger.clone(),
            rpc_service.clone(),
            config.cluster.clone(),
            config.mds.clone(),
        );
        let storage = track!(StorageClient::new(logger.clone(), config, rpc_service, ec))?;
        Ok(Client {
            logger,
            mds,
            storage,
        })
    }

    /// オブジェクトを取得する。
    pub fn get(
        &self,
        id: ObjectId,
        deadline: Deadline,
        consistency: ReadConsistency,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectValue>, Error = Error> {
        let storage = self.storage.clone();
        self.mds
            .get(id, consistency, parent.clone())
            .and_then(move |object| {
                if let Some(object) = object {
                    let version = object.version;
                    let future = storage
                        .get(object, deadline, parent)
                        .map(move |content| ObjectValue { version, content })
                        .map(Some);
                    Either::A(future)
                } else {
                    Either::B(futures::future::ok(None))
                }
            })
    }

    /// オブジェクトの存在確認を行う。
    pub fn head(
        &self,
        id: ObjectId,
        consistency: ReadConsistency,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        self.mds.head(id, consistency, parent)
    }

    /// オブジェクトの存在確認をストレージ側に問い合わせる。
    pub fn head_storage(
        &self,
        id: ObjectId,
        deadline: Deadline,
        consistency: ReadConsistency,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        let storage = self.storage.clone();
        self.mds
            .head(id, consistency, parent.clone())
            .and_then(move |version| {
                if let Some(version) = version {
                    let future = storage
                        .head(version, deadline, parent)
                        .map(move |()| Some(version));
                    Either::A(future)
                } else {
                    Either::B(futures::future::ok(None))
                }
            })
    }

    /// フラグメント数をストレージに問い合わせる。
    pub fn count_fragments(
        &self,
        id: ObjectId,
        deadline: Deadline,
        consistency: ReadConsistency,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<FragmentsSummary>, Error = Error> {
        let storage = self.storage.clone();
        self.mds
            .head(id, consistency, parent.clone())
            .and_then(move |version| {
                if let Some(version) = version {
                    Either::A(storage.count_fragments(version, deadline, parent).map(Some))
                } else {
                    Either::B(futures::future::ok(None))
                }
            })
    }

    /// オブジェクトを保存する。
    pub fn put(
        &self,
        id: ObjectId,
        mut content: Vec<u8>,
        deadline: Deadline,
        expect: Expect,
        parent: SpanHandle,
    ) -> impl Future<Item = (ObjectVersion, bool), Error = Error> {
        // TODO: mdsにdeadlineを渡せるようにする
        // (repairのトリガー時間の判断用)
        let storage = self.storage.clone();
        let metadata = if self.storage.is_metadata() {
            mem::replace(&mut content, Vec::new())
        } else {
            Vec::new()
        };
        let object_id = id.clone();
        let logger = self.logger.clone();

        let mds = self.mds.clone();
        let expect_future = match expect {
            Expect::Any => {
                let f = mds
                    .head(id.clone(), ReadConsistency::Consistent, parent.clone())
                    .map(|version| version.map_or(Expect::None, |v| Expect::IfMatch(vec![v])));
                Either::A(f)
            }
            _ => Either::B(futures::future::ok(expect)),
        };

        expect_future.and_then(move |expect| {
            mds.put(id, metadata, expect, deadline, parent.clone())
                .and_then(move |(version, created)| {
                    let mut tracking = PutFailureTracking::new(logger, object_id);
                    storage
                        .put(version, content, deadline, parent)
                        .map(move |()| {
                            tracking.complete();
                            (version, created)
                        })
                })
        })
    }

    /// オブジェクトを削除する。
    pub fn delete(
        &self,
        id: ObjectId,
        _deadline: Deadline,
        expect: Expect,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        // TODO: mdsにdeadlineを渡せるようにする
        // (lump削除タイミングの決定用)
        let mds = self.mds.clone();
        let expect_future = match expect {
            Expect::Any => {
                let f = mds
                    .head(id.clone(), ReadConsistency::Consistent, parent.clone())
                    .map(|version| version.map_or(Expect::None, |v| Expect::IfMatch(vec![v])));
                Either::A(f)
            }
            _ => Either::B(futures::future::ok(expect)),
        };
        expect_future.and_then(move |expect| mds.delete(id, expect, parent))
    }

    /// バージョン指定でオブジェクトを削除する。
    pub fn delete_by_version(
        &self,
        version: ObjectVersion,
        _deadline: Deadline,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        self.mds.delete_by_version(version, parent)
    }

    /// バージョンの範囲指定でオブジェクトを削除する。
    pub fn delete_by_range(
        &self,
        targets: Range<ObjectVersion>,
        _deadline: Deadline,
        parent: SpanHandle,
    ) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        self.mds.delete_by_range(targets, parent)
    }

    /// IDの接頭辞指定でオブジェクトを削除する。
    pub fn delete_by_prefix(
        &self,
        prefix: ObjectPrefix,
        _deadline: Deadline,
        parent: SpanHandle,
    ) -> impl Future<Item = DeleteObjectsByPrefixSummary, Error = Error> {
        self.mds.delete_by_prefix(prefix, parent)
    }

    /// オブジェクトのフラグメントのみを削除する
    pub fn delete_fragment(
        &self,
        id: ObjectId,
        deadline: Deadline,
        parent: SpanHandle,
        index: usize,
    ) -> impl Future<Item = Option<(ObjectVersion, Option<(bool, DeviceId, LumpId)>)>, Error = Error>
    {
        let storage = self.storage.clone();
        self.mds
            .head(id, ReadConsistency::Consistent, parent.clone())
            .and_then(move |version| {
                if let Some(version) = version {
                    let future = storage
                        .delete_fragment(version, deadline, parent, index)
                        .map(move |result| {
                            if let Some((deleted, device_id, lump_id)) = result {
                                Some((version, Some((deleted, device_id, lump_id))))
                            } else {
                                None
                            }
                        });
                    Either::A(future)
                } else {
                    Either::B(futures::future::ok(None))
                }
            })
    }

    /// 保存済みのオブジェクト一覧を取得する。
    pub fn list(&self) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        self.mds.list()
    }

    /// 保存済みのオブジェクト一覧を接頭辞を指定して取得する。
    pub fn list_by_prefix(
        &self,
        prefix: ObjectPrefix,
        _deadline: Deadline,
        parent: SpanHandle,
    ) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        self.mds.list_by_prefix(prefix, parent)
    }

    /// セグメント内の最新オブジェクトのバージョンを取得する。
    pub fn latest(&self) -> impl Future<Item = Option<ObjectSummary>, Error = Error> {
        self.mds.latest()
    }

    /// セグメント内に保持されているオブジェクトの数を返す.
    pub fn object_count(&self) -> impl Future<Item = u64, Error = Error> {
        self.mds.object_count()
    }

    /// セグメントの統計情報を返す.
    pub fn stats(
        &self,
        parent: SpanHandle,
    ) -> impl Future<Item = SegmentStatistics, Error = Error> {
        let storage = self.storage.clone();
        let is_metadata = self.storage.is_metadata();
        storage
            .storage_usage(parent)
            .and_then(move |usages| {
                let f = |e: &StorageUsage| -> bool { matches!(e, StorageUsage::Unknown) };
                if !is_metadata && usages.iter().all(f) {
                    Err(ErrorKind::Invalid
                        .cause("All segment-nodes disk-usage unavailable")
                        .into())
                } else {
                    Ok(usages)
                }
            })
            .map(|usages| {
                let max_usage = usages
                    .iter()
                    .map(|usage| {
                        if let StorageUsage::Approximate(n) = usage {
                            *n
                        } else {
                            0
                        }
                    })
                    .max();
                let max_usage = if let Some(max) = max_usage { max } else { 0 };
                let (storage_usage_bytes_real, storage_usage_bytes_approximation) = usages
                    .iter()
                    .fold((0, 0), |(sum_real, sum_approximation), usage| {
                        if let StorageUsage::Approximate(n) = usage {
                            (sum_real + *n, sum_approximation)
                        } else {
                            (sum_real, sum_approximation + max_usage)
                        }
                    });
                SegmentStatistics {
                    storage_usage_bytes_real,
                    storage_usage_bytes_approximation,
                }
            })
    }
}

/// Put がアトミックではないため、ストレージへの保存に失敗した可能性を追跡する。
struct PutFailureTracking {
    logger: Logger,
    /// 追跡対象のオブジェクトID。
    object_id: ObjectId,
    /// 操作が完了したか。
    is_completed: bool,
}

impl PutFailureTracking {
    fn new(logger: Logger, object_id: ObjectId) -> Self {
        Self {
            logger,
            object_id,
            is_completed: false,
        }
    }
    fn complete(&mut self) {
        self.is_completed = true;
    }
}

impl Drop for PutFailureTracking {
    fn drop(&mut self) {
        if !self.is_completed {
            warn!(
                self.logger,
                "A put operation might have failed: object_id={:?}", self.object_id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cannyls_rpc::DeviceId;
    use config::ClusterMember;
    use fibers::executor::Executor;
    use rustracing_jaeger::span::Span;
    use std::{thread, time};
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    #[test]
    // This case reproduce the issue https://github.com/frugalos/frugalos/issues/78 .
    // The issue says that:
    //  For putting an object, when a frugalos server accidentally halts
    //   after MdsClient::put but before finishing StorageClient::put,
    //  we can head the object but cannot get the object.
    fn head_work_but_get_doesnt() -> TestResult {
        let data_fragments = 2;
        let parity_fragments = 1;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let segment_size = system.fragments() as usize;
        let (members, client) = setup_system(&mut system, segment_size)?;
        let object_id = "test_data";
        let expected = vec![0x02];

        thread::spawn(move || loop {
            system.executor.run_once().unwrap();
            thread::sleep(time::Duration::from_micros(100));
        });

        // wait until the segment becomes stable; for example, there is a raft leader.
        // However, 5-secs is an ungrounded value.
        thread::sleep(time::Duration::from_secs(5));

        let (object_version, _) = wait(client.put(
            object_id.to_owned(),
            expected,
            Deadline::Infinity,
            Expect::Any,
            Span::inactive().handle(),
        ))?;

        // Deletes all fragments the dispersed device.
        for (_node_id, _device_id, device_handle) in members.clone() {
            let result = wait(
                device_handle
                    .request()
                    .list()
                    .map_err(|e| track!(Error::from(e))),
            )?;

            for lump_id in result {
                if lump_id.to_string().starts_with("01") {
                    // then, lump_id is for a put data rather than a raft data
                    let _ = wait(
                        device_handle
                            .request()
                            .delete(lump_id)
                            .map_err(|e| track!(Error::from(e))),
                    )?;
                }
            }
        }

        // Heads return `object_version`
        // since it only looks for the <ObjectId, ObjectVersion>-table in the MDS
        // and does not visit the dispersed device.
        let result = wait(client.head(
            object_id.to_owned(),
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ))?;
        assert_eq!(result, Some(object_version));

        // Gets failed since there are no fragments in the dispersed device.
        let result = wait(client.get(
            object_id.to_owned(),
            Deadline::Infinity,
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ));

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn put_delete_and_get_work() -> TestResult {
        let data_fragments = 2;
        let parity_fragments = 1;
        let cluster_size = 3;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (_members, client) = setup_system(&mut system, cluster_size)?;

        thread::spawn(move || loop {
            system.executor.run_once().unwrap();
            thread::sleep(time::Duration::from_micros(100));
        });

        let expected = vec![0x03];
        let object_id = "test_data".to_owned();

        // wait until the segment becomes stable; for example, there is a raft leader.
        // However, 5-secs is an ungrounded value.
        thread::sleep(time::Duration::from_secs(5));

        let _ = wait(client.put(
            object_id.clone(),
            expected.clone(),
            Deadline::Infinity,
            Expect::Any,
            Span::inactive().handle(),
        ))?;

        let data = wait(client.get(
            object_id.clone(),
            Deadline::Infinity,
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ))?;

        assert_eq!(expected, data.unwrap().content);

        let _ = wait(client.delete(
            object_id.clone(),
            Deadline::Infinity,
            Expect::Any,
            Span::inactive().handle(),
        ))?;

        let data = wait(client.get(
            object_id.clone(),
            Deadline::Infinity,
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ))?;

        // 上の object_id の clone を一つだけ消したくないので、ここで drop する
        drop(object_id);

        assert!(data.is_none());

        Ok(())
    }

    #[test]
    fn head_storage_work() -> TestResult {
        let data_fragments = 2;
        let parity_fragments = 1;
        let cluster_size = 3;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (members, client) = setup_system(&mut system, cluster_size)?;
        let rpc_service_handle = system.rpc_service_handle();

        thread::spawn(move || loop {
            system.executor.run_once().unwrap();
            thread::sleep(time::Duration::from_micros(100));
        });

        let expected = vec![0x03];
        let object_id = "test_data".to_owned();

        // wait until the segment becomes stable; for example, there is a raft leader.
        // However, 5-secs is an ungrounded value.
        thread::sleep(time::Duration::from_secs(5));

        let (object_version, _) = wait(client.put(
            object_id.clone(),
            expected,
            Deadline::Infinity,
            Expect::Any,
            Span::inactive().handle(),
        ))?;

        let result = wait(client.head_storage(
            object_id.to_owned(),
            Deadline::Infinity,
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ))?;
        assert_eq!(result, Some(object_version));
        // delete (num of data_fragments) lumps
        for (i, (node_id, device_id, _)) in members.into_iter().enumerate() {
            let client = cannyls_rpc::Client::new(node_id.addr, rpc_service_handle.clone());
            let cluster_member = ClusterMember {
                node: node_id,
                device: device_id.clone(),
            };
            let lump_id = cluster_member.make_lump_id(object_version);
            let request = client.request();
            let future = request
                .delete_lump(DeviceId::new(device_id.clone()), lump_id)
                .map_err(|e| e.into());
            let result = wait(future)?;
            assert_eq!(result, true);
            if i >= usize::from(data_fragments) {
                break;
            }
        }

        // head_storage request will be failed
        let result = wait(client.head(
            object_id.to_owned(),
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ))?;
        assert_eq!(result, Some(object_version));
        let result = wait(client.head_storage(
            object_id,
            Deadline::Infinity,
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn delete_fragment_works() -> TestResult {
        let data_fragments = 2;
        let parity_fragments = 1;
        let cluster_size = 3;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (_members, client) = setup_system(&mut system, cluster_size)?;

        thread::spawn(move || loop {
            system.executor.run_once().unwrap();
            thread::sleep(time::Duration::from_micros(100));
        });

        let blob = vec![0x03];
        let object_id = "test_data".to_owned();

        thread::sleep(time::Duration::from_secs(5));

        let (object_version, _) = wait(client.put(
            object_id.to_owned(),
            blob,
            Deadline::Infinity,
            Expect::Any,
            Span::inactive().handle(),
        ))?;

        for index in 0..3 {
            if let Some((v, Some((b, _, _)))) = wait(client.delete_fragment(
                object_id.to_owned(),
                Deadline::Infinity,
                Span::inactive().handle(),
                index,
            ))? {
                assert_eq!(v, object_version);
                assert_eq!(b, true);
            }

            if let Some((v, Some((b, _, _)))) = wait(client.delete_fragment(
                object_id.to_owned(),
                Deadline::Infinity,
                Span::inactive().handle(),
                index,
            ))? {
                assert_eq!(v, object_version);
                assert_eq!(b, false);
            }
        }

        let result = wait(client.head(
            object_id.to_owned(),
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ))?;
        assert_eq!(result, Some(object_version));

        let result = wait(client.head_storage(
            object_id,
            Deadline::Infinity,
            ReadConsistency::Consistent,
            Span::inactive().handle(),
        ));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn storage_usage_works() -> TestResult {
        let data_fragments = 2;
        let parity_fragments = 1;
        let cluster_size = 3;
        let mut system = System::new(data_fragments, parity_fragments)?;
        let (_members, client) = setup_system(&mut system, cluster_size)?;

        thread::spawn(move || loop {
            system.executor.run_once().unwrap();
            thread::sleep(time::Duration::from_micros(100));
        });

        let blob = vec![0x03];
        let object_id = "test_data".to_owned();

        thread::sleep(time::Duration::from_secs(5));

        let _ = wait(client.put(
            object_id,
            blob,
            Deadline::Infinity,
            Expect::Any,
            Span::inactive().handle(),
        ))?;

        let result = wait(client.stats(Span::inactive().handle()))?;
        assert_eq!(
            result,
            SegmentStatistics {
                storage_usage_bytes_approximation: 0,
                storage_usage_bytes_real: 99840
            }
        );

        Ok(())
    }
}
