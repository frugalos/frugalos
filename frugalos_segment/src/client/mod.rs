use cannyls::deadline::Deadline;
use fibers_rpc::client::ClientServiceHandle as RpcServiceHandle;
use futures::future::Either;
use futures::{self, Future};
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use rustracing_jaeger::span::SpanHandle;
use slog::Logger;
use std::mem;
use std::ops::Range;

use self::mds::MdsClient;
use self::storage::{ErasureCoder, StorageClient};
use config::ClientConfig;
use {Error, ObjectValue};

mod mds;
pub mod storage; // TODO: private

/// セグメントにアクセスるために使用するクライアント。
#[derive(Clone)]
pub struct Client {
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
    ) -> Self {
        let mds = MdsClient::new(
            logger.clone(),
            rpc_service.clone(),
            config.cluster.clone(),
            config.mds.clone(),
        );
        let storage = StorageClient::new(logger, config, rpc_service, ec);
        Client { mds, storage }
    }

    /// オブジェクトを取得する。
    pub fn get(
        &self,
        id: ObjectId,
        deadline: Deadline,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectValue>, Error = Error> {
        let storage = self.storage.clone();
        self.mds.get(id, parent.clone()).and_then(move |object| {
            if let Some(object) = object {
                let version = object.version;
                let future = storage
                    .get(object, deadline, parent)
                    .map(move |content| ObjectValue { version, content })
                    .map(Some);
                Either::A(future)
            } else {
                Either::B(futures::finished(None))
            }
        })
    }

    /// オブジェクトの存在確認を行う。
    pub fn head(
        &self,
        id: ObjectId,
        parent: SpanHandle,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        self.mds.head(id, parent)
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
        self.mds
            .put(id, metadata, expect, deadline, parent.clone())
            .and_then(move |(version, created)| {
                storage
                    .put(version, content, deadline, parent)
                    .map(move |()| (version, created))
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
        self.mds.delete(id, expect, parent)
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

    /// 保存済みのオブジェクト一覧を取得する。
    pub fn list(&self) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        self.mds.list()
    }

    /// セグメント内の最新オブジェクトのバージョンを取得する。
    pub fn latest(&self) -> impl Future<Item = Option<ObjectSummary>, Error = Error> {
        self.mds.latest()
    }

    /// セグメント内に保持されているオブジェクトの数を返す.
    pub fn object_count(&self) -> impl Future<Item = u64, Error = Error> {
        self.mds.object_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibers::executor::Executor;
    use rustracing_jaeger::span::Span;
    use std::{thread, time};
    use test_util::tests::{setup_system, wait, System};
    use trackable::result::TestResult;

    #[test]
    fn put_delete_and_get_work() -> TestResult {
        let fragments = 3;
        let cluster_size = 3;
        let mut system = System::new(fragments, 1)?;
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
            Span::inactive().handle(),
        ))?;

        assert!(data.is_none());

        Ok(())
    }
}
