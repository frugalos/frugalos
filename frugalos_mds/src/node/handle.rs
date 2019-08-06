use fibers::sync::{mpsc, oneshot};
use futures::future::Either;
use futures::{self, Future};
use libfrugalos::entity::node::RemoteNodeId;
use libfrugalos::entity::object::{
    DeleteObjectsByPrefixSummary, Metadata, ObjectId, ObjectPrefix, ObjectSummary, ObjectVersion,
};
use libfrugalos::expect::Expect;
use libfrugalos::time::Seconds;
use std::ops::Range;
use std::time::Instant;

use super::Request;
use Error;

macro_rules! future_try {
    ($e:expr) => {
        match $e {
            Err(e) => return Either::B(::futures::failed(track!(Error::from(e)))),
            Ok(v) => v,
        }
    };
}

#[derive(Debug, Clone)]
pub struct NodeHandle {
    request_tx: mpsc::Sender<Request>,
}
impl NodeHandle {
    pub(super) fn new(request_tx: mpsc::Sender<Request>) -> Self {
        NodeHandle { request_tx }
    }
    pub fn stop(&self) {
        let _ = self.request_tx.send(Request::Stop);
    }
    pub fn take_snapshot(&self) {
        let _ = self.request_tx.send(Request::TakeSnapshot);
    }
    pub fn start_reelection(&self) {
        let _ = self.request_tx.send(Request::StartElection);
    }
    pub fn get_leader(
        &self,
        started_at: Instant,
    ) -> impl Future<Item = RemoteNodeId, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::GetLeader(started_at, monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor
            .map(|node| (node.addr, node.local_id.to_string()))
            .map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }
    pub fn list_objects(&self) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::List(monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn latest_version(&self) -> impl Future<Item = Option<ObjectSummary>, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::LatestVersion(monitored);

        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn object_count(&self) -> impl Future<Item = u64, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::ObjectCount(monitored);

        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn get_object(
        &self,
        object_id: ObjectId,
        expect: Expect,
        started_at: Instant,
    ) -> impl Future<Item = Option<Metadata>, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::Get(object_id, expect, started_at, monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn head_object(
        &self,
        object_id: ObjectId,
        expect: Expect,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::Head(object_id, expect, monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn delete_object(
        &self,
        object_id: ObjectId,
        expect: Expect,
        started_at: Instant,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::Delete(object_id, expect, started_at, monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn delete_version(
        &self,
        object_version: ObjectVersion,
    ) -> impl Future<Item = Option<ObjectVersion>, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::DeleteByVersion(object_version, monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn delete_by_range(
        &self,
        targets: Range<ObjectVersion>,
    ) -> impl Future<Item = Vec<ObjectSummary>, Error = Error> {
        let mut futures = Vec::new();

        // FIX: for v in targets { ... } で書けるようにしたい
        for v in (targets.start.0)..(targets.end.0) {
            let (monitored, monitor) = oneshot::monitor();
            let request = Request::DeleteByVersion(ObjectVersion(v), monitored);

            let send = self.request_tx.send(request);
            if send.is_ok() {
                futures.push(monitor.map_err(|e| track!(Error::from(e))));
            }
        }

        let joined_future = futures::future::join_all(futures);

        joined_future.map(|vec| {
            vec.into_iter()
                .filter_map(|e| {
                    e.map(|v| ObjectSummary {
                        // DeleteByVersionはVersionのみを返すので
                        // ここではdummy値でpaddingしている
                        id: "dummy".to_string(),
                        version: v,
                    })
                })
                .collect::<Vec<_>>()
        })

        /*
           // 本当は以下のようにしたいが
           // node.rs の handle_command の return type の制約
           // (Vec型ではなくOption型を返す)
           // のため、現状は上のように個別リクエストをまとめあげている
           let (monitored, monitor) = oneshot::monitor();
           let request = Request::DeleteByRange(version_from, version_to, expect.clone(), monitored);
           future_try!(self.request_tx.send(request));
           let future = monitor.map_err(|e| track!(Error::from(e)));
           Either::A(future)
        */
    }

    pub fn delete_by_prefix(
        &self,
        prefix: ObjectPrefix,
    ) -> impl Future<Item = DeleteObjectsByPrefixSummary, Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::DeleteByPrefix(prefix, monitored);
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }

    pub fn put_object(
        &self,
        object_id: ObjectId,
        body: Vec<u8>,
        expect: Expect,
        put_content_timeout: Seconds,
        started_at: Instant,
    ) -> impl Future<Item = (ObjectVersion, Option<ObjectVersion>), Error = Error> {
        let (monitored, monitor) = oneshot::monitor();
        let request = Request::Put(
            object_id,
            body,
            expect,
            put_content_timeout,
            started_at,
            monitored,
        );
        future_try!(self.request_tx.send(request));
        let future = monitor.map_err(|e| track!(Error::from(e)));
        Either::A(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibers::sync::mpsc;
    use fibers::sync::mpsc::Receiver;
    use fibers_global;
    use futures::{Async, Future, Stream};
    use std::thread;
    use std::time::Duration;
    use trackable::result::TestResult;

    fn make_handle() -> (NodeHandle, Receiver<Request>) {
        let (tx, tr) = mpsc::channel();
        (NodeHandle::new(tx), tr)
    }

    #[test]
    fn it_deletes_objects_by_prefix() -> TestResult {
        let (handle, mut receiver) = make_handle();

        fibers_global::spawn(
            handle
                .delete_by_prefix(ObjectPrefix("chunk".to_owned()))
                .and_then(move |summary| {
                    assert_eq!(summary.total, 3);
                    handle.stop();
                    Ok(())
                })
                .map_err(|e| {
                    let _ = track!(e);
                }),
        );

        loop {
            while let Async::Ready(Some(request)) = receiver.poll().unwrap() {
                match request {
                    Request::DeleteByPrefix(prefix, monitored) => {
                        assert_eq!(prefix, ObjectPrefix("chunk".to_owned()));
                        monitored.exit(Ok(DeleteObjectsByPrefixSummary { total: 3 }));
                    }
                    Request::Stop => return Ok(()),
                    _ => (),
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
