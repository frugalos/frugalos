use cannyls::deadline::Deadline;
use cannyls::device::DeviceHandle;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll};
use libfrugalos::entity::object::ObjectVersion;
use slog::Logger;

use crate::util::{into_box_future, BoxFuture};
use crate::{config, Error};

// #[derive(Debug)]
pub(crate) struct DeleteContent {
    futures: Vec<BoxFuture<bool>>,
}
impl DeleteContent {
    pub fn new(
        logger: &Logger,
        device: &DeviceHandle,
        node_id: NodeId,
        versions: Vec<ObjectVersion>,
    ) -> Self {
        debug!(logger, "Starts deleting contents: versions={:?}", versions);

        let futures = versions
            .into_iter()
            .map(move |v| {
                let lump_id = config::make_lump_id(&node_id, v);
                let future = device
                    .request()
                    .deadline(Deadline::Infinity)
                    .delete(lump_id);
                into_box_future(future)
            })
            .collect();
        DeleteContent { futures }
    }
}
impl Future for DeleteContent {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut i = 0;
        while i < self.futures.len() {
            // NOTE: 一つ失敗しても全てを失敗扱いにする必要はない
            if let Async::Ready(_) = track!(self.futures[i].poll().map_err(Error::from))? {
                self.futures.swap_remove(i);
            } else {
                i += 1;
            }
        }
        if self.futures.is_empty() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}
