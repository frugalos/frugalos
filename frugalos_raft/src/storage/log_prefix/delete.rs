use cannyls::deadline::Deadline;
use futures::{Async, Future, Poll};
use raftlog::log::LogIndex;
use raftlog::{Error, ErrorKind, Result};
use std::ops::Range;

use super::super::{into_box_future, BoxFuture, Handle};

// #[derive(Debug)]
pub(crate) struct DeleteOldLogPrefixBytes {
    handle: Handle,
    old_index: Range<u64>,
    future: Option<BoxFuture<bool>>,
}
impl DeleteOldLogPrefixBytes {
    pub(crate) fn new(handle: Handle, old_index: Range<u64>) -> Self {
        info!(
            handle.logger,
            "[START] DeleteOldLogPrefixBytes: {}",
            dump!(old_index)
        );
        DeleteOldLogPrefixBytes {
            handle,
            old_index,
            future: None,
        }
    }
}
impl Future for DeleteOldLogPrefixBytes {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // FIXME:
        // 削除途中でプログラムが強制終了したらゴミが残ってしまうので、
        // 範囲削除で`0..現在`の範囲を一括削除するようにする
        while let Async::Ready(_) = track!(self.future.poll())? {
            if self.old_index.start == self.old_index.end {
                info!(self.handle.logger, "[FINISH] DeleteOldLogPrefixBytes");
                return Ok(Async::Ready(()));
            }
            let index = self.old_index.start;
            self.old_index.start += 1;
            let lump_id = self.handle.node_id.to_log_prefix_lump_id(index);
            info!(
                self.handle.logger,
                "[PROGRESS] DeleteOldLogPrefixBytes: {}",
                dump!(index, lump_id)
            );
            let future = self
                .handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .delete(lump_id);
            self.future = Some(into_box_future(future));
        }
        Ok(Async::NotReady)
    }
}

// #[derive(Debug)]
pub(crate) struct DeleteOldLogEntries {
    handle: Handle,
    future: BoxFuture<()>,
}
impl DeleteOldLogEntries {
    pub(crate) fn new(handle: Handle, old_entries: Range<LogIndex>) -> Result<Self> {
        track_assert!(
            old_entries.start <= old_entries.end,
            ErrorKind::InconsistentState,
            "The installed snapshot is too old: {}",
            dump!(old_entries)
        );
        info!(
            handle.logger,
            "[START] DeleteOldLogEntries: {}",
            dump!(old_entries)
        );
        let future = Self::delete(&handle, &old_entries);
        Ok(DeleteOldLogEntries { handle, future })
    }

    fn delete(handle: &Handle, old_entries: &Range<LogIndex>) -> BoxFuture<()> {
        // 削除し漏れた古いエントリも削除するために 0 から削除する。
        let deleted_range = Range {
            start: handle.node_id.to_log_entry_lump_id(LogIndex::new(0)),
            end: handle.node_id.to_log_entry_lump_id(old_entries.end),
        };
        into_box_future(
            handle
                .device
                .request()
                .deadline(Deadline::Infinity)
                .delete_range(deleted_range)
                .map(|_| ()), // 結果は不要なので捨てる
        )
    }
}
impl Future for DeleteOldLogEntries {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // handle をスレッドを跨がせるのは面倒なのでログは poll() 内で出す
        track!(
            self.future
                .poll()
                .map(|result| result
                    .map(|_| info!(self.handle.logger, "[FINISH] DeleteOldLogEntries")))
        )
    }
}

#[cfg(test)]
mod tests {
    use cannyls::device::DeviceHandle;
    use cannyls::lump::LumpData;
    use cannyls::Error;
    use futures;
    use std::ops::Range;
    use test_util;
    use test_util::{NodeIndex, System};
    use trackable::result::TestResult;

    use super::*;

    const NODE_SIZE: usize = 3;

    fn new_system() -> System {
        let mut system = track!(System::new()).expect("never fails");
        system.boot(NODE_SIZE).unwrap();
        system
    }

    fn get_by_range(
        system: &System,
        device_handle: &DeviceHandle,
        leader: NodeIndex,
        range: Range<u64>,
    ) -> impl Future<Item = Vec<Option<LumpData>>, Error = Error> {
        futures::future::join_all(
            range
                .map(|i| {
                    let lump_id = system
                        .to_log_entry_lump_id(leader, LogIndex::new(i.to_owned()))
                        .unwrap();
                    device_handle
                        .request()
                        .deadline(Deadline::Infinity)
                        .get(lump_id)
                })
                .collect::<Vec<_>>(),
        )
    }

    fn run_test<F>(proposal_size: usize, f: F) -> TestResult
    where
        F: FnOnce((System, NodeIndex)) -> TestResult,
    {
        let mut system = new_system();
        let leader = system.select_leader()?;
        let proposals = test_util::make_proposals(proposal_size);
        system.bulk_propose(leader, proposals)?;

        f((system, leader))
    }

    #[test]
    fn it_doesnt_fail_even_if_there_is_no_log() -> TestResult {
        run_test(0, |(system, leader)| {
            let _ = test_util::wait_for(DeleteOldLogEntries::new(
                system.get_handle(leader).unwrap(),
                Range {
                    start: LogIndex::new(0),
                    end: LogIndex::new(3),
                },
            )?);

            Ok(())
        })
    }

    #[test]
    fn it_deletes_only_specified_range() -> TestResult {
        run_test(20, |(system, leader)| {
            let device_handle = system.get_device_handle(leader).unwrap();
            // 0 ~ 9 は消す。
            let _ = test_util::wait_for(DeleteOldLogEntries::new(
                system.get_handle(leader).unwrap(),
                Range {
                    start: LogIndex::new(0),
                    end: LogIndex::new(10),
                },
            )?);

            let should_be_dead =
                test_util::wait_for(get_by_range(&system, &device_handle, leader, 0..10))?;

            assert!(should_be_dead.iter().all(|r| r.is_none()));

            let should_be_alive =
                test_util::wait_for(get_by_range(&system, &device_handle, leader, 10..20))?;

            assert!(should_be_alive.iter().all(|r| r.is_some()));

            Ok(())
        })
    }

    #[test]
    fn it_doesnt_delete_end_of_range() -> TestResult {
        run_test(10, |(system, leader)| {
            let device_handle = system.get_device_handle(leader).unwrap();
            // 0 ~ 9 は消す。10 は残る。
            let _ = test_util::wait_for(DeleteOldLogEntries::new(
                system.get_handle(leader).unwrap(),
                Range {
                    start: LogIndex::new(0),
                    end: LogIndex::new(10),
                },
            )?);

            let should_be_alive =
                test_util::wait_for(get_by_range(&system, &device_handle, leader, 10..11))?;

            assert!(should_be_alive.iter().all(|r| r.is_some()));

            Ok(())
        })
    }

    #[test]
    fn it_deletes_from_index_0() -> TestResult {
        run_test(9, |(system, leader)| {
            let device_handle = system.get_device_handle(leader).unwrap();

            // 0 ~ 4 は消す対象には含めない(が消される)
            let _ = test_util::wait_for(DeleteOldLogEntries::new(
                system.get_handle(leader).unwrap(),
                Range {
                    start: LogIndex::new(5),
                    end: LogIndex::new(10),
                },
            )?);

            let should_be_dead =
                test_util::wait_for(get_by_range(&system, &device_handle, leader, 0..10))?;

            assert!(should_be_dead.iter().all(|r| r.is_none()));

            Ok(())
        })
    }
}
