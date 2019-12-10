use futures::{Async, Future, Poll};
use slog::Logger;
use std::error::Error;

pub(crate) type UnitFuture =
    Box<dyn Future<Item = (), Error = Box<dyn Error + Send + 'static>> + Send + 'static>;

/// 実行開始と停止ができる型
pub(crate) trait Toggle {
    fn start(&self) -> UnitFuture;
    fn stop(&self) -> UnitFuture;
}

/// segment_gc の実行状況の管理を行う。
/// これは SegmentService に管理され、SegmentService の指示によって segment_gc を開始したりする。
/// Task は SegmentHandle のラッパを入れることを想定された型変数である。テストのしやすさのために多相型にしている。
pub(crate) struct SegmentGcManager<Task> {
    logger: Logger,
    tasks: Vec<Task>,
    running: Vec<(usize, UnitFuture)>,  // 実行中のタスクの番号の列
    waiting: Vec<usize>,                // 実行待ちのタスクの番号の列
    stopping: Vec<(usize, UnitFuture)>, // 停止中のタスクの番号の列
    limit: usize,                       // 実行可能なタスク数の上限
}

impl<Task: Toggle> SegmentGcManager<Task> {
    pub(crate) fn new(logger: Logger) -> Self {
        Self {
            logger,
            tasks: Vec::new(),
            running: Vec::new(),
            waiting: Vec::new(),
            stopping: Vec::new(),
            limit: 0,
        }
    }
    /// 初期化する。今まで覚えていたデータは失われる。
    /// また今まで実行していたタスクも中断される。
    /// タスクの実行中 (new してから set_limit をして、poll を始めた後) に呼んではいけない。
    pub(crate) fn init(&mut self, tasks: impl IntoIterator<Item = Task>) {
        // タスクがどれも実行されていないという想定なので、単純に全て捨てる。
        self.tasks = tasks.into_iter().collect();
        self.running = Vec::new();
        self.waiting = (0..self.tasks.len()).collect();
        self.stopping = Vec::new();
        self.limit = 0;
    }

    /// 実行数のリミットを設定する。すでに実行中のタスクがある場合、リミットに収まらない分は中断される。
    pub(crate) fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
        while self.running.len() > limit {
            let (index, _) = self.running.pop().unwrap();
            let fut = self.tasks[index].stop();
            self.stopping.push((index, fut));
        }
    }
}

impl<Task: Toggle> Future for SegmentGcManager<Task> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut new_running = Vec::new();
        // running -> discarded
        for (index, mut fut) in std::mem::replace(&mut self.running, Vec::new()) {
            match fut.poll() {
                Ok(Async::NotReady) => new_running.push((index, fut)),
                Ok(Async::Ready(())) => (),
                Err(e) => {
                    // ログだけ吐いて握り潰す
                    warn!(
                        self.logger,
                        "Error executing running: index = {}, error = {:?}", index, e
                    );
                }
            }
        }
        self.running = new_running;
        // stopping -> waiting
        let mut new_stopping = Vec::new();
        for (index, mut fut) in std::mem::replace(&mut self.stopping, Vec::new()) {
            match fut.poll() {
                Ok(Async::NotReady) => new_stopping.push((index, fut)),
                Ok(Async::Ready(())) => self.waiting.push(index),
                Err(e) => {
                    // ログだけ吐いて握り潰す
                    warn!(
                        self.logger,
                        "Error in executing a task: index = {}, error = {:?}", index, e
                    );
                }
            }
        }
        self.stopping = new_stopping;
        // fill
        while self.running.len() < self.limit {
            if let Some(index) = self.waiting.pop() {
                // start index
                self.running.push((index, self.tasks[index].start()));
            } else {
                break;
            }
        }
        // 全ての仕事が終わったかどうか確認する。
        if self.running.is_empty() && self.waiting.is_empty() && self.stopping.is_empty() {
            return Ok(Async::Ready(()));
        }
        Ok(Async::NotReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    struct IncrementToggle(Arc<AtomicI32>);

    impl Toggle for IncrementToggle {
        fn start(&self) -> UnitFuture {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::new(futures::future::ok(()))
        }
        fn stop(&self) -> UnitFuture {
            Box::new(futures::future::ok(()))
        }
    }

    #[test]
    fn all_tasks_done() {
        let count = 13;
        let counter = Arc::new(AtomicI32::new(0));
        let mut tasks = Vec::new();
        for _ in 0..count {
            tasks.push(IncrementToggle(Arc::clone(&counter)));
        }
        let logger = Logger::root(slog::Discard, o!());
        let mut manager = SegmentGcManager::new(logger);
        manager.init(tasks);
        manager.set_limit(1);
        while let Ok(Async::NotReady) = manager.poll() {}
        assert_eq!(counter.load(Ordering::SeqCst), count);
    }

    #[test]
    fn no_tasks_will_be_done_if_limit_is_zero() {
        let count = 13;
        let counter = Arc::new(AtomicI32::new(0));
        let mut tasks = Vec::new();
        for _ in 0..count {
            tasks.push(IncrementToggle(Arc::clone(&counter)));
        }
        let logger = Logger::root(slog::Discard, o!());
        let mut manager = SegmentGcManager::new(logger);
        manager.init(tasks);
        manager.set_limit(0);
        // 十分な回数 poll しても NotReady のまま
        for _ in 0..1000 {
            assert_eq!(Ok(Async::NotReady), manager.poll())
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }
}
