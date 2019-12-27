use futures::{Async, Future, Poll};
use prometrics::metrics::{Gauge, MetricBuilder};
use slog::Logger;

use util::UnitFuture;

/// 実行開始と停止ができる型
pub(crate) trait GcTask {
    fn start(&self) -> UnitFuture;
    fn stop(&self) -> UnitFuture;
}

/// segment_gc の実行状況の管理を行う。
/// これは SegmentService に管理され、SegmentService の指示によって segment_gc を開始したりする。
/// Task は SegmentHandle のラッパを入れることを想定された型変数である。テストのしやすさのために多相型にしている。
pub(crate) struct SegmentGcManager<Task> {
    logger: Logger,
    segment_gc_running: Gauge,
    segment_gc_waiting: Gauge,
    segment_gc_stopping: Gauge,
    tasks: Vec<Task>,
    running: Vec<(usize, UnitFuture)>,  // 実行中のタスクの番号の列
    waiting: Vec<usize>,                // 実行待ちのタスクの番号の列
    stopping: Vec<(usize, UnitFuture)>, // 停止中のタスクの番号の列
    limit: usize,                       // 実行可能なタスク数の上限
}

impl<Task: GcTask> SegmentGcManager<Task> {
    pub(crate) fn new(logger: Logger) -> Self {
        let metric_builder = MetricBuilder::new()
            .namespace("frugalos")
            .subsystem("segment_gc_manager")
            .clone();
        let segment_gc_running = metric_builder
            .gauge("running_task_total")
            .finish()
            .expect("metric should be well-formed");
        let segment_gc_waiting = metric_builder
            .gauge("waiting_task_total")
            .finish()
            .expect("metric should be well-formed");
        let segment_gc_stopping = metric_builder
            .gauge("stopping_task_total")
            .finish()
            .expect("metric should be well-formed");
        Self {
            logger,
            segment_gc_running,
            segment_gc_waiting,
            segment_gc_stopping,
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
        info!(self.logger, "set_limit: {} -> {}", self.limit, limit);
        self.limit = limit;
        while self.running.len() > limit {
            let (index, _) = self.running.pop().unwrap();
            let fut = self.tasks[index].stop();
            self.stopping.push((index, fut));
        }
    }
}

impl<Task: GcTask> Future for SegmentGcManager<Task> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut new_running = Vec::new();
        let old_count = (self.running.len(), self.waiting.len(), self.stopping.len());
        // running -> discarded
        for (index, mut fut) in std::mem::replace(&mut self.running, Vec::new()) {
            match fut.poll() {
                Ok(Async::NotReady) => new_running.push((index, fut)),
                Ok(Async::Ready(())) => (),
                Err(e) => {
                    // segment_gc に失敗したからといって致命的な何かが起こるわけではないので、ログだけ吐いて握り潰す。
                    warn!(
                        self.logger,
                        "Error in executing a task: index = {}, error = {:?}", index, e
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
                    // segment_gc の中止に失敗したからといって致命的な何かが起こるわけではないので、ログだけ吐いて握り潰す。
                    warn!(
                        self.logger,
                        "Error in stopping a task: index = {}, error = {:?}", index, e
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
        // Update metrics
        self.segment_gc_running.set(self.running.len() as f64);
        self.segment_gc_waiting.set(self.waiting.len() as f64);
        self.segment_gc_stopping.set(self.stopping.len() as f64);

        // 全ての仕事が終わったかどうか確認する。
        if self.running.is_empty() && self.waiting.is_empty() && self.stopping.is_empty() {
            return Ok(Async::Ready(()));
        }
        let new_count = (self.running.len(), self.waiting.len(), self.stopping.len());
        if old_count != new_count {
            // 進捗があったのでログを出す
            let (running, waiting, stopping) = new_count;
            debug!(
                self.logger,
                "remaining tasks: {} (running: {}, waiting: {}, stopping: {})",
                running + waiting + stopping,
                running,
                waiting,
                stopping,
            );
        }
        Ok(Async::NotReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibers::time::timer::timeout;
    use futures::future::{ok, Either, Loop};
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct IncrementToggle(Arc<AtomicI32>);

    impl GcTask for IncrementToggle {
        fn start(&self) -> UnitFuture {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::new(ok(()))
        }
        fn stop(&self) -> UnitFuture {
            Box::new(ok(()))
        }
    }

    /// self.0 で見ているカウンタが self.1 以上になるまで待つ。
    struct WaitingToggle(Arc<AtomicI32>, i32, Arc<AtomicI32>);

    impl GcTask for WaitingToggle {
        fn start(&self) -> UnitFuture {
            let future =
                futures::future::loop_fn((Arc::clone(&self.0), self.1), |(counter, limit)| {
                    if counter.load(Ordering::SeqCst) < limit {
                        Either::A(
                            timeout(Duration::from_millis(1))
                                .map(move |_| Loop::Continue((counter, limit))),
                        )
                    } else {
                        Either::B(ok(Loop::Break(())))
                    }
                });
            Box::new(future.map_err(Into::into))
        }

        fn stop(&self) -> UnitFuture {
            // Do nothing
            self.2.fetch_add(1, Ordering::SeqCst);
            Box::new(ok(()))
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

    #[test]
    fn stop_works() -> std::io::Result<()> {
        let logger = Logger::root(slog::Discard, o!());
        let mut manager = SegmentGcManager::new(logger);
        let count = 13;
        let stop_count = 6; // tasks[6] まで終わらせたら中断する
        let counter = Arc::new(AtomicI32::new(0));
        let stop_counter = Arc::new(AtomicI32::new(0));
        let mut tasks = Vec::new();
        for index in 0..count {
            tasks.push(WaitingToggle(
                Arc::clone(&counter),
                index,
                Arc::clone(&stop_counter),
            ));
        }
        manager.init(tasks);
        manager.set_limit(3);
        let manager = Arc::new(Mutex::new(manager));
        let manager_cloned = Arc::clone(&manager);
        // manager は別スレッドで走らせる
        let manager_thread = std::thread::spawn(move || {
            while let Ok(Async::NotReady) = manager_cloned.lock().unwrap().poll() {}
        });
        for _ in 0..stop_count {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        // 3 並列実行になるまで待つ
        while manager.lock().unwrap().running.len() != 3 {}
        manager.lock().unwrap().set_limit(1);
        // ジョブが 2 個 キャンセルされる。停止要求の送信は set_limit が返ってくる時には終わっている。
        assert_eq!(stop_counter.load(Ordering::SeqCst), 2);
        // 全ジョブを終了する
        counter.store(count - 1, Ordering::SeqCst);
        manager_thread.join().unwrap();
        Ok(())
    }
}
