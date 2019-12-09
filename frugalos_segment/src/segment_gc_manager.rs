/// 実行開始と実行中かどうかの確認ができる型
pub(crate) trait Toggle {
    fn is_running(&self) -> bool;
    fn start(&self);
    fn stop(&self);
}

/// segment_gc の実行状況の管理を行う。
/// これは SegmentService に管理され、SegmentService の指示によって segment_gc を開始したりする。
/// Task は SegmentHandle のラッパを入れることを想定された型変数である。テストのしやすさのために多相型にしている。
pub(crate) struct SegmentGcManager<Task> {
    tasks: Vec<Task>,
    running: Vec<usize>, // 実行中のタスクの番号の列
    waiting: Vec<usize>, // 実行待ちのタスクの番号の列
    limit: usize,        // 実行可能なタスク数の上限
}

impl<Task: Toggle> SegmentGcManager<Task> {
    pub(crate) fn new() -> Self {
        Self {
            tasks: Vec::new(),
            running: Vec::new(),
            waiting: Vec::new(),
            limit: 0,
        }
    }
    /// 初期化する。今まで覚えていたデータは失われる。
    /// また今まで実行していたタスクも中断される。
    pub(crate) fn init(&mut self, tasks: impl IntoIterator<Item = Task>) {
        // Abort all tasks that were running
        for &index in &self.running {
            self.tasks[index].stop();
        }
        self.tasks = tasks.into_iter().collect();
        self.running = Vec::new();
        self.waiting = (0..self.tasks.len()).collect();
    }

    /// 実行数のリミットを設定する。すでに実行中のタスクがある場合、リミットに収まらない分は中断される。
    pub(crate) fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
        while self.running.len() > limit {
            let index = self.running.pop().unwrap();
            self.tasks[index].stop();
            self.waiting.push(index);
        }
    }

    /// 全タスクが終わったかどうかチェックする
    pub(crate) fn is_done(&mut self) -> bool {
        self.update_info();
        self.running.is_empty() && self.waiting.is_empty()
    }

    /// この manager が持つ情報を最新にする。その結果 segment_gc の開始が必要そうならそうする。
    fn update_info(&mut self) {
        let new_running = self
            .running
            .iter()
            .filter(|&&index| self.tasks[index].is_running())
            .cloned()
            .collect();
        self.running = new_running;
        // Fill in self.running with waiting tasks
        while self.running.len() < self.limit {
            if let Some(index) = self.waiting.pop() {
                self.tasks[index].start();
                self.running.push(index);
            } else {
                // No waiting tasks
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    struct IncrementToggle(Arc<AtomicI32>);

    impl Toggle for IncrementToggle {
        fn is_running(&self) -> bool {
            false
        }
        fn start(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn stop(&self) {}
    }

    #[test]
    fn all_tasks_done() {
        let count = 13;
        let counter = Arc::new(AtomicI32::new(0));
        let mut tasks = Vec::new();
        for _ in 0..count {
            tasks.push(IncrementToggle(Arc::clone(&counter)));
        }
        let mut manager = SegmentGcManager::new();
        manager.init(tasks);
        manager.set_limit(1);
        while !manager.is_done() {}
        assert_eq!(counter.load(Ordering::SeqCst), count);
    }
}
