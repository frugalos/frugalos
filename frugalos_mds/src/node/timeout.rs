/// 一定値ずつ減少していくタイムアウト値。
#[derive(Debug)]
pub struct CountDownTimeout {
    expired_total: u32,
    threshold: usize,
    remaining: usize,
}
impl CountDownTimeout {
    /// `CountDownTimeout` を生成する。
    pub fn new(threshold: usize) -> Self {
        Self {
            expired_total: 0,
            threshold,
            remaining: threshold,
        }
    }

    /// 連続して期限切れになった回数を返す。
    ///
    /// リセットされた場合は 0 に戻る。
    pub fn expired_total(&self) -> u32 {
        self.expired_total
    }

    /// 残り時間を 1 単位減少させ、0 になったら true を返す。
    pub fn count_down(&mut self) -> bool {
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining == 0 {
            self.expired_total += 1;
            self.remaining = self.threshold;
            true
        } else {
            false
        }
    }

    /// 残り時間を初期値に戻す。
    pub fn reset(&mut self) {
        self.expired_total = 0;
        self.remaining = self.threshold;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_down_timeout_works() {
        let mut timeout = CountDownTimeout::new(3);
        assert!(!timeout.count_down());
        assert!(!timeout.count_down());
        assert!(timeout.count_down());
        assert!(!timeout.count_down());
        timeout.reset();
        assert!(!timeout.count_down());
        assert!(!timeout.count_down());
        assert!(timeout.count_down());
    }
}
