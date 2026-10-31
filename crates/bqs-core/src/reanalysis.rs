use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bqs_config::Config;

use crate::{ports::Clock, ports::Ports};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingReanalysis {
    pub variant: String,
    pub mcts_sims: i32,

    pub payload: Vec<u8>,

    pub positions: u32,
    pub requester: String,
    pub created_at: i64,
}

#[derive(Default)]
struct Inner {
    items: VecDeque<PendingReanalysis>,

    since_selfplay: i64,
}

pub struct ReanaQueue {
    inner: Mutex<Inner>,
    cfg: Arc<Config>,
    #[allow(dead_code)]
    clock: Arc<dyn Clock>,
}

impl ReanaQueue {
    pub fn new(_ports: &Ports, cfg: Arc<Config>, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            cfg,
            clock,
        }
    }

    pub fn enabled(&self) -> bool {
        self.cfg.reanalysis.interval_tasks > 0
    }

    pub fn enqueue(&self, item: PendingReanalysis) -> (u64, bool) {
        let mut inner = self.inner.lock().unwrap();
        if self.cfg.reanalysis.max_queue > 0
            && inner.items.len() as i64 >= self.cfg.reanalysis.max_queue
        {
            return (Self::pending_locked(&inner), false);
        }
        inner.items.push_back(item);
        (Self::pending_locked(&inner), true)
    }

    pub fn peek(&self) -> Option<PendingReanalysis> {
        let inner = self.inner.lock().unwrap();
        if inner.items.is_empty() || inner.since_selfplay < self.cfg.reanalysis.interval_tasks {
            return None;
        }
        inner.items.front().cloned()
    }

    pub fn commit(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.items.pop_front();
        inner.since_selfplay = 0;
    }

    pub fn note_selfplay_assigned(&self) {
        self.inner.lock().unwrap().since_selfplay += 1;
    }

    fn pending_locked(inner: &Inner) -> u64 {
        inner.items.iter().map(|it| it.positions as u64).sum()
    }

    pub fn pending(&self) -> (u64, usize) {
        let inner = self.inner.lock().unwrap();
        (Self::pending_locked(&inner), inner.items.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeClock, Fakes};

    fn queue(raw: &str) -> ReanaQueue {
        let f = Fakes::new();
        ReanaQueue::new(
            &f.ports(),
            Arc::new(Config::parse(raw, "test").expect("测试配置解析失败")),
            FakeClock::new(0),
        )
    }

    fn item(positions: u32) -> PendingReanalysis {
        PendingReanalysis {
            variant: "4x8".into(),
            mcts_sims: 0,
            payload: vec![1],
            positions,
            requester: "trainer".into(),
            created_at: 0,
        }
    }

    #[test]
    fn enabled_by_interval_tasks() {
        assert!(
            !queue("").enabled(),
            "interval_tasks = 0 关闭（SubmitReanalysis 直接拒绝）"
        );
        assert!(queue("[reanalysis]\ninterval_tasks = 2\n").enabled());
    }

    #[test]
    fn enqueue_rejects_when_full_without_dropping() {
        let q = queue("[reanalysis]\ninterval_tasks = 1\nmax_queue = 2\n");
        assert_eq!(q.enqueue(item(3)), (3, true));
        assert_eq!(q.enqueue(item(4)), (7, true));

        let (pos, ok) = q.enqueue(item(5));
        assert!(!ok);
        assert_eq!(pos, 7);
        assert_eq!(q.pending(), (7, 2));
    }

    #[test]
    fn max_queue_zero_means_unlimited() {
        let q = queue("[reanalysis]\ninterval_tasks = 1\nmax_queue = 0\n");
        for i in 0..10 {
            assert!(q.enqueue(item(i as u32)).1);
        }
        assert_eq!(q.pending().1, 10);
    }

    #[test]
    fn peek_respects_throttle_and_does_not_consume() {
        let q = queue("[reanalysis]\ninterval_tasks = 2\n");
        q.enqueue(item(1));
        q.enqueue(item(2));

        assert!(q.peek().is_none());
        q.note_selfplay_assigned();
        assert!(q.peek().is_none(), "1/2 不足间隔");
        q.note_selfplay_assigned();
        let first = q.peek().unwrap();
        assert_eq!(first.positions, 1, "FIFO 取队首");

        assert_eq!(q.peek().unwrap().positions, 1);
        assert_eq!(q.pending(), (3, 2), "peek 不消费队首");
    }

    #[test]
    fn commit_consumes_and_resets_throttle() {
        let q = queue("[reanalysis]\ninterval_tasks = 2\n");
        q.enqueue(item(1));
        q.enqueue(item(2));
        q.note_selfplay_assigned();
        q.note_selfplay_assigned();
        assert!(q.peek().is_some());
        q.commit();
        assert_eq!(q.pending(), (2, 1), "commit 消费队首");

        assert!(q.peek().is_none(), "commit 重置节流计数");
        q.note_selfplay_assigned();
        q.note_selfplay_assigned();
        assert_eq!(q.peek().unwrap().positions, 2);
    }

    #[test]
    fn empty_queue_peek_is_none() {
        let q = queue("[reanalysis]\ninterval_tasks = 1\n");
        q.note_selfplay_assigned();
        assert!(q.peek().is_none());
        q.commit();
        assert_eq!(q.pending(), (0, 0));
    }
}
