//! 【所有权：core-pipeline 成员】
//!
//! ReanaQueue：局面重搜队列（FIFO + 间隔节流）。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/reanalysis.go`。要点：
//! - `SubmitReanalysis` 拒绝：未启用 / 变体不符 / 载荷为空 / 队列满（不丢已有条目，
//!   最旧的位置恰恰重搜收益最大，故不按 FIFO 丢弃）；
//! - GetTask 在 rating 之后、selfplay 之前按「每 `interval_tasks` 个 selfplay 任务
//!   最多 1 个重搜」节流取一条；
//! - peek / commit 分离：组装失败不消费队首，下次重试（不丢 trainer 数据）；
//! - 队列跨重启不保留（Go 版内存态），载荷对调度器是不透明字节。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bqs_config::Config;

use crate::{ports::Clock, ports::Ports};

/// 一条待下发的重搜任务（trainer 提交的原始载荷 + 元信息）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingReanalysis {
    pub variant: String,
    pub mcts_sims: i32,
    /// 位置载荷（版本化紧凑编码，调度器视为不透明字节）。
    pub payload: Vec<u8>,
    /// 位置条数（仅统计/展示；权威值在 payload 内）。
    pub positions: u32,
    pub requester: String,
    pub created_at: i64,
}

#[derive(Default)]
struct Inner {
    items: VecDeque<PendingReanalysis>,
    /// 自上次下发重搜以来的 selfplay 任务数（节流计数）。
    since_selfplay: i64,
}

/// 重搜队列子系统（独立持锁）：FIFO + 按 selfplay 下发数节流。
/// interval_tasks = 0（配置文件键）时整个特性关闭（SubmitReanalysis 直接拒绝，
/// 队列不积累）。队列上限 max_queue 按「载荷条数」计：满了拒绝新提交——
/// 不按 FIFO 丢弃最旧（最旧的位置恰恰重搜收益最大）。
pub struct ReanaQueue {
    inner: Mutex<Inner>,
    cfg: Arc<Config>,
    #[allow(dead_code)]
    clock: Arc<dyn Clock>,
}

impl ReanaQueue {
    pub fn new(_ports: &Ports, cfg: Arc<Config>, clock: Arc<dyn Clock>) -> Self {
        Self { inner: Mutex::new(Inner::default()), cfg, clock }
    }

    /// 特性开关：interval_tasks = 0 时整个特性关闭。
    pub fn enabled(&self) -> bool {
        self.cfg.reanalysis.interval_tasks > 0
    }

    /// 入队；队列满时拒绝（返回 false），不丢弃已有条目。
    /// 返回（入队后的待重搜位置总数, 是否成功）。
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

    /// 按节流查看队首待下发的重搜任务（不消费）；不足间隔或无待办时返回 None。
    ///
    /// 与 commit 分开是为了「组装任务失败时不丢数据」：组装（拉 best / 签发 URL）
    /// 可能失败，此时保留队首待下次 GetTask 重试，而不是把 trainer 提交的位置丢掉。
    pub fn peek(&self) -> Option<PendingReanalysis> {
        let inner = self.inner.lock().unwrap();
        if inner.items.is_empty() || inner.since_selfplay < self.cfg.reanalysis.interval_tasks {
            return None;
        }
        inner.items.front().cloned()
    }

    /// 消费队首（成功下发后调用），并重置节流计数。
    pub fn commit(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.items.pop_front();
        inner.since_selfplay = 0;
    }

    /// 记录一次 selfplay 下发（重搜节流计数）。
    pub fn note_selfplay_assigned(&self) {
        self.inner.lock().unwrap().since_selfplay += 1;
    }

    fn pending_locked(inner: &Inner) -> u64 {
        inner.items.iter().map(|it| it.positions as u64).sum()
    }

    /// 待重搜位置总数与批数（WebUI / 日志用）。
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
        assert!(!queue("").enabled(), "interval_tasks = 0 关闭（SubmitReanalysis 直接拒绝）");
        assert!(queue("[reanalysis]\ninterval_tasks = 2\n").enabled());
    }

    #[test]
    fn enqueue_rejects_when_full_without_dropping() {
        let q = queue("[reanalysis]\ninterval_tasks = 1\nmax_queue = 2\n");
        assert_eq!(q.enqueue(item(3)), (3, true));
        assert_eq!(q.enqueue(item(4)), (7, true));
        // 满 → 拒绝新提交，已有条目原样保留（不按 FIFO 丢最旧）
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
        // 节流：每 2 个 selfplay 任务最多 1 个重搜——不足间隔时 peek 返回 None
        assert!(q.peek().is_none());
        q.note_selfplay_assigned();
        assert!(q.peek().is_none(), "1/2 不足间隔");
        q.note_selfplay_assigned();
        let first = q.peek().unwrap();
        assert_eq!(first.positions, 1, "FIFO 取队首");
        // peek 不消费：再次 peek 仍是队首
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
        // 节流计数重置：组装失败→重试语义（未攒够间隔不得再下发）
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
        q.commit(); // 空队列 commit 无害
        assert_eq!(q.pending(), (0, 0));
    }
}
