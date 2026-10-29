//! 【所有权：core-pipeline 成员】
//!
//! DeriveQueue：特征包补派队列（FIFO，按对象键去重；上限 `DERIVE_QUEUE_MAX`）。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/featurepack.go`。
//! 对象键派生规则（`episodes/<sha>/<data_id>.pack.v<n>.fpb.gz`，由记录键派生）
//! **三端各有一份实现，改动必须同步**。

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use bqs_config::Config;

use crate::{consts, ports::Clock, ports::Ports};

/// 当前签发/派生的特征格式版本（pack_version 缺省时回落此值）。
pub const DEFAULT_FEATURE_PACK_VERSION: i64 = 1;

/// 一条待下发的特征包补派任务。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDerive {
    pub episode_object_key: String,
    pub pack_version: i64,
    pub requester: String,
    pub created_at: i64,
}

/// 特征包对象键：由记录批次对象键派生（与记录同目录），
/// `.epb.gz` → `.pack.v<n>.fpb.gz`。
///
/// 三端（collector/scheduler/training）共用本派生规则，改动必须同步；
/// 与 Go 版 `r2.FeaturePackKey` 逐字符一致（含「键不以 .epb.gz 结尾时直接追加」
/// 的回退分支与 7 字节后缀的边界处理）。
pub fn feature_pack_key(episode_object_key: &str, pack_version: i64) -> String {
    let suffix = format!(".pack.v{pack_version}.fpb.gz");
    // 与 Go 版逐字符一致：后缀判定带 `len > 7` 边界（键本身恰为 ".epb.gz" 时
    // 按「非记录键」处理，直接追加）。
    if episode_object_key.len() > 7 && episode_object_key.ends_with(".epb.gz") {
        let stripped = &episode_object_key[..episode_object_key.len() - 7];
        return format!("{stripped}{suffix}");
    }
    format!("{episode_object_key}{suffix}")
}

#[derive(Default)]
struct Inner {
    items: VecDeque<PendingDerive>,
    /// 队列内去重标记（「对象键#v版本」）。commit 时移除对应标记，
    /// 已下发完成的键允许再次入队（例如记录重传后的再派生）。
    seen: HashSet<String>,
}

/// 特征包补派队列子系统（独立持锁）：FIFO + 按对象键去重。
/// 派生是轻量索引 gather，不与重搜争抢算力，无需节流。
pub struct DeriveQueue {
    inner: Mutex<Inner>,
    #[allow(dead_code)]
    cfg: Arc<Config>,
    #[allow(dead_code)]
    clock: Arc<dyn Clock>,
}

impl DeriveQueue {
    pub fn new(_ports: &Ports, cfg: Arc<Config>, clock: Arc<dyn Clock>) -> Self {
        Self { inner: Mutex::new(Inner::default()), cfg, clock }
    }

    fn dedupe_key(item: &PendingDerive) -> String {
        format!("{}#v{}", item.episode_object_key, item.pack_version)
    }

    /// 入队；重复（同对象键 + 版本已在队列）返回 false。队列满时拒绝。
    pub fn enqueue(&self, item: PendingDerive) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let key = Self::dedupe_key(&item);
        if inner.seen.contains(&key) {
            return false;
        }
        if inner.items.len() >= consts::DERIVE_QUEUE_MAX {
            return false;
        }
        inner.items.push_back(item);
        inner.seen.insert(key);
        true
    }

    /// 查看队首（不消费）；成功下发后调用 `commit` 消费。
    pub fn peek(&self) -> Option<PendingDerive> {
        self.inner.lock().unwrap().items.front().cloned()
    }

    /// 消费队首，并移除该条目的去重标记。
    pub fn commit(&self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(front) = inner.items.pop_front() {
            inner.seen.remove(&Self::dedupe_key(&front));
        }
    }

    /// 队列长度（WebUI / 日志用）。
    pub fn pending(&self) -> usize {
        self.inner.lock().unwrap().items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeClock, Fakes};

    fn queue() -> DeriveQueue {
        let f = Fakes::new();
        DeriveQueue::new(&f.ports(), Arc::new(Config::default()), FakeClock::new(0))
    }

    fn item(key: &str, version: i64) -> PendingDerive {
        PendingDerive {
            episode_object_key: key.into(),
            pack_version: version,
            requester: "trainer".into(),
            created_at: 0,
        }
    }

    /// 对象键派生规则与 Go 版 r2.FeaturePackKey 逐字符一致（三端共享，改动必须同步）。
    #[test]
    fn feature_pack_key_matches_go() {
        assert_eq!(
            feature_pack_key("episodes/sha/data.epb.gz", 1),
            "episodes/sha/data.pack.v1.fpb.gz"
        );
        assert_eq!(
            feature_pack_key("episodes/sha/data.epb.gz", 12),
            "episodes/sha/data.pack.v12.fpb.gz"
        );
        // 不以 .epb.gz 结尾 → 直接追加
        assert_eq!(feature_pack_key("episodes/sha/data", 2), "episodes/sha/data.pack.v2.fpb.gz");
        // 边界：键本身恰为 7 字节后缀时按「非记录键」处理（Go 版 len > 7 判定）
        assert_eq!(feature_pack_key(".epb.gz", 1), ".epb.gz.pack.v1.fpb.gz");
    }

    #[test]
    fn enqueue_dedupes_by_key_and_version() {
        let q = queue();
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 1)));
        assert!(!q.enqueue(item("episodes/a/x.epb.gz", 1)), "同对象键同版本去重");
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 2)), "不同版本不去重");
        assert!(q.enqueue(item("episodes/a/y.epb.gz", 1)), "不同对象键不去重");
        assert_eq!(q.pending(), 3);
    }

    #[test]
    fn commit_releases_dedupe_marker() {
        let q = queue();
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 1)));
        q.commit();
        assert_eq!(q.pending(), 0);
        // 已下发完成的键允许再次入队（记录重传后的再派生）
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 1)));
    }

    #[test]
    fn queue_full_rejects() {
        let q = queue();
        for i in 0..consts::DERIVE_QUEUE_MAX {
            assert!(q.enqueue(item(&format!("episodes/a/{i}.epb.gz"), 1)));
        }
        assert!(!q.enqueue(item("episodes/a/overflow.epb.gz", 1)), "满即拒绝（训练端会重试）");
        assert_eq!(q.pending(), consts::DERIVE_QUEUE_MAX);
    }

    #[test]
    fn peek_peek_commit_sequence() {
        let q = queue();
        assert!(q.enqueue(item("episodes/a/first.epb.gz", 1)));
        assert!(q.enqueue(item("episodes/a/second.epb.gz", 1)));
        assert_eq!(q.peek().unwrap().episode_object_key, "episodes/a/first.epb.gz");
        assert_eq!(q.peek().unwrap().episode_object_key, "episodes/a/first.epb.gz", "peek 不消费");
        q.commit();
        assert_eq!(q.peek().unwrap().episode_object_key, "episodes/a/second.epb.gz");
        q.commit();
        assert!(q.peek().is_none());
    }
}
