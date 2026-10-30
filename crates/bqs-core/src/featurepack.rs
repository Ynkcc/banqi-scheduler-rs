use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use bqs_config::Config;

use crate::{consts, ports::Clock, ports::Ports};

pub const DEFAULT_FEATURE_PACK_VERSION: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDerive {
    pub episode_object_key: String,
    pub pack_version: i64,
    pub requester: String,
    pub created_at: i64,
}

pub fn feature_pack_key(episode_object_key: &str, pack_version: i64) -> String {
    let suffix = format!(".pack.v{pack_version}.fpb.gz");

    if episode_object_key.len() > 7 && episode_object_key.ends_with(".epb.gz") {
        let stripped = &episode_object_key[..episode_object_key.len() - 7];
        return format!("{stripped}{suffix}");
    }
    format!("{episode_object_key}{suffix}")
}

#[derive(Default)]
struct Inner {
    items: VecDeque<PendingDerive>,

    seen: HashSet<String>,
}

pub struct DeriveQueue {
    inner: Mutex<Inner>,
    #[allow(dead_code)]
    cfg: Arc<Config>,
    #[allow(dead_code)]
    clock: Arc<dyn Clock>,
}

impl DeriveQueue {
    pub fn new(_ports: &Ports, cfg: Arc<Config>, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            cfg,
            clock,
        }
    }

    fn dedupe_key(item: &PendingDerive) -> String {
        format!("{}#v{}", item.episode_object_key, item.pack_version)
    }

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

    pub fn peek(&self) -> Option<PendingDerive> {
        self.inner.lock().unwrap().items.front().cloned()
    }

    pub fn commit(&self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(front) = inner.items.pop_front() {
            inner.seen.remove(&Self::dedupe_key(&front));
        }
    }

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

        assert_eq!(
            feature_pack_key("episodes/sha/data", 2),
            "episodes/sha/data.pack.v2.fpb.gz"
        );

        assert_eq!(feature_pack_key(".epb.gz", 1), ".epb.gz.pack.v1.fpb.gz");
    }

    #[test]
    fn enqueue_dedupes_by_key_and_version() {
        let q = queue();
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 1)));
        assert!(
            !q.enqueue(item("episodes/a/x.epb.gz", 1)),
            "同对象键同版本去重"
        );
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 2)), "不同版本不去重");
        assert!(
            q.enqueue(item("episodes/a/y.epb.gz", 1)),
            "不同对象键不去重"
        );
        assert_eq!(q.pending(), 3);
    }

    #[test]
    fn commit_releases_dedupe_marker() {
        let q = queue();
        assert!(q.enqueue(item("episodes/a/x.epb.gz", 1)));
        q.commit();
        assert_eq!(q.pending(), 0);

        assert!(q.enqueue(item("episodes/a/x.epb.gz", 1)));
    }

    #[test]
    fn queue_full_rejects() {
        let q = queue();
        for i in 0..consts::DERIVE_QUEUE_MAX {
            assert!(q.enqueue(item(&format!("episodes/a/{i}.epb.gz"), 1)));
        }
        assert!(
            !q.enqueue(item("episodes/a/overflow.epb.gz", 1)),
            "满即拒绝（训练端会重试）"
        );
        assert_eq!(q.pending(), consts::DERIVE_QUEUE_MAX);
    }

    #[test]
    fn peek_peek_commit_sequence() {
        let q = queue();
        assert!(q.enqueue(item("episodes/a/first.epb.gz", 1)));
        assert!(q.enqueue(item("episodes/a/second.epb.gz", 1)));
        assert_eq!(
            q.peek().unwrap().episode_object_key,
            "episodes/a/first.epb.gz"
        );
        assert_eq!(
            q.peek().unwrap().episode_object_key,
            "episodes/a/first.epb.gz",
            "peek 不消费"
        );
        q.commit();
        assert_eq!(
            q.peek().unwrap().episode_object_key,
            "episodes/a/second.epb.gz"
        );
        q.commit();
        assert!(q.peek().is_none());
    }
}
