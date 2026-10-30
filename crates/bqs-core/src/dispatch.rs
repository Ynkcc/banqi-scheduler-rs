use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bqs_config::Config;
use bqs_proto::TaskKind;

use crate::{consts, ports::Clock, ports::Ports};

const TASK_DEADLINE_DEFAULT: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Assigned,
    Completed,
    Expired,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Assigned => "assigned",
            TaskState::Completed => "completed",
            TaskState::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningTask {
    pub task_id: String,
    pub worker_id: String,
    pub kind: TaskKind,
    pub state: TaskState,

    pub assigned_at: i64,

    pub finished_at: i64,

    pub reported: bool,

    pub match_id: i64,

    pub network_sha: String,

    pub opponent_sha: String,

    pub opponent_spec: String,

    pub games: i64,

    pub deadline: i64,

    pub job_id: String,
}

impl Default for RunningTask {
    fn default() -> Self {
        Self {
            task_id: String::new(),
            worker_id: String::new(),
            kind: TaskKind::TaskNone,
            state: TaskState::Assigned,
            assigned_at: 0,
            finished_at: 0,
            reported: false,
            match_id: 0,
            network_sha: String::new(),
            opponent_sha: String::new(),
            opponent_spec: String::new(),
            games: 0,
            deadline: 0,
            job_id: String::new(),
        }
    }
}

impl RunningTask {
    pub fn new(kind: TaskKind) -> Self {
        Self {
            kind,
            ..Default::default()
        }
    }
}

fn deadline_secs(kind: TaskKind) -> i64 {
    match kind {
        TaskKind::TaskEval | TaskKind::TaskReanalysis => {
            consts::EVAL_TASK_STALE_AFTER.as_secs() as i64
        }
        TaskKind::TaskCollect => consts::COLLECT_TASK_DEADLINE.as_secs() as i64,
        _ => TASK_DEADLINE_DEFAULT.as_secs() as i64,
    }
}

#[derive(Default)]
struct DispatcherInner {
    tasks: HashMap<String, RunningTask>,

    last_prune: Option<i64>,
}

pub struct TaskDispatcher {
    clock: Arc<dyn Clock>,
    inner: Mutex<DispatcherInner>,
}

impl TaskDispatcher {
    pub fn new(_ports: &Ports, _cfg: Arc<Config>, clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            inner: Mutex::new(DispatcherInner::default()),
        }
    }

    pub fn register(&self, task_id: &str, mut t: RunningTask) {
        let now = self.clock.now();
        t.task_id = task_id.to_string();
        t.state = TaskState::Assigned;
        t.assigned_at = now;
        t.finished_at = 0;
        t.reported = false;
        t.deadline = now + deadline_secs(t.kind);
        self.inner
            .lock()
            .unwrap()
            .tasks
            .insert(task_id.to_string(), t);
    }

    pub fn register_in_flight(&self, task_id: &str, mut t: RunningTask) {
        t.task_id = task_id.to_string();
        self.inner
            .lock()
            .unwrap()
            .tasks
            .insert(task_id.to_string(), t);
    }

    pub fn delete(&self, task_id: &str) {
        self.inner.lock().unwrap().tasks.remove(task_id);
    }

    pub fn override_deadline(&self, task_id: &str, deadline: i64) {
        if let Some(t) = self.inner.lock().unwrap().tasks.get_mut(task_id) {
            t.deadline = deadline;
        }
    }

    pub fn set_task_job(&self, task_id: &str, job_id: &str) {
        if let Some(t) = self.inner.lock().unwrap().tasks.get_mut(task_id) {
            t.job_id = job_id.to_string();
        }
    }

    pub fn lookup(&self, task_id: &str) -> Option<RunningTask> {
        self.inner.lock().unwrap().tasks.get(task_id).cloned()
    }

    pub fn transition(&self, task_id: &str, to: TaskState) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(t) = inner.tasks.get_mut(task_id) else {
            return false;
        };
        if t.state != TaskState::Assigned {
            return false;
        }
        let from = t.state;
        t.state = to;
        t.finished_at = self.clock.now();
        tracing::info!(
            task_id, from = from.as_str(), to = to.as_str(),
            kind = t.kind.as_str_name(), worker = %t.worker_id, games = t.games,
            "[task-state] 迁移"
        );
        true
    }

    pub fn mark_done(&self, task_id: &str) {
        self.transition(task_id, TaskState::Completed);
    }

    pub fn claim_report(&self, task_id: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(t) = inner.tasks.get_mut(task_id) else {
            return false;
        };
        if t.state == TaskState::Completed {
            return false;
        }
        let from = t.state;
        t.state = TaskState::Completed;
        t.finished_at = self.clock.now();
        t.reported = true;
        tracing::info!(
            task_id, from = from.as_str(), kind = t.kind.as_str_name(),
            worker = %t.worker_id, games = t.games,
            "[task-state] 首次上报 → completed"
        );
        true
    }

    pub fn rollback_report(&self, task_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(t) = inner.tasks.get_mut(task_id) {
            if t.state == TaskState::Completed {
                tracing::info!(
                    task_id,
                    "[task-state] completed → assigned（聚合失败回滚，等待重试上报）"
                );
                t.state = TaskState::Assigned;
                t.finished_at = 0;
                t.reported = false;
            }
        }
    }

    pub fn complete_in_flight(&self, task_id: &str) {
        self.transition(task_id, TaskState::Completed);
    }

    pub fn prune(&self) {
        let now = self.clock.now();
        let mut inner = self.inner.lock().unwrap();
        if let Some(last) = inner.last_prune {
            if now - last < consts::PRUNE_INTERVAL.as_secs() as i64 {
                return;
            }
        }
        inner.last_prune = Some(now);
        let mut removed = 0usize;
        let mut expired = 0usize;
        let ids: Vec<String> = inner.tasks.keys().cloned().collect();
        for id in ids {
            let Some(t) = inner.tasks.get_mut(&id) else {
                continue;
            };
            let age = now - t.assigned_at;

            if t.state == TaskState::Assigned && now >= t.deadline {
                tracing::info!(task_id = %id, "[task-state] assigned → expired（超期未上报）");
                t.state = TaskState::Expired;
                t.finished_at = now;
                expired += 1;
            }

            let terminal_retained = t.state != TaskState::Assigned
                && now - t.finished_at >= consts::DONE_TASK_RETENTION.as_secs() as i64;
            if terminal_retained || age >= consts::TASK_MAX_RETENTION.as_secs() as i64 {
                inner.tasks.remove(&id);
                removed += 1;
            }
        }
        if removed > 0 || expired > 0 {
            tracing::info!(
                "[task-state] 本轮迁移 {expired} 条 → expired，回收 {removed} 条，内存表剩余 {} 条",
                inner.tasks.len()
            );
        }
    }

    pub fn running(&self) -> Vec<RunningTask> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<RunningTask> = inner
            .tasks
            .values()
            .filter(|t| t.state == TaskState::Assigned)
            .cloned()
            .collect();
        out.sort_by(|a, b| b.assigned_at.cmp(&a.assigned_at));
        out
    }

    pub fn in_flight_games(
        &self,
        kind: TaskKind,
        sha: &str,
        spec: &str,
        now: i64,
        stale_after: Duration,
    ) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner
            .tasks
            .values()
            .filter(|t| {
                t.kind == kind
                    && t.state == TaskState::Assigned
                    && t.network_sha == sha
                    && t.opponent_spec == spec
                    && now - t.assigned_at < stale_after.as_secs() as i64
            })
            .map(|t| t.games)
            .sum()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn running_tasks_webui(&self) -> Vec<RunningTask> {
        self.running()
    }

    pub fn task_state_snapshot(&self) -> TaskStateStats {
        let now = self.clock.now();
        let inner = self.inner.lock().unwrap();
        let mut out = TaskStateStats::default();
        for (id, t) in inner.tasks.iter() {
            let age = now - t.assigned_at;
            match t.state {
                TaskState::Assigned => {
                    out.counts.assigned += 1;
                    if age > out.oldest_assigned_age_seconds {
                        out.oldest_assigned_age_seconds = age;
                    }
                }
                TaskState::Completed => out.counts.completed += 1,
                TaskState::Expired => out.counts.expired += 1,
            }
            if t.state == TaskState::Expired {
                out.expired.push(TaskStateRecord {
                    task_id: id.clone(),
                    worker_id: t.worker_id.clone(),
                    kind: t.kind.as_str_name().to_string(),
                    state: t.state.as_str().to_string(),
                    games: t.games,
                    created_at: t.assigned_at,
                    age_seconds: age,
                });
            }
        }

        out.expired.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        out.expired.truncate(MAX_EXPIRED_RECORDS);
        out
    }
}

const MAX_EXPIRED_RECORDS: usize = 32;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct TaskStateCounts {
    pub assigned: i64,
    pub completed: i64,
    pub expired: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStateRecord {
    pub task_id: String,
    pub worker_id: String,

    pub kind: String,
    pub state: String,
    pub games: i64,
    pub created_at: i64,
    pub age_seconds: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStateStats {
    pub counts: TaskStateCounts,
    pub expired: Vec<TaskStateRecord>,

    pub oldest_assigned_age_seconds: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeClock, Fakes};

    fn dispatcher(clock: Arc<dyn Clock>) -> TaskDispatcher {
        let f = Fakes::new();
        TaskDispatcher::new(&f.ports(), Arc::new(bqs_config::Config::default()), clock)
    }

    fn task(worker: &str, kind: TaskKind, games: i64) -> RunningTask {
        RunningTask {
            worker_id: worker.into(),
            network_sha: "n1".into(),
            opponent_spec: "random".into(),
            games,
            ..RunningTask::new(kind)
        }
    }

    #[test]
    fn register_and_lookup_snapshot() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 16));
        let t = d.lookup("t1").unwrap();
        assert_eq!(t.state, TaskState::Assigned);
        assert_eq!(t.assigned_at, 1000);
        assert_eq!(t.deadline, 1000 + 600);
        assert!(!t.reported);
        assert!(d.lookup("nope").is_none());
    }

    #[test]
    fn deadline_by_kind() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("e", task("w", TaskKind::TaskEval, 10));
        d.register("r", task("w", TaskKind::TaskReanalysis, 10));
        d.register("s", task("w", TaskKind::TaskSelfplay, 10));
        d.register("x", task("w", TaskKind::TaskDeriveFeatures, 1));
        assert_eq!(
            d.lookup("e").unwrap().deadline,
            1000 + consts::EVAL_TASK_STALE_AFTER.as_secs() as i64
        );
        assert_eq!(
            d.lookup("r").unwrap().deadline,
            1000 + consts::EVAL_TASK_STALE_AFTER.as_secs() as i64
        );
        assert_eq!(d.lookup("s").unwrap().deadline, 1000 + 600);
        assert_eq!(d.lookup("x").unwrap().deadline, 1000 + 600);
    }

    #[test]
    fn claim_report_is_cas() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 8));
        assert!(d.claim_report("t1"));
        assert!(!d.claim_report("t1"), "同 task_id 并发重复上报只能抢占一次");
        let t = d.lookup("t1").unwrap();
        assert_eq!(t.state, TaskState::Completed);
        assert!(t.reported);
        assert!(d.lookup("t1").unwrap().finished_at > 0);

        assert!(!d.claim_report("nope"));
    }

    #[test]
    fn expired_task_can_still_claim_late_report() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskEval, 10));

        clock.advance(consts::EVAL_TASK_STALE_AFTER.as_secs() as i64);
        d.prune();
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Expired);

        assert!(d.claim_report("t1"));
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);
    }

    #[test]
    fn rollback_report_restores_claimable_state() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskRating, 4));
        assert!(d.claim_report("t1"));
        d.rollback_report("t1");
        let t = d.lookup("t1").unwrap();
        assert_eq!(t.state, TaskState::Assigned);
        assert!(!t.reported);

        d.rollback_report("t1");
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Assigned);

        assert!(d.claim_report("t1"));
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);
    }

    #[test]
    fn transition_terminal_is_irreversible() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 4));
        d.mark_done("t1");

        assert!(!d.transition("t1", TaskState::Expired));
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);

        d.mark_done("t1");
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);

        d.register("t2", task("w1", TaskKind::TaskSelfplay, 4));
        d.complete_in_flight("t2");
        assert_eq!(d.lookup("t2").unwrap().state, TaskState::Completed);
        d.complete_in_flight("t2");

        assert!(!d.transition("nope", TaskState::Completed));
    }

    #[test]
    fn prune_deadline_boundary() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 4));
        clock.set(599);
        d.prune();
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Assigned);

        clock.set(600 + consts::PRUNE_INTERVAL.as_secs() as i64);
        d.prune();
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Expired);
    }

    #[test]
    fn prune_done_retention_and_max_retention() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("done", task("w1", TaskKind::TaskSelfplay, 4));
        d.mark_done("done");

        clock.set(consts::DONE_TASK_RETENTION.as_secs() as i64 - 1);
        d.prune();
        assert!(d.lookup("done").is_some());
        clock.advance(consts::PRUNE_INTERVAL.as_secs() as i64);
        d.prune();
        assert!(d.lookup("done").is_none(), "终态记录超保留期应回收");

        d.register("stale", task("w1", TaskKind::TaskEval, 10));
        clock.advance(consts::TASK_MAX_RETENTION.as_secs() as i64);
        d.prune();
        assert!(d.lookup("stale").is_none(), "age >= 24h 应直接回收");
    }

    #[test]
    fn prune_throttled_by_interval() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 4));
        clock.set(600);
        d.prune();
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Expired);

        d.register("t2", task("w1", TaskKind::TaskEval, 4));
        clock.set(659);
        d.prune();
        assert_eq!(
            d.lookup("t2").unwrap().state,
            TaskState::Assigned,
            "节流窗口内不应回收"
        );
        clock.set(660);
        d.prune();
        assert_eq!(
            d.lookup("t2").unwrap().state,
            TaskState::Assigned,
            "eval deadline 未到不应过期"
        );
        clock.set(1500);
        d.prune();
        assert_eq!(d.lookup("t2").unwrap().state, TaskState::Expired);
    }

    #[test]
    fn in_flight_games_filters() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("a", task("w1", TaskKind::TaskEval, 10));
        d.register("b", task("w2", TaskKind::TaskEval, 5));
        d.register("c", {
            let mut t = task("w3", TaskKind::TaskEval, 7);
            t.opponent_spec = "other".into();
            t
        });
        let stale = consts::EVAL_TASK_STALE_AFTER;

        assert_eq!(
            d.in_flight_games(TaskKind::TaskEval, "n1", "random", 0, stale),
            15
        );
        assert_eq!(
            d.in_flight_games(TaskKind::TaskEval, "n1", "other", 0, stale),
            7
        );
        assert_eq!(
            d.in_flight_games(TaskKind::TaskEval, "nX", "random", 0, stale),
            0
        );

        assert_eq!(
            d.in_flight_games(
                TaskKind::TaskEval,
                "n1",
                "random",
                stale.as_secs() as i64,
                stale
            ),
            0
        );
        assert_eq!(
            d.in_flight_games(
                TaskKind::TaskEval,
                "n1",
                "random",
                stale.as_secs() as i64 - 1,
                stale
            ),
            15
        );

        d.mark_done("a");
        assert_eq!(
            d.in_flight_games(TaskKind::TaskEval, "n1", "random", 0, stale),
            5
        );

        d.register("e", task("w4", TaskKind::TaskEval, 3));
        clock.advance(stale.as_secs() as i64);
        d.prune();
        assert_eq!(
            d.in_flight_games(
                TaskKind::TaskEval,
                "n1",
                "random",
                stale.as_secs() as i64,
                stale
            ),
            0
        );
    }

    #[test]
    fn running_returns_only_assigned_desc() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("old", task("w1", TaskKind::TaskSelfplay, 1));
        clock.advance(10);
        d.register("new", task("w2", TaskKind::TaskSelfplay, 2));
        d.mark_done("old");
        let running = d.running();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].worker_id, "w2");

        clock.advance(1000);
        d.prune();
        assert!(d.running().is_empty());
    }

    #[test]
    fn running_tasks_webui_carries_task_id_and_view_fields() {
        let clock = FakeClock::new(100);
        let d = dispatcher(clock.clone());
        d.register(
            "t1",
            RunningTask {
                worker_id: "w1".into(),
                network_sha: "n1".into(),
                opponent_sha: "opp".into(),
                opponent_spec: "rule:capture_first".into(),
                match_id: 7,
                games: 4,
                ..RunningTask::new(TaskKind::TaskSelfplay)
            },
        );
        let tasks = d.running_tasks_webui();
        assert_eq!(tasks.len(), 1);
        let t = &tasks[0];

        assert_eq!(t.task_id, "t1", "task_id 由 register 回填");
        assert_eq!(t.worker_id, "w1");
        assert_eq!(t.kind.as_str_name(), "TASK_SELFPLAY");
        assert_eq!(t.match_id, 7);
        assert_eq!(t.network_sha, "n1");
        assert_eq!(t.opponent_sha, "opp");
        assert_eq!(t.opponent_spec, "rule:capture_first");
        assert_eq!(t.games, 4);
        assert_eq!(t.assigned_at, 100);

        d.register_in_flight(
            "e1",
            RunningTask {
                games: 10,
                ..RunningTask::new(TaskKind::TaskEval)
            },
        );
        assert_eq!(d.lookup("e1").unwrap().task_id, "e1");
    }

    #[test]
    fn task_state_snapshot_counts_and_diagnostics() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());

        d.register("c1", task("w2", TaskKind::TaskRating, 4));

        clock.set(960);
        d.register("e1", task("w3", TaskKind::TaskEval, 10));
        clock.advance(1);
        d.register("e2", task("w4", TaskKind::TaskReanalysis, 3));

        clock.set(1860);
        d.register("a1", task("w1", TaskKind::TaskSelfplay, 4));
        d.mark_done("c1");
        clock.advance(1);
        d.prune();

        let snap = d.task_state_snapshot();
        assert_eq!(snap.counts.assigned, 1);
        assert_eq!(snap.counts.completed, 1);
        assert_eq!(snap.counts.expired, 2);
        assert_eq!(snap.oldest_assigned_age_seconds, 1, "最老在途 = a1 的年龄");

        assert_eq!(snap.expired.len(), 2);
        assert_eq!(snap.expired[0].task_id, "e2");
        assert_eq!(snap.expired[1].task_id, "e1");
        assert_eq!(snap.expired[0].kind, "TASK_REANALYSIS");
        assert_eq!(snap.expired[0].state, "expired");
        assert_eq!(snap.expired[0].worker_id, "w4");
        assert!(snap.expired[0].age_seconds > 0);

        let d2 = dispatcher(FakeClock::new(0));
        let empty = d2.task_state_snapshot();
        assert_eq!(empty.counts.assigned, 0);
        assert!(empty.expired.is_empty());
        assert_eq!(empty.oldest_assigned_age_seconds, 0);
    }

    #[test]
    fn task_state_snapshot_expired_capped_at_32() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        for i in 0..40 {
            d.register(&format!("t{i}"), task("w", TaskKind::TaskEval, 1));
            clock.advance(1);
        }
        clock.advance(consts::EVAL_TASK_STALE_AFTER.as_secs() as i64);
        d.prune();
        let snap = d.task_state_snapshot();
        assert_eq!(snap.counts.expired, 40);
        assert_eq!(snap.expired.len(), 32, "诊断列表最多 32 条");
        assert_eq!(snap.expired[0].task_id, "t39", "倒序：最近在前");
    }
}
