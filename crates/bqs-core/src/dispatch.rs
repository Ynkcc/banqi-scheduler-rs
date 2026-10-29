//! 【所有权：core-pipeline 成员】
//!
//! TaskDispatcher：内存任务表 + 状态机（Assigned → Completed | Expired，终态不可逆）。
//!
//! 参考实现（行为真源，非逐行翻译）：`banqi-scheduler/internal/scheduler/dispatch.go`
//! 与 `taskstate.go`。要点：
//! - 内存表只做 task↔worker 归属校验 / 在飞判定 / CAS 上报权；进度以 DB 为准，重启即丢；
//! - `claim_report`（CAS 抢占上报权，防 TOCTOU 双计）+ `rollback_report`
//!   （聚合失败回滚终态，「先 DB 落库、后置终态」是修过真实 bug 的保序约定）；
//! - Expired 也算「未上报过」：慢 worker 迟到上报仍可聚合；
//! - `prune`：Done 保留 `DONE_TASK_RETENTION` / 未上报最长 `TASK_MAX_RETENTION`，
//!   挂 GetTask 路径按 `PRUNE_INTERVAL` 节流（时间一律取自注入 Clock）；
//! - 在飞局数快照（`in_flight_games`，供 eval 流式额度收缩）。
//!
//! 锁序：本子系统是叶子锁（只允许被 evalc 在持锁期间获取，禁止反向）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bqs_config::Config;
use bqs_proto::TaskKind;

use crate::{consts, ports::Clock, ports::Ports};

/// selfplay / rating / derive 任务「已下发 → 超期」的默认期限。
/// Go 版 taskstate.go：48 局 × 100k 预算在桌面约 3~4 分钟，10 分钟留 2.5x 余量。
const TASK_DEADLINE_DEFAULT: Duration = Duration::from_secs(10 * 60);

/// 任务生命周期状态机（状态只在 TaskDispatcher 内变更）：
///
/// ```text
/// Assigned ──上报(episode / match result / eval)──▶ Completed（终态）
///     └────────超期未上报（Deadline）──────────────▶ Expired  （终态，释放在飞名额）
/// ```
///
/// 语义约定：
/// - 首次上报（Assigned 或 Expired，`claim_report` CAS）→ Completed；此后重复
///   上报保持 Completed（幂等短路）；
/// - Expired 只表示「调度器视角超期未收到上报」，不代表任务在 worker 侧失败；
///   迟到上报仍被接受（Deadline 只控制在飞名额，不否决数据），CAS 防重复计数；
/// - 聚合/落库失败时 Completed 可回滚为 Assigned（`rollback_report`）；
/// - 任务记录本身继续保留 `DONE_TASK_RETENTION` 时长供诊断，之后回收。
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

/// 内存任务表中的一条任务记录（`lookup` 一律返回快照副本，调用方只读）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningTask {
    /// 任务 id（内存表主键；register / register_in_flight 时由 dispatcher 回填）。
    pub task_id: String,
    pub worker_id: String,
    pub kind: TaskKind,
    pub state: TaskState,
    /// 下发时间（Unix 秒，注入 Clock）
    pub assigned_at: i64,
    /// 终态时间（0 = 在飞）
    pub finished_at: i64,
    /// 是否已抢占上报权（CAS 口径）
    pub reported: bool,
    /// 关联的 gatekeeper 对打（matches 表行）：rating 任务与混入 selfplay 任务
    /// 非 0，ReportMatchResult 据此路由聚合；其余任务为 0。
    pub match_id: i64,
    /// 主网络 sha（归属校验：ReportEpisode 的 network_sha 必须一致）。
    pub network_sha: String,
    /// rating / 混入 selfplay 任务的对手网络 sha；eval 任务恒为空（对手不是网络）。
    pub opponent_sha: String,
    /// eval 任务的对手标识（rule:capture_first 等）；其余任务为空。
    pub opponent_spec: String,
    /// 本批局数（重搜任务 = 位置条数；derive 任务 = 1）。
    pub games: i64,
    /// Assigned 状态的超期时间点（Unix 秒）；超期由 prune 迁移到 Expired。
    /// eval 认领路径经 `register_in_flight` 绕过通用 deadline（见 Go 版 F4-P1）。
    pub deadline: i64,
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
        }
    }
}

impl RunningTask {
    /// 以任务类别起步构造（其余字段由调用方填充；`register` 会重置状态机字段）。
    pub fn new(kind: TaskKind) -> Self {
        Self { kind, ..Default::default() }
    }
}

/// 按任务类别给出「已下发 → 超期」的期限秒数。
/// eval / reanalysis 沿用 15 分钟口径（与 `EVAL_TASK_STALE_AFTER` 同值——两者本就是
/// 同一语义「超时未上报即让位重下发」）。
fn deadline_secs(kind: TaskKind) -> i64 {
    match kind {
        TaskKind::TaskEval | TaskKind::TaskReanalysis => consts::EVAL_TASK_STALE_AFTER.as_secs() as i64,
        _ => TASK_DEADLINE_DEFAULT.as_secs() as i64,
    }
}

#[derive(Default)]
struct DispatcherInner {
    tasks: HashMap<String, RunningTask>,
    /// 上次 prune 时间（节流相位；None = 尚未扫过）。
    last_prune: Option<i64>,
}

/// 内存任务表 + 状态机子系统。锁用 `std::sync::Mutex`；全部时间取自注入 Clock
/// （测试注入 FakeClock 可精确控制 prune 时间边界）。
pub struct TaskDispatcher {
    clock: Arc<dyn Clock>,
    inner: Mutex<DispatcherInner>,
}

impl TaskDispatcher {
    pub fn new(_ports: &Ports, _cfg: Arc<Config>, clock: Arc<dyn Clock>) -> Self {
        Self { clock, inner: Mutex::new(DispatcherInner::default()) }
    }

    /// 登记新下发的任务：状态机从 Assigned 起步，Deadline 按任务类别设定
    /// （超期由 prune 迁移到 Expired）。调用方只填业务字段（kind/worker/sha/
    /// opponent/games/match_id），状态机字段与 task_id 由此处重置/回填。
    pub fn register(&self, task_id: &str, mut t: RunningTask) {
        let now = self.clock.now();
        t.task_id = task_id.to_string();
        t.state = TaskState::Assigned;
        t.assigned_at = now;
        t.finished_at = 0;
        t.reported = false;
        t.deadline = now + deadline_secs(t.kind);
        self.inner.lock().unwrap().tasks.insert(task_id.to_string(), t);
    }

    /// eval 认领路径的直入式登记：Deadline 由调用方按 eval 口径设置
    /// （见 EvalCoordinator.claim_eval_task，绕过 register 的通用 deadline）。
    /// task_id 同样由此处回填。
    pub fn register_in_flight(&self, task_id: &str, mut t: RunningTask) {
        t.task_id = task_id.to_string();
        self.inner.lock().unwrap().tasks.insert(task_id.to_string(), t);
    }

    /// 从任务表移除记录（eval 组装失败时释放在飞认领）。
    pub fn delete(&self, task_id: &str) {
        self.inner.lock().unwrap().tasks.remove(task_id);
    }

    /// 读取任务记录。返回**快照副本**（Go 版 F4-P1 教训：共享可变状态在锁外读
    /// 是 data race）；调用方只读，不得写回。
    pub fn lookup(&self, task_id: &str) -> Option<RunningTask> {
        self.inner.lock().unwrap().tasks.get(task_id).cloned()
    }

    /// 状态迁移（Assigned → 终态）；终态不可逆，返回是否发生迁移。
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

    /// 标记任务已上报结束：Assigned → Completed。幂等：重复上报保持 Completed。
    pub fn mark_done(&self, task_id: &str) {
        self.transition(task_id, TaskState::Completed);
    }

    /// CAS 抢占一次上报权：Assigned/Expired → Completed，返回抢占是否成功——
    /// 只有首次上报的调用方进入聚合/落库路径，同 task_id 的并发重复上报全部在
    /// 闸口失败（防「幂等闸口 → 数百行后才置终态」的 TOCTOU 双计窗口）。
    ///
    /// Expired 也算「未上报过」：Deadline 只控制「在飞名额 / 重复下发」，
    /// 慢 worker 的迟到上报仍可参与聚合；CAS 保证每个 task_id 恰好聚合一次。
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

    /// 聚合/落库失败回滚：Completed → Assigned。
    ///
    /// 与 claim_report 联合实现「落库成功才置终态」；回滚后（Deadline 已过）
    /// prune 会再迁 Expired，迟到上报仍可重试聚合。
    pub fn rollback_report(&self, task_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(t) = inner.tasks.get_mut(task_id) {
            if t.state == TaskState::Completed {
                tracing::info!(task_id, "[task-state] completed → assigned（聚合失败回滚，等待重试上报）");
                t.state = TaskState::Assigned;
                t.finished_at = 0;
                t.reported = false;
            }
        }
    }

    /// 把在飞任务迁入 Completed 终态（幂等）。eval 收尾路径在 evalc.mu 之下调用
    /// （跨子系统原子段：完成在飞与更新作业同段完成）。
    pub fn complete_in_flight(&self, task_id: &str) {
        self.transition(task_id, TaskState::Completed);
    }

    /// 回收内存任务表中的超期记录（时间取自注入 Clock）。
    /// 内部按 `PRUNE_INTERVAL` 节流，可在请求路径上安全高频调用。
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
            let Some(t) = inner.tasks.get_mut(&id) else { continue };
            let age = now - t.assigned_at;
            // 状态机：Assigned 超过 Deadline → Expired（释放在飞名额，显式追踪僵尸任务）
            if t.state == TaskState::Assigned && now >= t.deadline {
                tracing::info!(task_id = %id, "[task-state] assigned → expired（超期未上报）");
                t.state = TaskState::Expired;
                t.finished_at = now;
                expired += 1;
            }
            // 回收：终态记录保留 doneTaskRetention 供重复上报给出明确错误；未上报记录最长 24h
            let terminal_retained = t.state != TaskState::Assigned
                && now - t.finished_at >= consts::DONE_TASK_RETENTION.as_secs() as i64;
            if terminal_retained || age >= consts::TASK_MAX_RETENTION.as_secs() as i64 {
                inner.tasks.remove(&id);
                removed += 1;
            }
        }
        if removed > 0 || expired > 0 {
            tracing::info!("[task-state] 本轮迁移 {expired} 条 → expired，回收 {removed} 条，内存表剩余 {} 条", inner.tasks.len());
        }
    }

    /// 全部未结束任务的快照（按下发时间倒序；WebUI 任务面板）。
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

    /// 统计指定 (kind, network, opponent) 已派发（Assigned）且未超期（未过
    /// stale_after 窗口）的在飞任务局数合计（eval 流式作业的在飞额度口径）。
    /// 本方法是跨子系统原子段（evalc.mu → disp.mu）的显式访问点：调用方持
    /// evalc 锁时调用安全（disp 为叶子锁）。
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

    /// 内存表当前条数（诊断用）。
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 全部未结束任务的快照（WebUI /api/tasks；按下发时间倒序）。
    /// Go 版 api.taskView 所需字段（taskId/workerId/kind/matchId/networkSha/
    /// opponentSha/opponentSpec/games/createdAt）全部在 [`RunningTask`] 上，
    /// api 层直接投影即可。调用方不应依赖强一致性——这是诊断视图。
    pub fn running_tasks_webui(&self) -> Vec<RunningTask> {
        self.running()
    }

    /// 任务状态机快照（WebUI /api/task-stats；Go 版 stateSnapshot → TaskStateStats）：
    /// 各状态计数 + 最近过期任务（僵尸诊断，按过期时间倒序，最多 32 条）+
    /// 最老在途任务年龄（接近 Deadline 意味着 worker 掉队或任务积压）。
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
        // 按下发时间倒序取最近 32 条（Go 版注释的既定意图）
        out.expired.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        out.expired.truncate(MAX_EXPIRED_RECORDS);
        out
    }
}

/// WebUI /api/task-stats 的过期任务诊断列表上限（Go 版 maxExpiredRecords）。
const MAX_EXPIRED_RECORDS: usize = 32;

/// 各状态的任务计数（WebUI 任务状态面板）。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct TaskStateCounts {
    pub assigned: i64,
    pub completed: i64,
    pub expired: i64,
}

/// 单条任务状态记录（只读快照；字段名与 Go 版 WebUI 契约一致 camelCase）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStateRecord {
    pub task_id: String,
    pub worker_id: String,
    /// proto TaskKind 名（TASK_SELFPLAY 等，与 Go pb String() 同口径）。
    pub kind: String,
    pub state: String,
    pub games: i64,
    pub created_at: i64,
    pub age_seconds: i64,
}

/// 任务状态机快照：计数 + 最近过期任务（僵尸诊断）+ 最老在途任务年龄。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStateStats {
    pub counts: TaskStateCounts,
    pub expired: Vec<TaskStateRecord>,
    /// 最老 Assigned 任务的年龄（秒）；无在途任务为 0。
    /// 接近 Deadline 意味着 worker 掉队或任务积压。
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
        assert_eq!(t.deadline, 1000 + 600); // selfplay 默认 10 分钟
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
        assert_eq!(d.lookup("e").unwrap().deadline, 1000 + consts::EVAL_TASK_STALE_AFTER.as_secs() as i64);
        assert_eq!(d.lookup("r").unwrap().deadline, 1000 + consts::EVAL_TASK_STALE_AFTER.as_secs() as i64);
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
        // 未知任务
        assert!(!d.claim_report("nope"));
    }

    #[test]
    fn expired_task_can_still_claim_late_report() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskEval, 10));
        // 超期未上报 → prune 迁 Expired
        clock.advance(consts::EVAL_TASK_STALE_AFTER.as_secs() as i64);
        d.prune();
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Expired);
        // 迟到上报仍可聚合（Deadline 只控制在飞名额，不否决数据）
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
        // 非 Completed 状态 rollback 无害（幂等）
        d.rollback_report("t1");
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Assigned);
        // 回滚后可重新抢占（DB 失败 → worker 重试路径）
        assert!(d.claim_report("t1"));
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);
    }

    #[test]
    fn transition_terminal_is_irreversible() {
        let clock = FakeClock::new(1000);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 4));
        d.mark_done("t1");
        // 终态不可逆
        assert!(!d.transition("t1", TaskState::Expired));
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);
        // mark_done 幂等
        d.mark_done("t1");
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Completed);
        // complete_in_flight 只作用于 Assigned
        d.register("t2", task("w1", TaskKind::TaskSelfplay, 4));
        d.complete_in_flight("t2");
        assert_eq!(d.lookup("t2").unwrap().state, TaskState::Completed);
        d.complete_in_flight("t2"); // 幂等
        // 未知任务
        assert!(!d.transition("nope", TaskState::Completed));
    }

    #[test]
    fn prune_deadline_boundary() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 4)); // deadline 600s
        clock.set(599);
        d.prune();
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Assigned);
        // 越过 deadline（同时越过 60s 节流窗口）→ Expired
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
        // Done 保留 DONE_TASK_RETENTION（30m）
        clock.set(consts::DONE_TASK_RETENTION.as_secs() as i64 - 1);
        d.prune();
        assert!(d.lookup("done").is_some());
        clock.advance(consts::PRUNE_INTERVAL.as_secs() as i64); // 同时越过节流窗口
        d.prune();
        assert!(d.lookup("done").is_none(), "终态记录超保留期应回收");

        // 未上报记录最长 TASK_MAX_RETENTION（24h）：age 到点即回收（先迁 Expired 同轮删除）
        d.register("stale", task("w1", TaskKind::TaskEval, 10));
        clock.advance(consts::TASK_MAX_RETENTION.as_secs() as i64);
        d.prune();
        assert!(d.lookup("stale").is_none(), "age >= 24h 应直接回收");
    }

    #[test]
    fn prune_throttled_by_interval() {
        let clock = FakeClock::new(0);
        let d = dispatcher(clock.clone());
        d.register("t1", task("w1", TaskKind::TaskSelfplay, 4)); // deadline 600
        clock.set(600);
        d.prune(); // last_prune = 600
        assert_eq!(d.lookup("t1").unwrap().state, TaskState::Expired);
        // t2 为 eval（deadline 1500）：600 + 900
        d.register("t2", task("w1", TaskKind::TaskEval, 4));
        clock.set(659); // 距上次 prune 59s < 60s → 节流跳过
        d.prune();
        assert_eq!(d.lookup("t2").unwrap().state, TaskState::Assigned, "节流窗口内不应回收");
        clock.set(660); // 恰好 60s → 执行
        d.prune();
        assert_eq!(d.lookup("t2").unwrap().state, TaskState::Assigned, "eval deadline 未到不应过期");
        clock.set(1500); // t2 deadline
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
        // 同 (kind, sha, spec) 合计
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "n1", "random", 0, stale), 15);
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "n1", "other", 0, stale), 7);
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "nX", "random", 0, stale), 0);
        // 超过 stale 窗口的不计（worker 掉线让位重派）
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "n1", "random", stale.as_secs() as i64, stale), 0);
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "n1", "random", stale.as_secs() as i64 - 1, stale), 15);
        // Completed 不计
        d.mark_done("a");
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "n1", "random", 0, stale), 5);
        // Expired 不计
        d.register("e", task("w4", TaskKind::TaskEval, 3));
        clock.advance(stale.as_secs() as i64);
        d.prune(); // b/e → Expired（a 已 Completed）
        assert_eq!(d.in_flight_games(TaskKind::TaskEval, "n1", "random", stale.as_secs() as i64, stale), 0);
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
        // Expired 也不再展示为「进行中」
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
        // WebUI taskView 字段（Go handlers_status.go）：api 层直接投影
        assert_eq!(t.task_id, "t1", "task_id 由 register 回填");
        assert_eq!(t.worker_id, "w1");
        assert_eq!(t.kind.as_str_name(), "TASK_SELFPLAY");
        assert_eq!(t.match_id, 7);
        assert_eq!(t.network_sha, "n1");
        assert_eq!(t.opponent_sha, "opp");
        assert_eq!(t.opponent_spec, "rule:capture_first");
        assert_eq!(t.games, 4);
        assert_eq!(t.assigned_at, 100);
        // eval 认领路径（register_in_flight）同样回填 task_id
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
        // completed（临近 prune 才置终态：终态记录保留期 30 分钟，早置会被 prune 回收）
        d.register("c1", task("w2", TaskKind::TaskRating, 4));
        // 两条将过期的 eval（deadline 960 / 961）
        clock.set(960);
        d.register("e1", task("w3", TaskKind::TaskEval, 10));
        clock.advance(1);
        d.register("e2", task("w4", TaskKind::TaskReanalysis, 3));
        // 一条仍在途的 selfplay（刚下发，deadline 远未到）
        clock.set(1860);
        d.register("a1", task("w1", TaskKind::TaskSelfplay, 4));
        d.mark_done("c1");
        clock.advance(1);
        d.prune(); // e1/e2 → Expired；a1 保持 Assigned；c1 仍在保留期内

        let snap = d.task_state_snapshot();
        assert_eq!(snap.counts.assigned, 1);
        assert_eq!(snap.counts.completed, 1);
        assert_eq!(snap.counts.expired, 2);
        assert_eq!(snap.oldest_assigned_age_seconds, 1, "最老在途 = a1 的年龄");
        // 过期记录按下发时间倒序（最近过期在前）
        assert_eq!(snap.expired.len(), 2);
        assert_eq!(snap.expired[0].task_id, "e2");
        assert_eq!(snap.expired[1].task_id, "e1");
        assert_eq!(snap.expired[0].kind, "TASK_REANALYSIS");
        assert_eq!(snap.expired[0].state, "expired");
        assert_eq!(snap.expired[0].worker_id, "w4");
        assert!(snap.expired[0].age_seconds > 0);
        // 空表
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
            clock.advance(1); // 逐条错开下发时间，保证倒序断言确定
        }
        clock.advance(consts::EVAL_TASK_STALE_AFTER.as_secs() as i64);
        d.prune(); // 40 条全部 → Expired（未达保留期不回收）
        let snap = d.task_state_snapshot();
        assert_eq!(snap.counts.expired, 40);
        assert_eq!(snap.expired.len(), 32, "诊断列表最多 32 条");
        assert_eq!(snap.expired[0].task_id, "t39", "倒序：最近在前");
    }
}
