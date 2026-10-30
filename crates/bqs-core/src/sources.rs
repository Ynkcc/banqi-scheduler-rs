//! 【所有权：core-pipeline 成员】
//!
//! taskSource 接口链：GetTask 的「前置校验 → 平衡评估 → 逐源尝试」中，
//! eval / reanalysis / derive / selfplay 四个任务生产源各为实现，按注册顺序
//! 尝试，返回 `Some(resp)` 即终止。组装失败「log + 释放/保留」由各源自理。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/sources.go`、`tasks.go`。
//! 新增任务类型 = 追加一个实现 + 注册，GetTask 主干零改动。

use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{JobKind, TaskKind, TaskRequest, TaskResponse};

use crate::{
    assembler::{games_for_threads, SelfplayMode, TaskAssembler},
    consts,
    control::Control,
    dispatch::{RunningTask, TaskDispatcher},
    evalcoord::EvalCoordinator,
    featurepack::DeriveQueue,
    jobs::{JobManager, JobSpecJson},
    ports::{JobRow, MatchStore, NetworkStore},
    reanalysis::ReanaQueue,
    CoreResult,
};

/// 任务生产源接口：GetTask 的下发优先级即本接口实现的注册顺序。
///
/// 语义约定（与 Go 版三态对齐为两态）：
/// - `Ok(Some(resp))` 本源已处理本次请求（resp 即最终响应，含 TASK_NONE 空转等待）；
/// - `Ok(None)`       本源当前无可派任务（队列保留，交下一个源）；
/// - `Err(e)`         基础设施故障（读库 / 生成 id 失败）：上抛中断（→ gRPC internal）。
///   组装失败「log + 保留队列待重试」由 eval / reanalysis / derive 各源自理成
///   `Ok(None)`，不上抛。
pub trait TaskSource: Send + Sync {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>>;
}

/// 生成任务 id（16 随机字节的 hex，与 Go 版 newID 同形）。
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

// ---------------------------------------------------------------------------
// job 源（统一 Job 机制：eval / collect / selfplay 的派发统一入口）
// ---------------------------------------------------------------------------

/// JobSource 的工作相位（源链中的位置语义）：
/// - `High`：排在整个源链最前——eval job（默认优先级最高）先于 reanalysis /
///   derive，与旧 EvalSource 的「评估最优先」语义一致；
/// - `Low`：排在 reanalysis / derive 之后——collect / selfplay job 与旧
///   SelfplaySource 的兜底位置一致（selfplay 恒处理请求）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPhase {
    High,
    Low,
}

/// 统一 Job 源：从可派发 job（running 且 completed<total 或 total=0）按优先级
/// （priority DESC、created_at ASC）选源派发。三条路径：
/// - JOB_EVAL：委托 evalc.claim_eval_task 原子认领（在飞额度 + attempts 计数），
///   认领结果按 (network_sha, opponent_kind) 回映射到 eval job 做派发记账；
/// - JOB_COLLECT：按 worker 线程缩放局数（向上取偶成对）、按 job 级单调任务
///   序号解析种子（resolve_seed），经 collect_spec / collect_games / collect_seed
///   下发 TASK_COLLECT；
/// - JOB_SELFPLAY：沿用既有 selfplay 生产路径（规则哨兵 / 混入 / 纯自对弈），
///   仅把派发记账挂到 selfplay job 上（完成口径聚合经 job_tasks 映射）。
///
/// 抢占评估（惰性节流）在每个请求路径上执行：更高优先级 job 变为可派发时，
/// 其提交/恢复路径已即时取消更低优先级在飞；此处节流兜底覆盖「高优先级 job
/// 完成后窗口打开」等未经提交/恢复路径的场景。
pub struct JobSource {
    pub jobs: Arc<JobManager>,
    pub evalc: Arc<EvalCoordinator>,
    pub matches: Arc<dyn MatchStore>,
    pub nets: Arc<dyn NetworkStore>,
    pub disp: Arc<TaskDispatcher>,
    pub reana: Arc<ReanaQueue>,
    pub asm: Arc<TaskAssembler>,
    pub ctl: Arc<Control>,
    pub cfg: Arc<Config>,
    pub phase: JobPhase,
}

impl TaskSource for JobSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        // 抢占评估（内部按 JOB_PREEMPT_INTERVAL 节流；提交/恢复路径已即时评估）
        self.jobs.preemption_sweep(false);
        match self.phase {
            JobPhase::High => {
                // High 相位只处理 eval job（collect/selfplay 交由 reanalysis /
                // derive 之后的 Low 相位，保持既有源链次序语义）。
                self.try_eval(req)
            }
            JobPhase::Low => {
                // 可派发 job 统一按优先级排序（list_jobs 已按 priority DESC、
                // created_at ASC）；逐个尝试直到派发成功。
                let mut candidates: Vec<JobRow> = Vec::new();
                for kind in [JobKind::JobEval, JobKind::JobCollect, JobKind::JobSelfplay] {
                    candidates.extend(self.jobs.dispatchable(kind)?);
                }
                candidates.sort_by(|a, b| b.priority.cmp(&a.priority).then(a.created_at.cmp(&b.created_at)));
                for job in candidates {
                    match JobKind::try_from(job.kind as i32) {
                        Ok(JobKind::JobEval) => {
                            if let Some(resp) = self.try_eval(req)? {
                                return Ok(Some(resp));
                            }
                        }
                        Ok(JobKind::JobCollect) => {
                            if let Some(resp) = self.dispatch_collect(&job, req)? {
                                return Ok(Some(resp));
                            }
                        }
                        Ok(JobKind::JobSelfplay) => {
                            if let Some(resp) = self.dispatch_selfplay(req)? {
                                return Ok(Some(resp));
                            }
                        }
                        _ => {}
                    }
                }
                // 源链尾兜底已由旧 selfplay 语义挪到 JobSource：持续型 selfplay
                // job 恒存在（启动引导），其派发失败（如自动暂停）会在此返回
                // TASK_NONE；走到这里说明 selfplay job 缺失（异常状态），空转等待。
                Ok(None)
            }
        }
    }
}

impl JobSource {
    /// eval job 派发：有可派发 eval job 时委托 evalc 原子认领并回映射记账。
    fn try_eval(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        let eval_jobs = self.jobs.dispatchable(JobKind::JobEval)?;
        if eval_jobs.is_empty() {
            return Ok(None);
        }
        let Some((pending, task_id)) = self.evalc.claim_eval_task(&req.worker_id, req.threads) else {
            return Ok(None);
        };
        // 认领结果回映射到 eval job（evalc 队列与 eval job 一一对应；map 不中 =
        // job 已被控制/收敛，释放认领待下轮）。
        let matched = eval_jobs.iter().find(|j| {
            JobSpecJson::parse(&j.spec_json)
                .map(|s| {
                    matches!(
                        &s,
                        JobSpecJson::Eval(e)
                            if e.network_sha == pending.network_sha
                                && e.opponent_kind == pending.opponent_spec
                    )
                })
                .unwrap_or(false)
        });
        let Some(job) = matched else {
            self.evalc.release_eval_claim(&task_id);
            return Ok(None);
        };
        let games = self.disp.lookup(&task_id).map(|t| t.games).unwrap_or(0);
        match self.asm.eval_task(&task_id, &pending, games, req) {
            Ok(resp) => {
                if let Err(e) = self.jobs.note_eval_claim(&task_id, &job.job_id) {
                    self.evalc.release_eval_claim(&task_id);
                    return Err(e);
                }
                self.disp.set_task_job(&task_id, &job.job_id);
                tracing::info!(
                    "[task] eval assigned worker={} task={} job={} network={} opponent={} games={} sims={}",
                    req.worker_id, task_id, job.job_id, pending.network_sha,
                    pending.opponent_spec, games, self.cfg.eval.mcts_sims
                );
                Ok(Some(resp))
            }
            Err(e) => {
                self.evalc.release_eval_claim(&task_id);
                tracing::warn!("[eval] 本次无法下发（{e}），已释放认领待重试");
                Ok(None)
            }
        }
    }

    /// collect job 派发：线程缩放 + 向上取偶成对 + 逐任务种子解析。
    fn dispatch_collect(&self, job: &JobRow, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        let Some(JobSpecJson::Collect(spec)) = JobSpecJson::parse(&job.spec_json) else {
            tracing::warn!("[job] collect job {} spec_json 解析失败，跳过", job.job_id);
            return Ok(None);
        };
        let proto_spec = spec.to_proto();
        // 单任务局数：[jobs].collect_games_per_task（0 = 回落 selfplay.games_per_task）
        let base = if self.cfg.jobs.collect_games_per_task > 0 {
            self.cfg.jobs.collect_games_per_task
        } else {
            self.cfg.selfplay.games_per_task as i64
        };
        let mut games = games_for_threads(self.cfg.selfplay.threads_baseline, base, req.threads);
        games += games & 1; // 向上取偶：换色配对成对派发
        let seed = crate::jobs::resolve_seed(&spec.seed_strategy, job.next_seq);
        let task_id = new_id();
        let resp = self.asm.collect_task(&task_id, &proto_spec, games as u64, seed);
        self.jobs.note_dispatch(
            &task_id,
            &job.job_id,
            games,
            &req.worker_id,
            TaskKind::TaskCollect,
            "", // 采集任务无单一主网络（归属校验放宽，见 Server::report_episode）
            "",
            "",
            0,
            Some(self.cfg.jobs.collect_deadline_seconds),
        )?;
        tracing::info!(
            "[task] collect assigned worker={} task={} job={} variant={} games={} seed={} data_kind={}",
            req.worker_id, task_id, job.job_id, proto_spec.variant, games, seed, proto_spec.data_kind
        );
        Ok(Some(resp))
    }

    /// selfplay job 派发：沿用既有 selfplay 生产路径，派发记账挂 job。
    /// 返回 Some(TASK_NONE) = 请求已被处理（空转等待，与旧 SelfplaySource 一致）。
    fn dispatch_selfplay(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        // 自动暂停时返回等待（selfplay 是数据生产主通道，停它即止血）。
        if self.ctl.auto_paused() {
            return Ok(Some(TaskResponse {
                kind: TaskKind::TaskNone as i32,
                message: format!("self_play_auto_paused: {}", self.ctl.auto_pause_reason()),
                ..Default::default()
            }));
        }
        // 找到可派发的 selfplay job（持续型引导 job 恒存在；异常缺失时等待）
        let Some(job) = self.jobs.dispatchable(JobKind::JobSelfplay)?.into_iter().next() else {
            return Ok(None);
        };
        let pm = self.matches.pending_match()?;
        let best = self.nets.get_best()?;
        // 规则哨兵开关（[selfplay] best = "rule"）——条件哨兵（冷启动预热）。
        if pm.is_none() && best.is_none() && self.cfg.selfplay.best == consts::RULE_SENTINEL_SHA {
            let task_id = new_id();
            let resp = self.asm.sentinel_task(&task_id, req);
            self.reana.note_selfplay_assigned();
            self.jobs.note_dispatch(
                &task_id, &job.job_id, resp.games as i64, &req.worker_id,
                TaskKind::TaskSelfplay, consts::RULE_SENTINEL_SHA, "", "", 0, None,
            )?;
            tracing::info!(
                "[task] selfplay(rule-sentinel) assigned worker={} task={} job={} games={} data_kind=nnue",
                req.worker_id, task_id, job.job_id, resp.games
            );
            return Ok(Some(resp));
        }
        let sel = self.asm.resolve_selfplay(best, pm, self.ctl.data_kind() as i32)?;
        if sel.mode == SelfplayMode::None {
            return Ok(Some(TaskResponse {
                kind: TaskKind::TaskNone as i32,
                message: sel.none_message,
                ..Default::default()
            }));
        }
        let task_id = new_id();
        let (resp, match_id) = self.asm.selfplay_task(&task_id, req, &sel)?;
        self.reana.note_selfplay_assigned(); // 重搜节流计数
        let producer_sha = sel.producer.as_ref().map(|p| p.sha.clone()).unwrap_or_default();
        self.jobs.note_dispatch(
            &task_id,
            &job.job_id,
            resp.games as i64,
            &req.worker_id,
            TaskKind::TaskSelfplay,
            &producer_sha,
            &resp.opponent_sha,
            &resp.opponent_spec,
            match_id,
            None,
        )?;
        if match_id != 0 {
            tracing::info!(
                "[task] selfplay(mix) assigned worker={} task={} job={} match={} candidate={} games={} data_kind={}",
                req.worker_id, task_id, job.job_id, match_id, producer_sha, resp.games,
                resp.params.as_ref().map(|p| p.data_kind).unwrap_or_default()
            );
        } else {
            tracing::info!(
                "[task] selfplay assigned worker={} task={} job={} network={} games={}",
                req.worker_id, task_id, job.job_id, producer_sha, resp.games
            );
        }
        Ok(Some(resp))
    }
}

// ---------------------------------------------------------------------------
// reanalysis 源
// ---------------------------------------------------------------------------

/// 局面重搜源：按间隔节流下发，与自对弈共享算力。
pub struct ReanaSource {
    pub reana: Arc<ReanaQueue>,
    pub disp: Arc<TaskDispatcher>,
    pub asm: Arc<TaskAssembler>,
    pub ctl: Arc<Control>,
    pub cfg: Arc<Config>,
}

impl TaskSource for ReanaSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        // 自动暂停时可一并扣住（[balance] pause_reanalysis）：重搜产物同样要经 trainer 消费。
        if self.ctl.auto_paused() && self.cfg.balance.pause_reanalysis {
            return Ok(None);
        }
        // 放在 selfplay 之前——队列里的位置越早重搜收益越大（旧局面 + 当前网络）。
        let Some(item) = self.reana.peek() else {
            return Ok(None);
        };
        let task_id = new_id();
        match self.asm.reanalysis_task(&task_id, &item, req) {
            Ok(resp) => {
                self.reana.commit();
                self.disp.register(
                    &task_id,
                    RunningTask {
                        network_sha: resp.network_sha.clone(),
                        games: item.positions as i64,
                        worker_id: req.worker_id.clone(),
                        ..RunningTask::new(TaskKind::TaskReanalysis)
                    },
                );
                tracing::info!(
                    "[task] reanalysis assigned worker={} task={} network={} positions={}",
                    req.worker_id, task_id, resp.network_sha, item.positions
                );
                Ok(Some(resp))
            }
            Err(e) => {
                // 队首保留待下次重试（组装失败不消费，不丢 trainer 提交的数据）
                tracing::warn!("[reanalysis] 本次无法下发（{e}），保留队列待重试");
                Ok(None)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// derive 源
// ---------------------------------------------------------------------------

/// 特征包补派源：排在 selfplay 之前（补派是训练消费的阻塞点，优先消化积压；
/// 与重搜不争抢搜索算力，无需节流）。
pub struct DeriveSource {
    pub derive: Arc<DeriveQueue>,
    pub disp: Arc<TaskDispatcher>,
    pub asm: Arc<TaskAssembler>,
}

impl TaskSource for DeriveSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        let Some(item) = self.derive.peek() else {
            return Ok(None);
        };
        let task_id = new_id();
        match self.asm.derive_task(&task_id, &item) {
            Ok(resp) => {
                self.derive.commit();
                self.disp.register(
                    &task_id,
                    RunningTask {
                        games: 1,
                        worker_id: req.worker_id.clone(),
                        ..RunningTask::new(TaskKind::TaskDeriveFeatures)
                    },
                );
                tracing::info!(
                    "[task] derive assigned worker={} task={} episode={} pack_v{}",
                    req.worker_id, task_id, item.episode_object_key, item.pack_version
                );
                Ok(Some(resp))
            }
            Err(e) => {
                tracing::warn!("[derive] 本次无法下发（{e}），保留队列待重试");
                Ok(None)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 源链装配
// ---------------------------------------------------------------------------

/// 源链装配器：按优先级返回任务生产源。
///
/// 统一 Job 机制后的链：`JobSource(High: eval job) > reanalysis > derive >
/// JobSource(Low: collect / selfplay job)`。eval job（默认优先级 100）先于重搜
/// 与补派（与旧 EvalSource 一致）；collect / selfplay job 与旧 selfplay 源同为
/// 兜底位。派发不受在飞封顶 / 优先级抢占见 `jobs::JobManager` 与 proto 文件头。
pub struct Sources;

impl Sources {
    pub fn assemble(
        jobs: Arc<JobManager>,
        evalc: Arc<EvalCoordinator>,
        reana: Arc<ReanaQueue>,
        derive: Arc<DeriveQueue>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        disp: Arc<TaskDispatcher>,
        asm: Arc<TaskAssembler>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
    ) -> Vec<Box<dyn TaskSource>> {
        let mk = |phase: JobPhase| Box::new(JobSource {
            jobs: jobs.clone(),
            evalc: evalc.clone(),
            matches: matches.clone(),
            nets: nets.clone(),
            disp: disp.clone(),
            reana: reana.clone(),
            asm: asm.clone(),
            ctl: ctl.clone(),
            cfg: cfg.clone(),
            phase,
        }) as Box<dyn TaskSource>;
        vec![
            mk(JobPhase::High),
            Box::new(ReanaSource {
                reana: reana.clone(),
                disp: disp.clone(),
                asm: asm.clone(),
                ctl: ctl.clone(),
                cfg: cfg.clone(),
            }),
            Box::new(DeriveSource { derive, disp: disp.clone(), asm: asm.clone() }),
            mk(JobPhase::Low),
        ]
    }
}
