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
use bqs_proto::{TaskKind, TaskRequest, TaskResponse};

use crate::{
    assembler::{SelfplayMode, TaskAssembler},
    consts,
    control::Control,
    dispatch::{RunningTask, TaskDispatcher},
    evalcoord::EvalCoordinator,
    featurepack::DeriveQueue,
    ports::{MatchStore, NetworkStore},
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
// eval 源（优先级最高）
// ---------------------------------------------------------------------------

/// TASK_EVAL 源（优先级最高）：绝对强度评估先于重搜与自对弈——成本低
/// （纯策略秒级），越早拿到趋势越能及早发现「练了很久却没变强」。
pub struct EvalSource {
    pub evalc: Arc<EvalCoordinator>,
    pub disp: Arc<TaskDispatcher>,
    pub asm: Arc<TaskAssembler>,
    pub cfg: Arc<Config>,
}

impl TaskSource for EvalSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        // 流式生产模型：作业记目标局数，本处每次派发 per-task 额度。认领是原子的
        //（claim_eval_task 内含在飞额度检查 + 经 disp.register_in_flight 登记在飞，
        // 锁序 evalc.mu → disp.mu）。
        let Some((job, task_id)) = self.evalc.claim_eval_task(&req.worker_id, req.threads) else {
            return Ok(None);
        };
        // 分片局数已按净剩余与线程缩放截断，由任务表快照回查
        let games = self.disp.lookup(&task_id).map(|t| t.games).unwrap_or(0);
        match self.asm.eval_task(&task_id, &job, games, req) {
            Ok(resp) => {
                tracing::info!(
                    "[task] eval assigned worker={} task={} network={} opponent={} games={} sims={}",
                    req.worker_id, task_id, job.network_sha, job.opponent_spec, games, self.cfg.eval.mcts_sims
                );
                Ok(Some(resp))
            }
            Err(e) => {
                // 组装失败（如网络已被清理）：释放本次认领，队列保留待下轮重试
                self.evalc.release_eval_claim(&task_id);
                tracing::warn!("[eval] 本次无法下发（{e}），已释放认领待重试");
                Ok(None)
            }
        }
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
// selfplay 源（兜底）
// ---------------------------------------------------------------------------

/// 兜底源：混入模式 / 规则哨兵 / 纯自对弈的最终归属，恒处理请求
/// （除非读库 / 预签名等基础设施失败上抛）。
pub struct SelfplaySource {
    pub matches: Arc<dyn MatchStore>,
    pub nets: Arc<dyn NetworkStore>,
    pub disp: Arc<TaskDispatcher>,
    pub reana: Arc<ReanaQueue>,
    pub asm: Arc<TaskAssembler>,
    pub ctl: Arc<Control>,
    pub cfg: Arc<Config>,
}

impl TaskSource for SelfplaySource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        // 常规 selfplay：自动暂停时返回等待（selfplay 是数据生产主通道，停它即止血）。
        if self.ctl.auto_paused() {
            return Ok(Some(TaskResponse {
                kind: TaskKind::TaskNone as i32,
                message: format!("self_play_auto_paused: {}", self.ctl.auto_pause_reason()),
                ..Default::default()
            }));
        }

        let pm = self.matches.pending_match()?;
        let best = self.nets.get_best()?;
        // 规则哨兵开关（[selfplay] best = "rule"）——**条件哨兵**：仅在谱系尚无 best
        // 网络时下发规则哨兵任务（冷启动预热：Expectimax+规则评估器蒸馏教师，无需
        // R2 权重）。首个真实网络晋级 best 后自动转入 NNUE 自对弈（无需改配置/重启）
        // ——预热 → 自对弈的阶段切换由「best 是否存在」这一个事实驱动。存在混入
        // 对打时保持既有路径。
        if pm.is_none()
            && best.is_none()
            && self.cfg.selfplay.best == consts::RULE_SENTINEL_SHA
        {
            return self.assign_sentinel(req);
        }
        // 产数据方决策（候选 / 架构不符换道 / 无可用网络的判定）收敛在组装层。
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
        self.disp.register(
            &task_id,
            RunningTask {
                match_id,
                network_sha: producer_sha.clone(),
                opponent_sha: resp.opponent_sha.clone(),
                opponent_spec: resp.opponent_spec.clone(),
                games: resp.games as i64,
                worker_id: req.worker_id.clone(),
                ..RunningTask::new(TaskKind::TaskSelfplay)
            },
        );
        if match_id != 0 {
            tracing::info!(
                "[task] selfplay(mix) assigned worker={} task={} match={} candidate={} opponent={} games={} data_kind={}",
                req.worker_id, task_id, match_id, producer_sha, resp.opponent_sha, resp.games,
                resp.params.as_ref().map(|p| p.data_kind).unwrap_or_default()
            );
        } else {
            tracing::info!(
                "[task] selfplay assigned worker={} task={} network={} games={}",
                req.worker_id, task_id, producer_sha, resp.games
            );
        }
        Ok(Some(resp))
    }
}

impl SelfplaySource {
    /// 下发规则哨兵 selfplay 任务（组装见 TaskAssembler::sentinel_task）；
    /// 登记与节流口径与常规 selfplay 一致（note_selfplay_assigned 供重搜节流计数）。
    fn assign_sentinel(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        let task_id = new_id();
        let resp = self.asm.sentinel_task(&task_id, req);
        self.reana.note_selfplay_assigned();
        self.disp.register(
            &task_id,
            RunningTask {
                network_sha: consts::RULE_SENTINEL_SHA.to_string(),
                games: resp.games as i64,
                worker_id: req.worker_id.clone(),
                ..RunningTask::new(TaskKind::TaskSelfplay)
            },
        );
        tracing::info!(
            "[task] selfplay(rule-sentinel) assigned worker={} task={} games={} data_kind=nnue",
            req.worker_id, task_id, resp.games
        );
        Ok(Some(resp))
    }
}

// ---------------------------------------------------------------------------
// 源链装配
// ---------------------------------------------------------------------------

/// 源链装配器：按优先级返回任务生产源（eval > reanalysis > derive > selfplay）。
pub struct Sources;

impl Sources {
    pub fn assemble(
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
        vec![
            Box::new(EvalSource { evalc, disp: disp.clone(), asm: asm.clone(), cfg: cfg.clone() }),
            Box::new(ReanaSource {
                reana: reana.clone(),
                disp: disp.clone(),
                asm: asm.clone(),
                ctl: ctl.clone(),
                cfg: cfg.clone(),
            }),
            Box::new(DeriveSource { derive, disp: disp.clone(), asm: asm.clone() }),
            Box::new(SelfplaySource { matches, nets, disp, reana, asm, ctl, cfg }),
        ]
    }
}
