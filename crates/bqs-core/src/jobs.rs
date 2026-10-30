//! 【所有权：scheduler-dev 成员】
//!
//! JobManager：统一 Job 机制（collect / selfplay / eval 三类 job 的登记、控制、
//! 派发记账、完成口径聚合、优先级抢占与在途取消）。
//!
//! 契约：`proto/scheduler.proto` 文件头「统一 Job 机制」注释段（禁止在本文件
//! 复述口径，语义以 proto 为准）。要点回顾：
//! - completed_games 只按已收到的真实上报聚合（collect/selfplay 走 ReportEpisode、
//!   eval 走 ReportMatchResult），不含在飞；task→job 映射落库（job_tasks），
//!   先落库后更新计数，撑住重启后的迟到上报聚合；
//! - 派发不受在飞封顶：只要 completed < total（或 total=0）就继续按 worker
//!   线程缩放派发；
//! - completed ≥ total（total>0）即置 completed，并立即把该 job 全部在飞任务
//!   加入取消集合（经 HeartbeatReply.cancel_task_ids 下发，按 worker 清除）；
//! - 优先级抢占：更高优先级 job 变为可派发（提交/恢复）时立即取消全部更低
//!   优先级 job 的在飞任务；GetTask 路径按 JOB_PREEMPT_INTERVAL 节流惰性兜底；
//! - 暂停/恢复/取消状态机：running ⇄ paused；cancel → 终态 cancelled；终态
//!   （completed/cancelled）不可再控制（accepted=false）。
//!
//! 锁约定：本子系统不嵌套获取其他子系统锁——需要 disp 快照时先取快照再进本
//! 子系统的 inner 锁（disp 是叶子锁；evalc.mu → 本子系统 mu 单向，见
//! evalcoord::set_job_hook 的回调路径）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bqs_config::Config;
use bqs_proto::{
    CollectPlayerSpec, CollectSearchParams, CollectionJobSpec, EvalJobSpec, JobKind,
    OpeningScenario, RatingJobSpec, SeedStrategy, SeedStrategyKind, SelfPlayJobSpec, SubmitJobRequest,
};

use crate::{
    consts,
    control::Control,
    dispatch::TaskDispatcher,
    ports::{Clock, JobRow, JobStore, MatchStore, NetworkStore},
    CoreResult,
};

// ---------------------------------------------------------------------------
// 规格回显（spec_json 的载体；serde JSON 落库，字段名即 API 契约）
// ---------------------------------------------------------------------------

/// 搜索参数回显（0 值 = 回落 worker 本地默认，序列化不省略以便对照）。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SearchSpecJson {
    pub mcts_sims: u32,
    pub mcts_max_considered_actions: u32,
    pub mcts_c_scale: f64,
    pub mcts_gumbel_scale: f64,
    pub temperature: f32,
    pub playout_cap_random: bool,
    pub expectimax_node_budget: u64,
    pub expectimax_max_depth: u32,
    pub labeler_mode: String,
    pub labeler_budget_mult: u32,
    pub expl_enabled: bool,
    pub expl_topk: u32,
    pub expl_temp_start: f64,
    pub expl_temp_end: f64,
    pub expl_temp_decay_steps: u32,
}

impl SearchSpecJson {
    fn from_proto(s: Option<CollectSearchParams>) -> Self {
        match s {
            Some(s) => Self {
                mcts_sims: s.mcts_sims,
                mcts_max_considered_actions: s.mcts_max_considered_actions,
                mcts_c_scale: s.mcts_c_scale,
                mcts_gumbel_scale: s.mcts_gumbel_scale,
                temperature: s.temperature,
                playout_cap_random: s.playout_cap_random,
                expectimax_node_budget: s.expectimax_node_budget,
                expectimax_max_depth: s.expectimax_max_depth,
                labeler_mode: s.labeler_mode,
                labeler_budget_mult: s.labeler_budget_mult,
                expl_enabled: s.expl_enabled,
                expl_topk: s.expl_topk,
                expl_temp_start: s.expl_temp_start,
                expl_temp_end: s.expl_temp_end,
                expl_temp_decay_steps: s.expl_temp_decay_steps,
            },
            None => Self::default(),
        }
    }

    fn to_proto(&self) -> Option<CollectSearchParams> {
        let s = &self;
        if s.mcts_sims == 0
            && s.mcts_max_considered_actions == 0
            && s.mcts_c_scale == 0.0
            && s.mcts_gumbel_scale == 0.0
            && s.temperature == 0.0
            && !s.playout_cap_random
            && s.expectimax_node_budget == 0
            && s.expectimax_max_depth == 0
            && s.labeler_mode.is_empty()
            && s.labeler_budget_mult == 0
            && !s.expl_enabled
            && s.expl_topk == 0
            && s.expl_temp_start == 0.0
            && s.expl_temp_end == 0.0
            && s.expl_temp_decay_steps == 0
        {
            return None;
        }
        Some(CollectSearchParams {
            mcts_sims: s.mcts_sims,
            mcts_max_considered_actions: s.mcts_max_considered_actions,
            mcts_c_scale: s.mcts_c_scale,
            mcts_gumbel_scale: s.mcts_gumbel_scale,
            temperature: s.temperature,
            playout_cap_random: s.playout_cap_random,
            expectimax_node_budget: s.expectimax_node_budget,
            expectimax_max_depth: s.expectimax_max_depth,
            labeler_mode: s.labeler_mode.clone(),
            labeler_budget_mult: s.labeler_budget_mult,
            expl_enabled: s.expl_enabled,
            expl_topk: s.expl_topk,
            expl_temp_start: s.expl_temp_start,
            expl_temp_end: s.expl_temp_end,
            expl_temp_decay_steps: s.expl_temp_decay_steps,
        })
    }
}

/// 单方棋手规格回显。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct PlayerSpecJson {
    pub kind: String,
    pub network_sha: String,
    pub search: Option<SearchSpecJson>,
}

impl PlayerSpecJson {
    fn from_proto(p: Option<CollectPlayerSpec>) -> Self {
        match p {
            Some(p) => Self {
                kind: p.kind,
                network_sha: p.network_sha,
                search: p.search.map(|s| SearchSpecJson::from_proto(Some(s))),
            },
            None => Self::default(),
        }
    }

    fn to_proto(&self) -> CollectPlayerSpec {
        CollectPlayerSpec {
            kind: self.kind.clone(),
            network_sha: self.network_sha.clone(),
            search: self.search.as_ref().and_then(|s| s.to_proto()),
        }
    }
}

/// 种子策略回显。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SeedStrategyJson {
    pub kind: i32,
    pub fixed_seed: u64,
    pub base_seed: u64,
    pub min_seed: u64,
    pub max_seed: u64,
}

impl SeedStrategyJson {
    fn from_proto(s: Option<SeedStrategy>) -> Self {
        match s {
            Some(s) => Self {
                kind: s.kind,
                fixed_seed: s.fixed_seed,
                base_seed: s.base_seed,
                min_seed: s.min_seed,
                max_seed: s.max_seed,
            },
            None => Self::default(),
        }
    }

    fn to_proto(&self) -> SeedStrategy {
        SeedStrategy {
            kind: self.kind,
            fixed_seed: self.fixed_seed,
            base_seed: self.base_seed,
            min_seed: self.min_seed,
            max_seed: self.max_seed,
        }
    }
}

/// JOB_COLLECT 规格回显。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct CollectSpecJson {
    pub variant: String,
    pub scenario: i32,
    pub seed_strategy: SeedStrategyJson,
    pub player_a: PlayerSpecJson,
    pub player_b: PlayerSpecJson,
    pub data_kind: i32,
    pub total_games: u64,
}

impl CollectSpecJson {
    fn from_proto(s: CollectionJobSpec) -> Self {
        Self {
            variant: s.variant,
            scenario: s.scenario,
            seed_strategy: SeedStrategyJson::from_proto(s.seed_strategy),
            player_a: PlayerSpecJson::from_proto(s.player_a),
            player_b: PlayerSpecJson::from_proto(s.player_b),
            data_kind: s.data_kind,
            total_games: s.total_games,
        }
    }

    pub fn to_proto(&self) -> CollectionJobSpec {
        CollectionJobSpec {
            variant: self.variant.clone(),
            scenario: self.scenario,
            seed_strategy: Some(self.seed_strategy.to_proto()),
            player_a: Some(self.player_a.to_proto()),
            player_b: Some(self.player_b.to_proto()),
            data_kind: self.data_kind,
            total_games: self.total_games,
        }
    }
}

/// JOB_SELFPLAY 规格回显。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SelfPlaySpecJson {
    pub variant: String,
    pub data_kind: i32,
    pub network_sha: String,
    pub opponent_sha: String,
    pub mix_ratio: f64,
    pub search: Option<SearchSpecJson>,
    pub total_games: u64,
}

impl SelfPlaySpecJson {
    fn from_proto(s: SelfPlayJobSpec) -> Self {
        Self {
            variant: s.variant,
            data_kind: s.data_kind,
            network_sha: s.network_sha,
            opponent_sha: s.opponent_sha,
            mix_ratio: s.mix_ratio,
            search: s.search.map(|x| SearchSpecJson::from_proto(Some(x))),
            total_games: s.total_games,
        }
    }

    pub fn to_proto(&self) -> SelfPlayJobSpec {
        SelfPlayJobSpec {
            variant: self.variant.clone(),
            data_kind: self.data_kind,
            network_sha: self.network_sha.clone(),
            opponent_sha: self.opponent_sha.clone(),
            mix_ratio: self.mix_ratio,
            search: self.search.as_ref().and_then(|s| s.to_proto()),
            total_games: self.total_games,
        }
    }
}

/// JOB_EVAL 规格回显。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct EvalSpecJson {
    pub variant: String,
    pub network_sha: String,
    pub opponent_kind: String,
    pub opponent_network_sha: String,
    pub search: Option<SearchSpecJson>,
    pub total_games: u64,
}

impl EvalSpecJson {
    fn from_proto(s: EvalJobSpec) -> Self {
        Self {
            variant: s.variant,
            network_sha: s.network_sha,
            opponent_kind: s.opponent_kind,
            opponent_network_sha: s.opponent_network_sha,
            search: s.search.map(|x| SearchSpecJson::from_proto(Some(x))),
            total_games: s.total_games,
        }
    }

    pub fn to_proto(&self) -> EvalJobSpec {
        EvalJobSpec {
            variant: self.variant.clone(),
            network_sha: self.network_sha.clone(),
            opponent_kind: self.opponent_kind.clone(),
            opponent_network_sha: self.opponent_network_sha.clone(),
            search: self.search.as_ref().and_then(|s| s.to_proto()),
            total_games: self.total_games,
        }
    }
}

/// JOB_RATING 规格回显（match_id 为调度器运行时回写字段：提交时解析对手并
/// 建对打后记录，取消/聚合据此定位对打行；不在 proto 契约内）。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RatingSpecJson {
    pub variant: String,
    pub candidate_sha: String,
    pub opponent_sha: String,
    pub total_games: u64,
    pub sprt_elo0: f64,
    pub sprt_elo1: f64,
    pub sprt_alpha: f64,
    pub sprt_beta: f64,
    pub match_id: i64,
}

impl RatingSpecJson {
    fn from_proto(s: RatingJobSpec) -> Self {
        Self {
            variant: s.variant,
            candidate_sha: s.candidate_sha,
            opponent_sha: s.opponent_sha,
            total_games: s.total_games,
            sprt_elo0: s.sprt_elo0,
            sprt_elo1: s.sprt_elo1,
            sprt_alpha: s.sprt_alpha,
            sprt_beta: s.sprt_beta,
            match_id: 0,
        }
    }

    pub fn to_proto(&self) -> RatingJobSpec {
        RatingJobSpec {
            variant: self.variant.clone(),
            candidate_sha: self.candidate_sha.clone(),
            opponent_sha: self.opponent_sha.clone(),
            total_games: self.total_games,
            sprt_elo0: self.sprt_elo0,
            sprt_elo1: self.sprt_elo1,
            sprt_alpha: self.sprt_alpha,
            sprt_beta: self.sprt_beta,
        }
    }
}

/// spec_json 的统一载体（kind 决定哪个字段有效）。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobSpecJson {
    Collect(CollectSpecJson),
    SelfPlay(SelfPlaySpecJson),
    Eval(EvalSpecJson),
    Rating(RatingSpecJson),
}

impl JobSpecJson {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn parse(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}

// ---------------------------------------------------------------------------
// 种子策略解析（纯函数，逐任务确定性）
// ---------------------------------------------------------------------------

/// 由 job 级单调任务序号 seq 解析本任务的开局种子。
/// - FIXED：恒 fixed_seed；
/// - RANDOM：base_seed 为偏移基值，按 seq 哈希派生（splitmix64 终散射）；
/// - INCREMENTAL：base_seed + seq；
/// - RANGE：min + 哈希 % (max - min + 1)（含端点；min>max 时回落 min）。
pub fn resolve_seed(strategy: &SeedStrategyJson, seq: i64) -> u64 {
    let seq = seq.max(0) as u64;
    match SeedStrategyKind::try_from(strategy.kind) {
        Ok(SeedStrategyKind::SeedFixed) => strategy.fixed_seed,
        Ok(SeedStrategyKind::SeedRandom) => splitmix64(strategy.base_seed ^ splitmix64(seq)),
        Ok(SeedStrategyKind::SeedIncremental) => strategy.base_seed.wrapping_add(seq),
        Ok(SeedStrategyKind::SeedRange) => {
            if strategy.max_seed <= strategy.min_seed {
                strategy.min_seed
            } else {
                let span = strategy.max_seed - strategy.min_seed;
                strategy.min_seed + splitmix64(strategy.base_seed ^ splitmix64(seq)) % (span + 1)
            }
        }
        _ => strategy.fixed_seed,
    }
}

/// splitmix64 终散射（无外部依赖的确定性哈希；种子派生专用）。
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// 提交校验（纯函数）
// ---------------------------------------------------------------------------

/// 校验棋手规格：kind 词表（bq-policy player_kind 单一真源，接受历史别名）+
/// 模型类必须登记网络 / 非模型类不得带 sha。
fn validate_player(p: &PlayerSpecJson, nets: &dyn NetworkStore) -> Result<(), String> {
    let Some(kind) = bq_policy::player_kind::canonical(&p.kind) else {
        return Err(format!(
            "invalid_player_kind=\"{}\"（可选 {}）",
            p.kind,
            bq_policy::player_kind::PLAYER_KINDS.join("/")
        ));
    };
    let is_model = bq_policy::player_kind::requires_model(kind);
    if is_model {
        if p.network_sha.is_empty() {
            return Err(format!("player_kind={kind} 需要 network_sha"));
        }
        let registered = nets
            .get_network(&p.network_sha)
            .map_err(|e| format!("查询网络登记失败: {e}"))?;
        if registered.is_none() {
            return Err(format!(
                "network_not_registered sha={}（模型类棋手的网络必须已登记）",
                p.network_sha
            ));
        }
    } else if !p.network_sha.is_empty() {
        return Err(format!("player_kind={kind} 的 network_sha 必须为空"));
    }
    Ok(())
}

/// 校验变体（JOB_VARIANTS 词表）。
fn validate_variant(v: &str) -> Result<(), String> {
    if consts::JOB_VARIANTS.contains(&v) {
        Ok(())
    } else {
        Err(format!("invalid_variant=\"{v}\"（可选 {}）", consts::JOB_VARIANTS.join("/")))
    }
}

// ---------------------------------------------------------------------------
// JobManager
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Inner {
    /// 在途取消集合：task_id → worker_id（经心跳下发后移除；任务消失即移除）。
    cancelled: HashMap<String, String>,
    /// 上次抢占评估时间（节流相位；None = 尚未扫过）。
    last_preempt: Option<i64>,
}

pub struct JobManager {
    store: Arc<dyn JobStore>,
    disp: Arc<TaskDispatcher>,
    nets: Arc<dyn NetworkStore>,
    matches: Arc<dyn MatchStore>,
    ctl: Arc<Control>,
    cfg: Arc<Config>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
}

impl JobManager {
    pub fn new(
        store: Arc<dyn JobStore>,
        disp: Arc<TaskDispatcher>,
        nets: Arc<dyn NetworkStore>,
        matches: Arc<dyn MatchStore>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self { store, disp, nets, matches, ctl, cfg, clock, inner: Mutex::new(Inner::default()) }
    }

    fn now(&self) -> i64 {
        self.clock.now()
    }

    fn default_priority(&self, kind: JobKind) -> i64 {
        match kind {
            JobKind::JobSelfplay => self.cfg.jobs.priority_selfplay,
            JobKind::JobCollect => self.cfg.jobs.priority_collect,
            JobKind::JobEval => self.cfg.jobs.priority_eval,
            JobKind::JobRating => self.cfg.jobs.priority_rating,
        }
    }

    // ---- 提交 --------------------------------------------------------------

    /// 提交 job：校验 → 登记（state=running）→ 即时抢占评估。业务拒绝返回
    /// (false, 原因)；真故障（读库失败）上抛。
    pub fn submit(&self, req: &SubmitJobRequest) -> CoreResult<(bool, String, String)> {
        let Ok(kind) = JobKind::try_from(req.kind) else {
            return Ok((false, format!("invalid_job_kind={}", req.kind), String::new()));
        };
        let priority = if req.priority == 0 { self.default_priority(kind) } else { req.priority as i64 };
        let spec: JobSpecJson = match kind {
            JobKind::JobCollect => {
                let Some(c) = &req.collect else {
                    return Ok((false, "collect spec required for JOB_COLLECT".into(), String::new()));
                };
                match self.validate_collect(c) {
                    Ok(()) => JobSpecJson::Collect(CollectSpecJson::from_proto(c.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobSelfplay => {
                let Some(s) = &req.selfplay else {
                    return Ok((false, "selfplay spec required for JOB_SELFPLAY".into(), String::new()));
                };
                match self.validate_selfplay(s) {
                    Ok(()) => JobSpecJson::SelfPlay(SelfPlaySpecJson::from_proto(s.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobEval => {
                let Some(e) = &req.eval else {
                    return Ok((false, "eval spec required for JOB_EVAL".into(), String::new()));
                };
                match self.validate_eval(e) {
                    Ok(()) => JobSpecJson::Eval(EvalSpecJson::from_proto(e.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobRating => {
                let Some(r) = &req.rating else {
                    return Ok((false, "rating spec required for JOB_RATING".into(), String::new()));
                };
                // 校验 → 解析对手（空 = 当前 best）→ 建对打行（六桶聚合载体）。
                if let Err(m) = self.validate_rating(r) {
                    return Ok((false, m, String::new()));
                }
                let opponent_sha = if r.opponent_sha.is_empty() {
                    match self.nets.get_best()? {
                        Some(b) => b.sha,
                        None => {
                            return Ok((false, "no_best_network_registered（对手空 = 当前 best，但尚无 best 网络）".into(), String::new()))
                        }
                    }
                } else {
                    r.opponent_sha.clone()
                };
                let mut spec = RatingSpecJson::from_proto(r.clone());
                // 对打目标（换色成对数）= 局数上限的一半；0 = 仅由 SPRT 判停。
                let match_id = self.matches.create_match(
                    &r.candidate_sha,
                    &opponent_sha,
                    (spec.total_games / 2) as i64,
                )?;
                spec.match_id = match_id;
                tracing::info!(
                    "[job] rating match created match={} candidate={} opponent={} target_pairs={}",
                    match_id, spec.candidate_sha, opponent_sha, spec.total_games / 2
                );
                JobSpecJson::Rating(spec)
            }
        };
        let job_id = crate::sources::new_id();
        let now = self.now();
        self.store.insert_job(JobRow {
            job_id: job_id.clone(),
            kind: kind as i64,
            priority,
            state: "running".into(),
            total_games: spec_total(&spec) as i64,
            completed_games: 0,
            dispatched_games: 0,
            spec_json: spec.to_json(),
            created_at: now,
            updated_at: now,
            next_seq: 0,
        })?;
        tracing::info!(
            "[job] submitted requester={} id={} kind={} priority={} total={}",
            req.requester, job_id, kind.as_str_name(), priority, spec_total(&spec)
        );
        // 即时抢占评估：本 job 变为可派发，立即取消更低优先级 job 的在飞任务。
        self.preemption_sweep(true);
        Ok((true, String::new(), job_id))
    }

    fn validate_collect(&self, c: &CollectionJobSpec) -> Result<(), String> {
        validate_variant(&c.variant)?;
        if c.total_games < 2 || c.total_games % 2 != 0 {
            return Err(format!("collect.total_games 必须 ≥2 且为偶数，当前 {}", c.total_games));
        }
        let nets = self.nets.as_ref();
        let pa = c
            .player_a
            .as_ref()
            .map(|p| PlayerSpecJson::from_proto(Some(p.clone())))
            .ok_or("collect.player_a required")?;
        let pb = c
            .player_b
            .as_ref()
            .map(|p| PlayerSpecJson::from_proto(Some(p.clone())))
            .ok_or("collect.player_b required")?;
        validate_player(&pa, nets)?;
        validate_player(&pb, nets)?;
        if c.data_kind != bqs_proto::DataKind::DataResnet as i32
            && c.data_kind != bqs_proto::DataKind::DataNnue as i32
        {
            return Err("collect.data_kind 必须为 DATA_RESNET / DATA_NNUE".into());
        }
        Ok(())
    }

    fn validate_selfplay(&self, s: &SelfPlayJobSpec) -> Result<(), String> {
        validate_variant(&s.variant)?;
        if s.data_kind != bqs_proto::DataKind::DataResnet as i32
            && s.data_kind != bqs_proto::DataKind::DataNnue as i32
        {
            return Err("selfplay.data_kind 必须为 DATA_RESNET / DATA_NNUE".into());
        }
        // network_sha / opponent_sha 为空 = 当前 best / 无混入；非空必须已登记。
        for (label, sha) in [("network_sha", &s.network_sha), ("opponent_sha", &s.opponent_sha)] {
            if !sha.is_empty() {
                let registered = self
                    .nets
                    .get_network(sha)
                    .map_err(|e| format!("查询网络登记失败: {e}"))?;
                if registered.is_none() {
                    return Err(format!("network_not_registered {label}={sha}"));
                }
            }
        }
        Ok(())
    }

    fn validate_eval(&self, e: &EvalJobSpec) -> Result<(), String> {
        validate_variant(&e.variant)?;
        if e.total_games < 2 || e.total_games % 2 != 0 {
            return Err(format!("eval.total_games 必须 ≥2 且为偶数，当前 {}", e.total_games));
        }
        let Some(kind) = bq_policy::player_kind::canonical(&e.opponent_kind) else {
            return Err(format!(
                "invalid_opponent_kind=\"{}\"（可选 {}）",
                e.opponent_kind,
                bq_policy::player_kind::PLAYER_KINDS.join("/")
            ));
        };
        let is_model = bq_policy::player_kind::requires_model(kind);
        if is_model {
            if e.opponent_network_sha.is_empty() {
                return Err("eval.opponent_network_sha required for model opponent".into());
            }
            let registered = self
                .nets
                .get_network(&e.opponent_network_sha)
                .map_err(|err| format!("查询网络登记失败: {err}"))?;
            if registered.is_none() {
                return Err(format!(
                    "network_not_registered sha={}（eval 对手网络必须已登记）",
                    e.opponent_network_sha
                ));
            }
        } else if !e.opponent_network_sha.is_empty() {
            return Err("eval.opponent_network_sha 必须为空（非模型类对手）".into());
        }
        // network_sha 为空 = 当前 best；非空必须已登记。
        if !e.network_sha.is_empty() {
            let registered = self
                .nets
                .get_network(&e.network_sha)
                .map_err(|err| format!("查询网络登记失败: {err}"))?;
            if registered.is_none() {
                return Err(format!("network_not_registered sha={}", e.network_sha));
            }
        }
        Ok(())
    }

    fn validate_rating(&self, r: &RatingJobSpec) -> Result<(), String> {
        // 变体空 = 回落调度器 selfplay.variant；非空必须合法。
        if !r.variant.is_empty() {
            validate_variant(&r.variant)?;
        }
        if r.candidate_sha.is_empty() {
            return Err("rating.candidate_sha required（被测网络必填）".into());
        }
        let registered = self
            .nets
            .get_network(&r.candidate_sha)
            .map_err(|err| format!("查询网络登记失败: {err}"))?;
        if registered.is_none() {
            return Err(format!(
                "network_not_registered sha={}（rating 候选网络必须已登记）",
                r.candidate_sha
            ));
        }
        // 对手空 = 当前 best（提交时解析）；非空必须已登记。
        if !r.opponent_sha.is_empty() {
            let registered = self
                .nets
                .get_network(&r.opponent_sha)
                .map_err(|err| format!("查询网络登记失败: {err}"))?;
            if registered.is_none() {
                return Err(format!(
                    "network_not_registered sha={}（rating 对手网络必须已登记）",
                    r.opponent_sha
                ));
            }
        }
        // 局数上限：0 = 仅由 SPRT 判停；否则 ≥2 偶数。
        if r.total_games != 0 && (r.total_games < 2 || r.total_games % 2 != 0) {
            return Err(format!(
                "rating.total_games 必须为 0（仅 SPRT 判停）或 ≥2 且为偶数，当前 {}",
                r.total_games
            ));
        }
        // SPRT 覆盖参数：0 = 回落 [sprt] 配置；非 0 时校验取值范围。
        for (name, v) in [("alpha", r.sprt_alpha), ("beta", r.sprt_beta)] {
            if v != 0.0 && !(0.0 < v && v < 1.0) {
                return Err(format!("rating.sprt_{name} 必须在 (0,1)，当前 {v}"));
            }
        }
        if r.sprt_elo0 != 0.0 && r.sprt_elo1 != 0.0 && r.sprt_elo1 <= r.sprt_elo0 {
            return Err(format!(
                "rating.sprt_elo1 ({}) 必须大于 rating.sprt_elo0 ({})",
                r.sprt_elo1, r.sprt_elo0
            ));
        }
        Ok(())
    }

    // ---- 查询 / 控制 -------------------------------------------------------

    pub fn list(&self, state: Option<&str>, kind: Option<i64>) -> CoreResult<Vec<JobRow>> {
        self.store.list_jobs(state, kind)
    }

    pub fn get(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        self.store.get_job(job_id)
    }

    /// pause / resume / cancel。终态（completed/cancelled）不可再控制。
    /// 返回 (accepted, message, state)。
    pub fn control(&self, job_id: &str, action: &str) -> CoreResult<(bool, String, String)> {
        let Some(job) = self.store.get_job(job_id)? else {
            return Ok((false, format!("job_not_found:{job_id}"), String::new()));
        };
        let now = self.now();
        match action {
            "pause" => {
                if job.state != "running" {
                    return Ok((false, format!("pause_requires_running state={}", job.state), job.state));
                }
                self.store.update_job_state(job_id, "paused", now)?;
                tracing::info!("[job] paused id={job_id}");
                Ok((true, String::new(), "paused".into()))
            }
            "resume" => {
                if job.state != "paused" {
                    return Ok((false, format!("resume_requires_paused state={}", job.state), job.state));
                }
                self.store.update_job_state(job_id, "running", now)?;
                tracing::info!("[job] resumed id={job_id}");
                // 恢复即变为可派发：即时抢占评估。
                self.preemption_sweep(true);
                Ok((true, String::new(), "running".into()))
            }
            "cancel" => {
                if job.state == "completed" || job.state == "cancelled" {
                    return Ok((false, format!("job_is_terminal state={}", job.state), job.state));
                }
                self.store.update_job_state(job_id, "cancelled", now)?;
                // 取消全部在飞（已上报部分不回滚；迟到上报照常聚合）。
                self.cancel_in_flight_for_job(job_id);
                // rating job 额外把对打置 cancelled（不晋级、不标 rejected；
                // 迟到上报经 add_match_games 的 running 闸门被拒绝）。
                if job.kind == JobKind::JobRating as i64 {
                    self.cancel_rating_match(&job.spec_json);
                }
                tracing::info!("[job] cancelled id={job_id}（在飞任务已加入取消集合）");
                Ok((true, String::new(), "cancelled".into()))
            }
            other => Ok((false, format!("invalid_control=\"{other}\"（可选 pause/resume/cancel）"), job.state)),
        }
    }

    // ---- 派发记账 ----------------------------------------------------------

    /// 派发记账：disp 登记（含 job_id）+ job_tasks 映射落库 + dispatched/seq 推进。
    /// 返回本任务序号（种子派生用）。
    pub fn note_dispatch(
        &self,
        task_id: &str,
        job_id: &str,
        games: i64,
        worker_id: &str,
        kind: bqs_proto::TaskKind,
        network_sha: &str,
        opponent_sha: &str,
        opponent_spec: &str,
        match_id: i64,
        deadline_secs_override: Option<i64>,
    ) -> CoreResult<i64> {
        let row = self.store.incr_job_dispatched(job_id, games, self.now())?;
        self.store.insert_job_task(task_id, job_id, games)?;
        let mut t = crate::dispatch::RunningTask::new(kind);
        t.worker_id = worker_id.to_string();
        t.network_sha = network_sha.to_string();
        t.opponent_sha = opponent_sha.to_string();
        t.opponent_spec = opponent_spec.to_string();
        t.games = games;
        t.match_id = match_id;
        t.job_id = job_id.to_string();
        self.disp.register(task_id, t);
        if let Some(extra) = deadline_secs_override {
            // 采集任务的超期期限可由 [jobs].collect_deadline_seconds 覆盖内置默认。
            if extra > 0 {
                self.disp.override_deadline(task_id, self.now() + extra);
            }
        }
        Ok(row.next_seq - 1)
    }

    /// eval 认领路径（evalc.claim_eval_task 已登记在飞任务）的映射补登记：
    /// 任务 id → eval job，games 从任务快照回查。
    pub fn note_eval_claim(&self, task_id: &str, job_id: &str) -> CoreResult<()> {
        let games = self.disp.lookup(task_id).map(|t| t.games).unwrap_or(0);
        let row = self.store.incr_job_dispatched(job_id, games, self.now())?;
        let _ = row;
        self.store.insert_job_task(task_id, job_id, games)
    }

    // ---- 完成口径聚合 -------------------------------------------------------

    /// 上报聚合（collect/selfplay 的 ReportEpisode 与 eval 的 ReportMatchResult 共用）：
    /// task→job 映射（先落库的依据）→ completed += games（单事务）→ 达标即置
    /// completed 并取消该 job 全部在飞。映射行聚合后删除（CAS 防双计）。
    ///
    /// 无映射（rating / reanalysis / derive / 非 job 任务）静默忽略；
    /// job 已不在（被外部清理）也静默忽略（计数行已消失，重复聚合无意义）。
    pub fn record_report(&self, task_id: &str, games: i64) -> CoreResult<()> {
        let Some(link) = self.store.get_job_task(task_id)? else {
            return Ok(());
        };
        if games <= 0 {
            return Ok(());
        }
        let row = self.store.incr_job_completed(&link.job_id, games, self.now())?;
        self.store.delete_job_task(task_id)?;
        let total = row.total_games;
        if total > 0 && row.completed_games >= total && row.state == "running" {
            self.store.update_job_state(&link.job_id, "completed", self.now())?;
            self.cancel_in_flight_for_job(&link.job_id);
            tracing::info!(
                "[job] ✅ completed id={} completed={}/{}（在飞任务已加入取消集合）",
                link.job_id, row.completed_games, total
            );
        } else if total > 0 {
            tracing::info!(
                "[job] += id={} task={} games={} completed={}/{}",
                link.job_id, task_id, games, row.completed_games, total
            );
        } else {
            tracing::info!(
                "[job] += id={} task={} games={} completed={}（持续型）",
                link.job_id, task_id, games, row.completed_games
            );
        }
        Ok(())
    }

    // ---- 在飞取消 / 抢占 ----------------------------------------------------

    /// 把某 job 的全部在飞任务加入取消集合（幂等）。
    pub fn cancel_in_flight_for_job(&self, job_id: &str) {
        let in_flight: Vec<(String, String)> = self
            .disp
            .running()
            .into_iter()
            .filter(|t| t.job_id == job_id)
            .map(|t| (t.task_id, t.worker_id))
            .collect();
        let mut inner = self.inner.lock().unwrap();
        for (task_id, worker_id) in in_flight {
            inner.cancelled.entry(task_id).or_insert(worker_id);
        }
    }

    /// 抢占评估：当前最高可派发优先级 P 之外的更低优先级可派发 job，其在飞任务
    /// 全部取消。GetTask 路径按 JOB_PREEMPT_INTERVAL 节流调用（force=true 跳过
    /// 节流，用于提交/恢复后的即时评估）。
    pub fn preemption_sweep(&self, force: bool) {
        let now = self.now();
        {
            let mut inner = self.inner.lock().unwrap();
            if !force {
                if let Some(last) = inner.last_preempt {
                    if now - last < consts::JOB_PREEMPT_INTERVAL.as_secs() as i64 {
                        return;
                    }
                }
            }
            inner.last_preempt = Some(now);
        }
        let jobs = match self.store.list_jobs(None, None) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!("[job] ⚠️ 抢占评估读取 job 列表失败: {e}");
                return;
            }
        };
        let max_dispatchable = jobs
            .iter()
            .filter(|j| j.state == "running" && (j.total_games == 0 || j.completed_games < j.total_games))
            .map(|j| j.priority)
            .max();
        let Some(max_p) = max_dispatchable else { return };
        for j in jobs.iter().filter(|j| {
            j.state == "running"
                && j.priority < max_p
                && (j.total_games == 0 || j.completed_games < j.total_games)
        }) {
            self.cancel_in_flight_for_job(&j.job_id);
        }
    }

    /// 心跳回包的在途取消下发：返回该 worker 名下的取消任务 id（下发即移除；
    /// 任务已消失（prune / 重启）的条目一并清除；非本 worker 的条目保留）。
    pub fn cancel_task_ids_for_worker(&self, worker_id: &str) -> Vec<String> {
        let mut inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        let ids: Vec<String> = inner.cancelled.keys().cloned().collect();
        for id in ids {
            match self.disp.lookup(&id) {
                Some(t) if t.worker_id == worker_id => {
                    out.push(id.clone());
                    inner.cancelled.remove(&id);
                }
                Some(_) => {}
                None => {
                    inner.cancelled.remove(&id);
                }
            }
        }
        out
    }

    // ---- rating job 专用 ------------------------------------------------------

    /// rating job 的对打置 cancelled（幂等：非 running 的 match 为 no-op）。
    fn cancel_rating_match(&self, spec_json: &str) {
        if let Some(JobSpecJson::Rating(spec)) = JobSpecJson::parse(spec_json) {
            if spec.match_id != 0 {
                match self.matches.cancel_match(spec.match_id) {
                    Ok(true) => tracing::info!(
                        "[job] rating match {} cancelled（不晋级）",
                        spec.match_id
                    ),
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!("[job] ⚠️ rating match 取消失败 match={}: {e}", spec.match_id)
                    }
                }
            }
        }
    }

    /// rating 对打判停/达到局数上限后的 job 收敛：置 completed + 取消全部在飞
    /// （终态幂等：已 completed/cancelled 的 job 不再改写）。
    pub fn complete_rating_job(&self, job_id: &str) -> CoreResult<()> {
        let Some(job) = self.store.get_job(job_id)? else {
            return Ok(());
        };
        if job.state != "running" && job.state != "paused" {
            return Ok(());
        }
        self.store.update_job_state(job_id, "completed", self.now())?;
        self.cancel_in_flight_for_job(job_id);
        tracing::info!("[job] ✅ rating job completed id={job_id}（对打已判停/达上限，在飞已取消）");
        Ok(())
    }

    /// 上报路径回查 rating job 规格（无映射 / 非 rating job → None；旧版显式
    /// TASK_RATING 任务走 gatekeeper 既有判据链）。
    pub fn rating_spec(&self, job_id: &str) -> CoreResult<Option<RatingSpecJson>> {
        if job_id.is_empty() {
            return Ok(None);
        }
        Ok(self
            .store
            .get_job(job_id)?
            .and_then(|j| match JobSpecJson::parse(&j.spec_json) {
                Some(JobSpecJson::Rating(r)) => Some(r),
                _ => None,
            }))
    }

    /// 该对打是否属于某个 rating job（selfplay 混入查询排除用：pending_match
    /// 会取到任意 running 对打，rating job 的对打不得劫持 selfplay 混入）。
    pub fn is_rating_match(&self, match_id: i64) -> bool {
        self.store
            .list_jobs(None, Some(JobKind::JobRating as i64))
            .map(|rows| {
                rows.iter().any(|j| {
                    matches!(
                        JobSpecJson::parse(&j.spec_json),
                        Some(JobSpecJson::Rating(r)) if r.match_id == match_id
                    )
                })
            })
            .unwrap_or(false)
    }

    // ---- 引导 ---------------------------------------------------------------

    /// 启动引导：确保存在持续型 selfplay job（total=0、默认优先级）。
    /// 重启恢复语义：running job 的行已在库（重新载入即可）；仅当库中不存在
    /// running/paused 的 selfplay job 时按 [selfplay] 配置补建。
    pub fn ensure_selfplay_job(&self) -> CoreResult<()> {
        let existing = self.store.list_jobs(None, Some(JobKind::JobSelfplay as i64))?;
        if existing.iter().any(|j| j.state == "running" || j.state == "paused") {
            return Ok(());
        }
        let spec = SelfPlaySpecJson {
            variant: self.cfg.selfplay.variant.clone(),
            data_kind: match self.ctl.data_kind() {
                bqs_config::DataKind::Resnet => bqs_proto::DataKind::DataResnet as i32,
                bqs_config::DataKind::Nnue => bqs_proto::DataKind::DataNnue as i32,
            },
            network_sha: String::new(),
            opponent_sha: String::new(),
            mix_ratio: 0.0,
            search: None,
            total_games: 0,
        };
        let now = self.now();
        let job_id = crate::sources::new_id();
        self.store.insert_job(JobRow {
            job_id: job_id.clone(),
            kind: JobKind::JobSelfplay as i64,
            priority: self.default_priority(JobKind::JobSelfplay),
            state: "running".into(),
            total_games: 0,
            completed_games: 0,
            dispatched_games: 0,
            spec_json: JobSpecJson::SelfPlay(spec).to_json(),
            created_at: now,
            updated_at: now,
            next_seq: 0,
        })?;
        tracing::info!("[job] bootstrapped persistent selfplay job id={job_id}");
        Ok(())
    }

    /// evalcoord 钩子：为 (network, opponent) 自动创建 eval job（total 取 eval
    /// 配置）。已在（running/paused）的 (network, opponent) 幂等跳过。
    /// network_sha 为空 = 当前 best（此处解析为具体 sha 便于派发）。
    /// opponent_sha：模型类对手的网络 sha，规则类对手传空串。
    pub fn create_eval_job(
        &self,
        network_sha: &str,
        opponent_kind: &str,
        opponent_sha: &str,
    ) -> CoreResult<()> {
        let sha = if network_sha.is_empty() {
            match self.nets.get_best()? {
                Some(b) => b.sha,
                None => return Ok(()),
            }
        } else {
            network_sha.to_string()
        };
        let total = self.cfg.eval.games;
        let existing = self.store.list_jobs(None, Some(JobKind::JobEval as i64))?;
        let dup = existing.iter().any(|j| {
            (j.state == "running" || j.state == "paused")
                && JobSpecJson::parse(&j.spec_json)
                    .map(|s| matches!(
                        &s,
                        JobSpecJson::Eval(e)
                            if e.network_sha == sha
                                && e.opponent_kind == opponent_kind
                                && e.opponent_network_sha == opponent_sha
                    ))
                    .unwrap_or(false)
        });
        if dup {
            return Ok(());
        }
        let spec = JobSpecJson::Eval(EvalSpecJson {
            variant: self.cfg.selfplay.variant.clone(),
            network_sha: sha.clone(),
            opponent_kind: opponent_kind.to_string(),
            opponent_network_sha: opponent_sha.to_string(),
            search: None,
            total_games: total as u64,
        });
        let now = self.now();
        let job_id = crate::sources::new_id();
        self.store.insert_job(JobRow {
            job_id: job_id.clone(),
            kind: JobKind::JobEval as i64,
            priority: self.default_priority(JobKind::JobEval),
            state: "running".into(),
            total_games: total,
            completed_games: 0,
            dispatched_games: 0,
            spec_json: spec.to_json(),
            created_at: now,
            updated_at: now,
            next_seq: 0,
        })?;
        tracing::info!(
            "[job] auto eval job id={} network={sha} opponent={opponent_kind} total={total}",
            job_id
        );
        // eval job 默认优先级最高：即时抢占评估。
        self.preemption_sweep(true);
        Ok(())
    }

    /// 派发候选快照：running 且（completed < total 或 total=0），按 priority DESC、
    /// created_at ASC（list_jobs 的排序即派发顺序），可按 kind 过滤。
    pub fn dispatchable(&self, kind: JobKind) -> CoreResult<Vec<JobRow>> {
        let all = self.store.list_jobs(None, Some(kind as i64))?;
        Ok(all
            .into_iter()
            .filter(|j| j.state == "running" && (j.total_games == 0 || j.completed_games < j.total_games))
            .collect())
    }
}

fn spec_total(spec: &JobSpecJson) -> u64 {
    match spec {
        JobSpecJson::Collect(c) => c.total_games,
        JobSpecJson::SelfPlay(s) => s.total_games,
        JobSpecJson::Eval(e) => e.total_games,
        JobSpecJson::Rating(r) => r.total_games,
    }
}

// ---------------------------------------------------------------------------
// 场景 / 开局场景词表（提交校验共用）
// ---------------------------------------------------------------------------

/// OpeningScenario 是否合法（proto 枚举外值拒绝）。
pub fn valid_scenario(scenario: i32) -> bool {
    OpeningScenario::try_from(scenario).is_ok()
}

#[cfg(test)]
mod tests;
