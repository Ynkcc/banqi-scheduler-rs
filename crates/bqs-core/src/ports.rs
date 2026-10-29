//! 端口（Ports）：内核对外部世界的抽象。全部为**同步 trait**（SQLite 短事务 +
//! 字符串签名运算，阻塞开销可忽略；传输层负责 `spawn_blocking`）。
//!
//! 设计与 Go 版 `stores.go` 同构：**接口定义在消费方**——每个子系统只看见自己
//! 需要的方法集（窄接口），`bqs-store::Store` 实现全部端口，编译期由
//! `bqs-store` 侧断言。测试可注入内存 fake，不必绑真 SQLite。
//!
//! 错误约定：所有方法返回 [`CoreResult`]；「业务拒绝」不是错误（由调用方
//! 以回包字段表达），Err 一律是真故障。

use std::sync::Arc;

use bqs_judgment::Hexanomial;

use crate::{types::*, CoreResult};

// ---------------------------------------------------------------------------
// settings 表：Control 热更状态与节流计数器共用
// ---------------------------------------------------------------------------

pub trait SettingsStore: Send + Sync {
    fn get_setting(&self, key: &str) -> CoreResult<Option<String>>;
    /// 缺失时返回 default；值非整数视为故障（配置被外部污染）
    fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64>;
    fn set_setting(&self, key: &str, value: &str) -> CoreResult<()>;
    /// 原子自增，返回自增后的值
    fn inc_setting(&self, key: &str) -> CoreResult<i64>;
}

// ---------------------------------------------------------------------------
// networks 表：best 指针读写、登记、晋级与状态标记
// ---------------------------------------------------------------------------

pub trait NetworkStore: Send + Sync {
    fn get_best(&self) -> CoreResult<Option<Network>>;
    fn get_network(&self, sha: &str) -> CoreResult<Option<Network>>;
    /// 该权重格式最新登记的网络（数据类别换道时用作产数据方）
    fn latest_network_by_format(&self, format: &str) -> CoreResult<Option<Network>>;
    /// 全部网络列表（best 优先；WebUI /api/networks）
    fn list_networks(&self) -> CoreResult<Vec<Network>>;
    /// 登记新网络；返回 true = 该 sha 首次登记（触发 gatekeeper 建对打）
    fn register_network(
        &self,
        sha: &str,
        parent_sha: Option<&str>,
        notes: Option<&str>,
        format: &str,
    ) -> CoreResult<bool>;
    /// best 指针事务切换（含旧 best 归档语义）
    fn promote_best(&self, sha: &str) -> CoreResult<()>;
    fn update_network_status(&self, sha: &str, status: &str) -> CoreResult<()>;
}

// ---------------------------------------------------------------------------
// matches 表：gatekeeper 聚合（原子累加 + 条件结转）与 selfplay 混入查询
// ---------------------------------------------------------------------------

pub trait MatchStore: Send + Sync {
    fn create_match(&self, candidate: &str, opponent: &str, target_games: i64) -> CoreResult<i64>;
    /// 进行中的对打（selfplay 混入查询；按 id 最新优先）
    fn pending_match(&self) -> CoreResult<Option<Match>>;
    fn get_match(&self, id: i64) -> CoreResult<Option<Match>>;
    /// 对打列表（WebUI /api/matches，附 LLR 由 api 侧计算）
    fn list_matches(&self) -> CoreResult<Vec<Match>>;
    /// 原子累加六桶计数；返回（累加后的对打，bool 语义以 Go 版
    /// `AddMatchGames` 为准——infra 实现者必须核对并注释）
    fn add_match_games(&self, id: i64, delta: Hexanomial, games: i64) -> CoreResult<(Match, bool)>;
    /// 条件结转（status: running→concluded 的 CAS）；返回 true = 本调用完成结转
    /// （防多路径双晋级）
    fn conclude_match(&self, id: i64) -> CoreResult<bool>;
}

// ---------------------------------------------------------------------------
// eval：EvalCoordinator 消费面（settings + eval_queue + eval_results + 网络读取）
// ---------------------------------------------------------------------------

/// 评估观测消费面：settings（节流计数器）+ eval_queue（流式作业）+ eval_results（趋势）。
/// best 谱系的写权（晋级 / 标记拒绝）不在此接口——回滚副作用经 `BestMutator` 注入。
pub trait EvalStore: SettingsStore {
    fn load_eval_queue(&self) -> CoreResult<Vec<EvalQueueEntry>>;
    /// 入队评估作业；返回 false = 已存在（幂等）
    fn enqueue_eval_job(
        &self,
        network_sha: &str,
        spec: &str,
        target_games: i64,
        per_task_games: i64,
    ) -> CoreResult<bool>;
    fn delete_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<()>;
    /// 自增 attempts，返回自增后的值
    fn inc_eval_job_attempts(&self, network_sha: &str, spec: &str) -> CoreResult<i64>;
    /// 增量累加（attempts 在有进度时清零），返回累加后的行
    fn accumulate_eval_job(&self, network_sha: &str, spec: &str, delta: EvalDelta)
        -> CoreResult<EvalQueueEntry>;
    fn get_eval_job(&self, network_sha: &str, spec: &str) -> CoreResult<Option<EvalQueueEntry>>;
    /// 聚合落库（UNIQUE 覆盖去重；部分结果不劣化完整结果）
    fn upsert_eval_result(&self, r: EvalResult) -> CoreResult<()>;
    fn get_eval_result(&self, network_sha: &str, opponent_spec: &str)
        -> CoreResult<Option<EvalResult>>;
    /// 按对手的版本序列（升序，WebUI 趋势图数据源）
    fn eval_trend(&self, opponent_spec: &str, limit: i64) -> CoreResult<Vec<EvalResult>>;

    // ---- [api 需求] 追加（default 返回 Err：仅 bqs-store::Store 覆盖，
    // ---- test_support 的 fake 无需同步修改）----

    /// WebUI 首页跨表计数（[api 需求] Go: store.Counts）。
    fn counts(&self) -> CoreResult<StoreCounts> {
        let _ = self;
        Err(CoreError::Internal("counts 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
    /// 评估明细倒序列表（[api 需求] Go: store.ListEvalResults；limit<=0 视为不限）。
    fn list_eval_results(&self, limit: i64) -> CoreResult<Vec<EvalResult>> {
        let _ = (self, limit);
        Err(CoreError::Internal("list_eval_results 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
    /// 出现过的对手标识去重列表（[api 需求] Go: store.ListEvalOpponents，按首次出现顺序）。
    fn list_eval_opponents(&self) -> CoreResult<Vec<String>> {
        let _ = self;
        Err(CoreError::Internal("list_eval_opponents 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
}

// ---------------------------------------------------------------------------
// trainers 表：BalanceMonitor 消费面（权威积压）
// ---------------------------------------------------------------------------

pub trait TrainerStore: Send + Sync {
    fn list_trainers(&self) -> CoreResult<Vec<Trainer>>;
    /// 该类别的登记水位：返回 (maxID, beyond)——kind 匹配、id > after_id 的
    /// 批次中的最大 id 与超出条数（权威积压口径）
    fn kind_watermark(&self, kind: i64, after_id: i64) -> CoreResult<(i64, i64)>;
}

// ---------------------------------------------------------------------------
// episodes 表：episode 登记与 trainer 消费面
// ---------------------------------------------------------------------------

pub trait EpisodeStore: Send + Sync {
    /// 幂等查重（ReportEpisode 重试路径）
    fn get_episode_by_task(&self, task_id: &str) -> CoreResult<Option<Episode>>;
    fn insert_episode(&self, draft: EpisodeDraft) -> CoreResult<i64>;
    /// 游标分页：after_key 反查 episodes.id 后按 id 递增返回（不依赖对象键字典序）
    fn list_episode_keys(&self, after_key: &str, limit: i64, kind: Option<i64>)
        -> CoreResult<Vec<String>>;
    /// 消费水位映射（TrainerHeartbeat.consumed_key → episodes.id）
    fn episode_id_by_key(&self, key: &str) -> CoreResult<Option<i64>>;
    /// 元数据按 id 倒序分页（WebUI /api/episodes）
    fn list_episodes_page(&self, before_id: i64, limit: i64) -> CoreResult<Vec<Episode>>;

    // ---- [api 需求] 追加（default 返回 Err：仅 bqs-store::Store 覆盖，
    // ---- test_support 的 fake 无需同步修改）----

    // ---- [replay 需求] 追加（default 返回 Err：仅 bqs-store::Store 覆盖，
    // ---- test_support 的 fake 无需同步修改）----

    /// 按 id 查单条 episode 元数据（[replay 需求] Go: store.GetEpisode；
    /// 回放端点据 object_key 拉对象、network_sha/opponent_* 渲染对局双方）。
    fn get_episode(&self, id: i64) -> CoreResult<Option<Episode>> {
        let _ = id;
        Err(CoreError::Internal("get_episode 仅 bqs-store::Store 实现（[replay 需求]）".into()))
    }

    /// 按窗口聚合各 worker 登记的 episode 产出（[api 需求] Go: store.EpisodeStatsByWorker；
    /// 键 = worker_id；window<=0 表示不限窗口/累计口径）。
    fn episode_stats_by_worker(&self, window_secs: i64) -> CoreResult<BTreeMap<String, KindStats>> {
        let _ = (self, window_secs);
        Err(CoreError::Internal("episode_stats_by_worker 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
    /// 各 worker 产出速率估算样本（[api 需求] Go: store.EpisodeRateEstimates；
    /// 窗口内每 worker 取最近 samples_per_worker 批；samples<=0 取默认 24）。
    fn episode_rate_estimates(
        &self,
        window_secs: i64,
        samples_per_worker: i64,
    ) -> CoreResult<BTreeMap<String, EpisodeRateEstimate>> {
        let _ = (self, window_secs, samples_per_worker);
        Err(CoreError::Internal("episode_rate_estimates 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
    /// 数据流入总览（[api 需求] Go: store.EpisodeDataStats 的三窗口聚合部分；
    /// recent 由组装侧经 list_episodes_page 自取）。
    fn episode_data_stats(&self) -> CoreResult<EpisodeDataStats> {
        let _ = self;
        Err(CoreError::Internal("episode_data_stats 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
    /// 本代数据量（[api 需求] Go: store.GenerationDataStats；以 networks 最新登记时间
    /// 为界，排除 none 类回放快照）。
    fn generation_data_stats(&self) -> CoreResult<GenerationStats> {
        let _ = self;
        Err(CoreError::Internal("generation_data_stats 仅 bqs-store::Store 实现（[api 需求]）".into()))
    }
}

// ---------------------------------------------------------------------------
// workers 表：worker 心跳登记与查询
// ---------------------------------------------------------------------------

pub trait WorkerStore: Send + Sync {
    /// UPSERT 单行心跳（last_seen 跨重启保留）
    fn touch_worker(
        &self,
        id: &str,
        threads: i64,
        completed_games: i64,
        client_version: &str,
        memory_mb: i64,
        health_status: &str,
        last_error: &str,
    ) -> CoreResult<()>;
    /// GetTask 版本管控：该 worker 上次声明的 client_version
    fn worker_version(&self, id: &str) -> CoreResult<Option<String>>;
    fn list_workers(&self) -> CoreResult<Vec<Worker>>;
    /// 运维备注（PUT /api/workers/{id}/note）
    fn set_worker_note(&self, id: &str, note: &str) -> CoreResult<()>;
}

// ---------------------------------------------------------------------------
// trainers 表：Server TrainerHeartbeat 登记面
// ---------------------------------------------------------------------------

pub trait TrainerRegistryStore: Send + Sync {
    fn get_trainer(&self, id: &str) -> CoreResult<Option<Trainer>>;
    /// UPSERT 单行心跳
    fn touch_trainer(&self, t: Trainer) -> CoreResult<()>;
}

// ---------------------------------------------------------------------------
// feature_packs 表：特征包登记
// ---------------------------------------------------------------------------

pub trait FeaturePackStore: Send + Sync {
    fn insert_feature_pack(&self, fp: FeaturePackDraft) -> CoreResult<()>;
    fn get_feature_pack(&self, episode_object_key: &str, pack_version: i64)
        -> CoreResult<Option<FeaturePack>>;
}

/// 特征包登记草稿。
#[derive(Debug, Clone, Default)]
pub struct FeaturePackDraft {
    pub episode_object_key: String,
    pub pack_version: i64,
    pub object_key: String,
    pub created_at: i64,
}

// ---------------------------------------------------------------------------
// 对象签名（预签名 URL；凭据只在调度器持有，worker/trainer 零存储配置）
// ---------------------------------------------------------------------------

pub trait ObjectSigner: Send + Sync {
    /// 预签名 PUT（episode / 网络上传；数据直传对象存储，不经调度器）
    fn presign_put(&self, key: &str, ttl_secs: i64) -> CoreResult<String>;
    /// 预签名 GET（episode / 网络下载）
    fn presign_get(&self, key: &str, ttl_secs: i64) -> CoreResult<String>;
}

// ---------------------------------------------------------------------------
// 时钟：全部可计时行为显式注入时间点，便于测试
// ---------------------------------------------------------------------------

pub trait Clock: Send + Sync {
    /// Unix 秒
    fn now(&self) -> i64;
}

/// 系统时钟默认实现。
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// 组装好的端口束（bqs-server 装配根负责构造；Store 实现全部 trait）
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Ports {
    pub settings: Arc<dyn SettingsStore>,
    pub networks: Arc<dyn NetworkStore>,
    pub matches: Arc<dyn MatchStore>,
    pub eval: Arc<dyn EvalStore>,
    pub trainers: Arc<dyn TrainerStore>,
    pub episodes: Arc<dyn EpisodeStore>,
    pub workers: Arc<dyn WorkerStore>,
    pub trainer_registry: Arc<dyn TrainerRegistryStore>,
    pub feature_packs: Arc<dyn FeaturePackStore>,
}

// ---------------------------------------------------------------------------
// [api 需求] —— WebUI /api 聚合查询（infra 追加，行类型对照 Go internal/store 的
// 对应 struct；JSON 字段名仍由 bqs-api 的 views.rs 锁定）。
// 追加方法一律带默认实现（返回显式 Err），保证 test_support 的 fake 不必同步修改。
use std::collections::BTreeMap;
use crate::CoreError;

/// WebUI 首页跨表计数（[api 需求] Go: store.Counts）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct StoreCounts {
    pub networks: i64,
    pub candidates: i64,
    pub matches_running: i64,
    pub episodes: i64,
    pub episode_games: i64,
    pub workers: i64,
    pub eval_results: i64,
}

/// 单数据类别 / 单 worker 的登记统计（[api 需求] Go: store.KindStats；
/// episodes=批数 games=局数 steps=步数）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct KindStats {
    pub episodes: i64,
    pub games: i64,
    pub steps: i64,
}

/// 单 worker 的产出速率估算样本（[api 需求] Go: store.EpisodeRateEstimate）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct EpisodeRateEstimate {
    pub games: i64,
    pub episodes: i64,
    pub span_seconds: i64,
}

/// 数据流入总览（[api 需求] Go: store.EpisodeDataStats；不含 recent——
/// 组装侧经 `list_episodes_page` 自取最新一页）。键 = episodes.kind 原值
/// （0=resnet 1=nnue 2=none），kind → 名称映射由 api 层统一做。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct EpisodeDataStats {
    pub total: BTreeMap<i64, KindStats>,
    pub last_1h: BTreeMap<i64, KindStats>,
    pub last_24h: BTreeMap<i64, KindStats>,
}

/// 本代数据量（[api 需求] Go: store.GenerationStats）：最近一次候选导出
/// （networks 最新登记时间）以来的训练数据统计，排除 none 类回放快照；
/// fresh-only 训练语义下该窗口 = 下一候选的训练集。尚无任何网络时为全零。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct GenerationStats {
    /// 代际起点（unix 秒）= 最新登记网络的登记时间；无网络时 0。
    pub since: i64,
    /// 触发边界的网络 sha（即最近导出的候选）；无网络时空串。
    pub candidate: String,
    pub episodes: i64,
    pub games: i64,
    pub steps: i64,
}

// —— 追加方法挂在上方 EvalStore / EpisodeStore trait 定义处（带 default 实现）——

