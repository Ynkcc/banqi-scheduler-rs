//! JSON 视图类型：字段名与 Go 版 `internal/api/` 各视图 struct 的 json tag 逐一
//! 对应（契约真源：`banqi-scheduler/internal/api/` + `contract_test.go` 字段集）。
//! 前端按名字取值——多字段是加法兼容，少字段/改名是破坏性变更。
//!
//! 所有视图只在本 crate 内构造（serde Serialize，不参与反序列化）。

use serde::Serialize;

use bqs_core::ports::{EpisodeRateEstimate, GenerationStats, KindStats, StoreCounts};
use bqs_judgment::Hexanomial;

// ---------------------------------------------------------------------------
// 通用
// ---------------------------------------------------------------------------

/// `{"ok": true}`（Go: okView）。
#[derive(Debug, Serialize)]
pub struct OkView {
    #[serde(rename = "ok")]
    pub ok: bool,
}

/// store 行数据类别的稳定标识（Go: store.kindDisplayName；0=resnet 1=nnue 2=none，
/// 其余按 resnet 兜底）。
pub fn data_kind_name(kind: i64) -> &'static str {
    match kind {
        1 => "nnue",
        2 => "none",
        _ => "resnet",
    }
}

/// 评估搜索模式名（Go: scheduler.EvalSearchModeName；0 = 纯策略 argmax）。
pub fn eval_search_mode_name(mcts_sims: i32) -> String {
    if mcts_sims <= 0 {
        "纯策略 argmax".into()
    } else {
        format!("MCTS {mcts_sims} sims")
    }
}

/// 平衡模式对外标识（Go: BalanceConfig.Mode 为小写字符串 warn / pause）。
pub fn balance_mode_name(mode: bqs_config::BalanceMode) -> &'static str {
    match mode {
        bqs_config::BalanceMode::Warn => "warn",
        bqs_config::BalanceMode::Pause => "pause",
    }
}

// ---------------------------------------------------------------------------
// /api/status（Go: handlers_status.go statusView / networkView / store.Counts）
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct NetworkView {
    #[serde(rename = "sha")]
    pub sha: String,
    #[serde(rename = "parentSha")]
    pub parent_sha: String,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "isBest")]
    pub is_best: bool,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "notes")]
    pub notes: String,
}

/// GSPRT 参数视图（Go: sprtView = config.SprtConfig）。
#[derive(Debug, Serialize)]
pub struct SprtView {
    #[serde(rename = "elo0")]
    pub elo0: f64,
    #[serde(rename = "elo1")]
    pub elo1: f64,
    #[serde(rename = "alpha")]
    pub alpha: f64,
    #[serde(rename = "beta")]
    pub beta: f64,
}

#[derive(Debug, Serialize)]
pub struct CountsView {
    #[serde(rename = "networks")]
    pub networks: i64,
    #[serde(rename = "candidates")]
    pub candidates: i64,
    #[serde(rename = "matchesRunning")]
    pub matches_running: i64,
    #[serde(rename = "episodes")]
    pub episodes: i64,
    #[serde(rename = "episodeGames")]
    pub episode_games: i64,
    #[serde(rename = "workers")]
    pub workers: i64,
    #[serde(rename = "evalResults")]
    pub eval_results: i64,
}

/// 端口行 → 视图（字段一一对应）。
impl From<StoreCounts> for CountsView {
    fn from(c: StoreCounts) -> Self {
        CountsView {
            networks: c.networks,
            candidates: c.candidates,
            matches_running: c.matches_running,
            episodes: c.episodes,
            episode_games: c.episode_games,
            workers: c.workers,
            eval_results: c.eval_results,
        }
    }
}

/// 评估配置快照（Go: evalConfigView = config.EvalConfig + mode/pending）。
#[derive(Debug, Serialize)]
pub struct EvalConfigView {
    #[serde(rename = "enabled")]
    pub enabled: bool,
    #[serde(rename = "opponents")]
    pub opponents: Vec<String>,
    #[serde(rename = "games")]
    pub games: i64,
    #[serde(rename = "taskGames")]
    pub task_games: i64,
    #[serde(rename = "mctsSims")]
    pub mcts_sims: i32,
    #[serde(rename = "everyNPromotions")]
    pub every_n_promotions: i64,
    #[serde(rename = "noProgressN")]
    pub no_progress_n: i64,
    #[serde(rename = "noProgressEps")]
    pub no_progress_eps: f64,
    #[serde(rename = "rollbackEps")]
    pub rollback_eps: f64,
    #[serde(rename = "floorWinRate")]
    pub floor_win_rate: f64,
    #[serde(rename = "stopWinRate")]
    pub stop_win_rate: f64,
    #[serde(rename = "mode")]
    pub mode: String,
    #[serde(rename = "pending")]
    pub pending: i64,
}

#[derive(Debug, Serialize)]
pub struct StatusView {
    #[serde(rename = "variant")]
    pub variant: String,
    #[serde(rename = "paused")]
    pub paused: bool,
    #[serde(rename = "initialRevealed")]
    pub initial_revealed: i32,
    #[serde(rename = "dataKind")]
    pub data_kind: String,
    #[serde(rename = "minClientVersion")]
    pub min_client_version: String,
    #[serde(rename = "sprt")]
    pub sprt: SprtView,
    #[serde(rename = "best")]
    pub best: Option<NetworkView>,
    #[serde(rename = "counts")]
    pub counts: CountsView,
    #[serde(rename = "eval")]
    pub eval: EvalConfigView,
    #[serde(rename = "shouldStop")]
    pub should_stop: bool,
    #[serde(rename = "stopReason")]
    pub stop_reason: String,
    #[serde(rename = "autoPaused")]
    pub auto_paused: bool,
    #[serde(rename = "autoPauseReason")]
    pub auto_pause_reason: String,
}

// ---------------------------------------------------------------------------
// /api/matches（Go: matchView；LLR / verdict 由本 crate 用 bqs_judgment 计算）
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct MatchView {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "candidate")]
    pub candidate: String,
    #[serde(rename = "opponent")]
    pub opponent: String,
    #[serde(rename = "status")]
    pub status: String,
    /// 六桶成对计数 [LL, LD, DD, WL, DW, WW]（Hexanomial 序列化为 6 元数组）。
    #[serde(rename = "pairs")]
    pub pairs: Hexanomial,
    #[serde(rename = "numGames")]
    pub num_games: i64,
    #[serde(rename = "targetGames")]
    pub target_games: i64,
    #[serde(rename = "score")]
    pub score: Option<f64>,
    #[serde(rename = "llr")]
    pub llr: f64,
    #[serde(rename = "lower")]
    pub lower: f64,
    #[serde(rename = "upper")]
    pub upper: f64,
    #[serde(rename = "verdict")]
    pub verdict: String,
}

// ---------------------------------------------------------------------------
// /api/workers（Go: workerView）
// ---------------------------------------------------------------------------

/// 单 worker 的窗口产出统计（Go: store.KindStats）。
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct KindStatsView {
    #[serde(rename = "episodes")]
    pub episodes: i64,
    #[serde(rename = "games")]
    pub games: i64,
    #[serde(rename = "steps")]
    pub steps: i64,
}

impl From<KindStats> for KindStatsView {
    fn from(k: KindStats) -> Self {
        KindStatsView { episodes: k.episodes, games: k.games, steps: k.steps }
    }
}

/// 单 worker 的产出速率估算样本（Go: store.EpisodeRateEstimate）。
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct EpisodeRateEstimateView {
    #[serde(rename = "games")]
    pub games: i64,
    #[serde(rename = "episodes")]
    pub episodes: i64,
    #[serde(rename = "spanSeconds")]
    pub span_seconds: i64,
}

impl From<EpisodeRateEstimate> for EpisodeRateEstimateView {
    fn from(e: EpisodeRateEstimate) -> Self {
        EpisodeRateEstimateView {
            games: e.games,
            episodes: e.episodes,
            span_seconds: e.span_seconds,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WorkerView {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "note")]
    pub note: String,
    #[serde(rename = "lastSeen")]
    pub last_seen: i64,
    #[serde(rename = "online")]
    pub online: bool,
    #[serde(rename = "threads")]
    pub threads: i64,
    #[serde(rename = "completedGames")]
    pub completed_games: i64,
    #[serde(rename = "clientVersion")]
    pub client_version: String,
    #[serde(rename = "memoryMb")]
    pub memory_mb: i64,
    #[serde(rename = "healthStatus")]
    pub health_status: String,
    #[serde(rename = "lastError")]
    pub last_error: String,
    #[serde(rename = "episodes1h")]
    pub episodes_1h: i64,
    #[serde(rename = "games1h")]
    pub games_1h: i64,
    #[serde(rename = "episodes24h")]
    pub episodes_24h: i64,
    #[serde(rename = "games24h")]
    pub games_24h: i64,
    #[serde(rename = "estGamesPerHour")]
    pub est_games_per_hour: f64,
    #[serde(rename = "estPerThreadHour")]
    pub est_per_thread_hour: f64,
    #[serde(rename = "estSamples")]
    pub est_samples: i64,
}

// ---------------------------------------------------------------------------
// /api/tasks（Go: taskView，内存任务快照）
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TaskView {
    #[serde(rename = "taskId")]
    pub task_id: String,
    #[serde(rename = "workerId")]
    pub worker_id: String,
    #[serde(rename = "kind")]
    pub kind: String,
    #[serde(rename = "matchId")]
    pub match_id: i64,
    #[serde(rename = "networkSha")]
    pub network_sha: String,
    #[serde(rename = "opponentSha")]
    pub opponent_sha: String,
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,
    #[serde(rename = "games")]
    pub games: i32,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

// /api/task-stats 直接序列化 bqs_core::dispatch::TaskStateStats（serde camelCase
// 已与 Go WebUI 契约逐字段一致：taskId/workerId/oldestAssignedAgeSeconds 等）。

// ---------------------------------------------------------------------------
// /api/eval（Go: handlers_eval.go evalResultView / evalTrendView / evalRunningView / evalView）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct EvalResultView {
    #[serde(rename = "networkSha")]
    pub network_sha: String,
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,
    #[serde(rename = "wins")]
    pub wins: i64,
    #[serde(rename = "draws")]
    pub draws: i64,
    #[serde(rename = "losses")]
    pub losses: i64,
    #[serde(rename = "redWins")]
    pub red_wins: i64,
    #[serde(rename = "blackWins")]
    pub black_wins: i64,
    #[serde(rename = "drawNoCapture")]
    pub draw_no_capture: i64,
    #[serde(rename = "drawTruncation")]
    pub draw_truncation: i64,
    #[serde(rename = "numGames")]
    pub num_games: i64,
    #[serde(rename = "winRate")]
    pub win_rate: f64,
    #[serde(rename = "avgMoves")]
    pub avg_moves: f64,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

/// 单对手的版本序列（升序），含「连续无提升」计数。
#[derive(Debug, Serialize)]
pub struct EvalTrendView {
    #[serde(rename = "opponent")]
    pub opponent: String,
    #[serde(rename = "versions")]
    pub versions: i64,
    #[serde(rename = "noProgress")]
    pub no_progress: i64,
    #[serde(rename = "latestWinRate")]
    pub latest_win_rate: f64,
    #[serde(rename = "points")]
    pub points: Vec<EvalResultView>,
}

#[derive(Debug, Serialize)]
pub struct EvalRunningView {
    #[serde(rename = "networkSha")]
    pub network_sha: String,
    #[serde(rename = "spec")]
    pub spec: String,
    #[serde(rename = "numGames")]
    pub num_games: i64,
    #[serde(rename = "targetGames")]
    pub target_games: i64,
    #[serde(rename = "inFlight")]
    pub in_flight: i64,
}

/// 单对手趋势摘要（Go: scheduler.EvalProgress 的 API 投影；供 trends 组装）。
#[derive(Debug, Clone, Copy)]
pub struct EvalTrendProgressView {
    pub versions: i64,
    pub no_progress: i64,
    pub latest_win_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct EvalView {
    #[serde(rename = "config")]
    pub config: EvalConfigView,
    #[serde(rename = "shouldStop")]
    pub should_stop: bool,
    #[serde(rename = "stopReason")]
    pub stop_reason: String,
    #[serde(rename = "latest")]
    pub latest: Vec<EvalResultView>,
    #[serde(rename = "trends")]
    pub trends: Vec<EvalTrendView>,
    #[serde(rename = "running")]
    pub running: Vec<EvalRunningView>,
}

/// POST /api/eval/trigger 响应（Go: {"ok":true,"triggered":n,"message":msg}）。
#[derive(Debug, Serialize)]
pub struct EvalTriggerView {
    #[serde(rename = "ok")]
    pub ok: bool,
    #[serde(rename = "triggered")]
    pub triggered: i64,
    #[serde(rename = "message")]
    pub message: String,
}

/// GET /api/eval-config 响应：在线覆盖 + 生效值（Go: {"override": ov, "effective": {...}}）。
#[derive(Debug, Serialize)]
pub struct EvalConfigGetView {
    /// Go 键名 "override"（Rust 关键字，显式 rename）。
    #[serde(rename = "override")]
    pub override_: Option<bqs_core::evalcoord::EvalOverride>,
    #[serde(rename = "effective")]
    pub effective: EvalEffectiveView,
}

#[derive(Debug, Serialize)]
pub struct EvalEffectiveView {
    #[serde(rename = "enabled")]
    pub enabled: bool,
    #[serde(rename = "opponents")]
    pub opponents: Vec<String>,
    #[serde(rename = "stopWinRate")]
    pub stop_win_rate: f64,
}

// ---------------------------------------------------------------------------
// /api/trainers（Go: handlers_trainers.go balanceConfigView / trainerView / trainersView）
// ---------------------------------------------------------------------------

/// [balance] 配置快照（Go: balanceConfigView = config.BalanceConfig）。
#[derive(Debug, Serialize)]
pub struct BalanceConfigView {
    #[serde(rename = "enabled")]
    pub enabled: bool,
    #[serde(rename = "mode")]
    pub mode: String,
    #[serde(rename = "trainerOnlineSeconds")]
    pub trainer_online_seconds: i64,
    #[serde(rename = "stalledSeconds")]
    pub stalled_seconds: i64,
    #[serde(rename = "backlogPauseBatches")]
    pub backlog_pause_batches: i64,
    #[serde(rename = "pauseReanalysis")]
    pub pause_reanalysis: bool,
}

#[derive(Debug, Serialize)]
pub struct TrainerView {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "lastSeen")]
    pub last_seen: i64,
    #[serde(rename = "online")]
    pub online: bool,
    #[serde(rename = "variant")]
    pub variant: String,
    #[serde(rename = "dataKind")]
    pub data_kind: String,
    #[serde(rename = "consumedKey")]
    pub consumed_key: String,
    #[serde(rename = "consumedId")]
    pub consumed_id: i64,
    #[serde(rename = "registeredMaxId")]
    pub registered_max_id: i64,
    #[serde(rename = "unconsumedBatches")]
    pub unconsumed_batches: i64,
    #[serde(rename = "samplesConsumed")]
    pub samples_consumed: i64,
    #[serde(rename = "backlog")]
    pub backlog: i64,
    #[serde(rename = "buffered")]
    pub buffered: i64,
    #[serde(rename = "healthStatus")]
    pub health_status: String,
    #[serde(rename = "lastError")]
    pub last_error: String,
    #[serde(rename = "progress")]
    pub progress: String,
}

#[derive(Debug, Serialize)]
pub struct TrainersView {
    #[serde(rename = "config")]
    pub config: BalanceConfigView,
    #[serde(rename = "autoPaused")]
    pub auto_paused: bool,
    #[serde(rename = "autoPauseReason")]
    pub auto_pause_reason: String,
    #[serde(rename = "trainers")]
    pub trainers: Vec<TrainerView>,
}

// ---------------------------------------------------------------------------
// /api/train-config（Go: trainConfigView + scheduler.TrainConfigField）
// ---------------------------------------------------------------------------

/// 白名单字段的只读描述（Go: scheduler.TrainConfigField，含 `enum,omitempty`）。
#[derive(Debug, Serialize)]
pub struct TrainConfigFieldView {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "kind")]
    pub kind: String,
    #[serde(rename = "min")]
    pub min: f64,
    #[serde(rename = "max")]
    pub max: f64,
    #[serde(rename = "hasMax")]
    pub has_max: bool,
    #[serde(rename = "gtZero")]
    pub gt_zero: bool,
    #[serde(rename = "enum", skip_serializing_if = "Vec::is_empty")]
    pub r#enum: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TrainConfigView {
    #[serde(rename = "overrides")]
    pub overrides: std::collections::BTreeMap<String, String>,
    #[serde(rename = "fields")]
    pub fields: Vec<TrainConfigFieldView>,
}

// ---------------------------------------------------------------------------
// /api/episodes 与 /api/data-stats（Go: handlers_episodes.go）
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EpisodeView {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "workerId")]
    pub worker_id: String,
    #[serde(rename = "taskId")]
    pub task_id: String,
    #[serde(rename = "networkSha")]
    pub network_sha: String,
    #[serde(rename = "gameCount")]
    pub game_count: i64,
    #[serde(rename = "totalSteps")]
    pub total_steps: i64,
    #[serde(rename = "winner")]
    pub winner: i64,
    #[serde(rename = "dataKind")]
    pub data_kind: String,
    #[serde(rename = "objectKey")]
    pub object_key: String,
    #[serde(rename = "opponentSha")]
    pub opponent_sha: String,
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,
    #[serde(rename = "matchId")]
    pub match_id: i64,
    #[serde(rename = "redWins")]
    pub red_wins: i64,
    #[serde(rename = "blackWins")]
    pub black_wins: i64,
    #[serde(rename = "drawNoCapture")]
    pub draw_no_capture: i64,
    #[serde(rename = "drawTruncation")]
    pub draw_truncation: i64,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
pub struct EpisodePageView {
    #[serde(rename = "items")]
    pub items: Vec<EpisodeView>,
    #[serde(rename = "nextBefore")]
    pub next_before: i64,
    #[serde(rename = "hasMore")]
    pub has_more: bool,
}

/// 数据流入总览（Go: handleDataStats 的 map[string]any 响应）。
#[derive(Debug, Serialize)]
pub struct DataStatsView {
    #[serde(rename = "total")]
    pub total: std::collections::BTreeMap<String, KindStatsView>,
    #[serde(rename = "last1h")]
    pub last_1h: std::collections::BTreeMap<String, KindStatsView>,
    #[serde(rename = "last24h")]
    pub last_24h: std::collections::BTreeMap<String, KindStatsView>,
    #[serde(rename = "recent")]
    pub recent: Vec<EpisodeView>,
    #[serde(rename = "generation")]
    pub generation: GenerationStatsView,
}

/// 本代数据量（Go: store.GenerationStats）。
#[derive(Debug, Serialize)]
pub struct GenerationStatsView {
    #[serde(rename = "since")]
    pub since: i64,
    #[serde(rename = "candidate")]
    pub candidate: String,
    #[serde(rename = "episodes")]
    pub episodes: i64,
    #[serde(rename = "games")]
    pub games: i64,
    #[serde(rename = "steps")]
    pub steps: i64,
}

impl From<GenerationStats> for GenerationStatsView {
    fn from(g: GenerationStats) -> Self {
        GenerationStatsView {
            since: g.since,
            candidate: g.candidate,
            episodes: g.episodes,
            games: g.games,
            steps: g.steps,
        }
    }
}

// ---------------------------------------------------------------------------
// /api/storage（Go: storage.go storageProfileView / storageView）
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct StorageProfileView {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "endpoint")]
    pub endpoint: String,
    #[serde(rename = "region")]
    pub region: String,
    /// 掩码（前 4 + **** + 后 4）；secret 永不出域。
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "hasSecret")]
    pub has_secret: bool,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "pathStyle")]
    pub path_style: bool,
}

#[derive(Debug, Serialize)]
pub struct StorageView {
    #[serde(rename = "active")]
    pub active: String,
    #[serde(rename = "profiles")]
    pub profiles: Vec<StorageProfileView>,
}

// ---------------------------------------------------------------------------
// episode 回放（Go: internal/replay Game/Step/GameSummary + replay.go 信封；
// 契约真源 = banqi-frontend/src/scheduler/types.ts 的 ReplayList/ReplayGame/
// ReplayStep/HiddenBag，camelCase 逐字段一致，前端零改动）
// ---------------------------------------------------------------------------

/// 步前双方剩余暗子按类型计数（兵炮马车相士帅 = 下标 0..6；Go: replay.HiddenBag）。
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct HiddenBagView {
    #[serde(rename = "red")]
    pub red: [i64; 7],
    #[serde(rename = "black")]
    pub black: [i64; 7],
}

/// 单局轻量摘要（Go: replay.GameSummary）。
#[derive(Debug, Serialize)]
pub struct ReplayGameSummaryView {
    #[serde(rename = "index")]
    pub index: usize,
    #[serde(rename = "steps")]
    pub steps: usize,
    #[serde(rename = "gameLength")]
    pub game_length: usize,
    #[serde(rename = "winner")]
    pub winner: Option<i32>,
    #[serde(rename = "termination")]
    pub termination: String,
    #[serde(rename = "hasSeed")]
    pub has_seed: bool,
}

/// GET /api/episodes/{id}/replay 信封（Go: handleEpisodeReplayList 的匿名 struct）。
#[derive(Debug, Serialize)]
pub struct ReplayListView {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "kind")]
    pub kind: String,
    #[serde(rename = "variant")]
    pub variant: String,
    #[serde(rename = "gameCount")]
    pub game_count: i64,
    #[serde(rename = "games")]
    pub games: Vec<ReplayGameSummaryView>,
}

/// 单步视图（上帝视角；Go: replay.Step）。
#[derive(Debug, Clone, Serialize)]
pub struct ReplayStepView {
    #[serde(rename = "ply")]
    pub ply: usize,
    #[serde(rename = "player")]
    pub player: String,
    #[serde(rename = "action")]
    pub action: u32,
    #[serde(rename = "from")]
    pub from: i64,
    #[serde(rename = "to")]
    pub to: i64,
    #[serde(rename = "kind")]
    pub kind: String,
    /// Go omitempty：false 时省略（前端 undefined 按 falsy 处理）。
    #[serde(rename = "isChance", skip_serializing_if = "std::ops::Not::not")]
    pub is_chance: bool,
    #[serde(rename = "revealed", skip_serializing_if = "String::is_empty")]
    pub revealed: String,
    #[serde(rename = "captured", skip_serializing_if = "String::is_empty")]
    pub captured: String,
    #[serde(rename = "moveCounter")]
    pub move_counter: i64,
    #[serde(rename = "scores")]
    pub scores: [i64; 2],
    #[serde(rename = "board")]
    pub board: Vec<String>,
    #[serde(rename = "revealedAt")]
    pub revealed_at: Vec<bool>,
    #[serde(rename = "hiddenBag")]
    pub hidden_bag: HiddenBagView,
    #[serde(rename = "boardSource")]
    pub board_source: String,
    #[serde(rename = "producer")]
    pub producer: i64,
    #[serde(rename = "searchValue")]
    pub search_value: f32,
    #[serde(rename = "playerValue")]
    pub player_value: f32,
    #[serde(rename = "hiddenCount")]
    pub hidden_count: i64,
    #[serde(rename = "searchDepth")]
    pub search_depth: i64,
    #[serde(rename = "searchNodes")]
    pub search_nodes: i64,
    #[serde(rename = "chosenValueGap")]
    pub chosen_value_gap: f32,
    #[serde(rename = "explRandom")]
    pub expl_random: bool,
    #[serde(rename = "labelerFallback")]
    pub labeler_fallback: bool,
    #[serde(rename = "labelerAction")]
    pub labeler_action: Option<u32>,
    #[serde(rename = "policyActions")]
    pub policy_actions: Option<Vec<u32>>,
    #[serde(rename = "policyValues")]
    pub policy_values: Option<Vec<f32>>,
}

/// 一局完整回放（Go: replay.Game）。
#[derive(Debug, Serialize)]
pub struct ReplayGameView {
    #[serde(rename = "index")]
    pub index: usize,
    #[serde(rename = "steps")]
    pub steps: usize,
    #[serde(rename = "gameLength")]
    pub game_length: usize,
    #[serde(rename = "winner")]
    pub winner: Option<i32>,
    #[serde(rename = "termination")]
    pub termination: String,
    #[serde(rename = "seed")]
    pub seed: u64,
    #[serde(rename = "hasSeed")]
    pub has_seed: bool,
    #[serde(rename = "sideA")]
    pub side_a: String,
    #[serde(rename = "sideB")]
    pub side_b: String,
    #[serde(rename = "redIsA")]
    pub red_is_a: bool,
    #[serde(rename = "aSideOnly")]
    pub a_side_only: bool,
    #[serde(rename = "initialBoard")]
    pub initial_board: Vec<String>,
    #[serde(rename = "initialShown")]
    pub initial_shown: Vec<bool>,
    #[serde(rename = "initialBag")]
    pub initial_bag: HiddenBagView,
    #[serde(rename = "finalBoard")]
    pub final_board: Vec<String>,
    #[serde(rename = "finalShown")]
    pub final_shown: Vec<bool>,
    #[serde(rename = "finalBag")]
    pub final_bag: HiddenBagView,
    #[serde(rename = "moves")]
    pub moves: Vec<ReplayStepView>,
    #[serde(rename = "labelerAgree")]
    pub labeler_agree: f64,
    #[serde(rename = "labelerTotal")]
    pub labeler_total: usize,
    #[serde(rename = "explCount")]
    pub expl_count: usize,
}


