//! 领域值类型：store 行的内存投影 + 上报增量。与 SQLite schema 字段一一对应
//! （bqs-store 的迁移链终态 = Go 版 v8）。

use bqs_judgment::Hexanomial;
use serde::Serialize;

/// 网络谱系状态（`networks.status`，字符串常量口径与 Go 版一致）。
pub mod network_status {
    pub const CANDIDATE: &str = "candidate";
    pub const BEST: &str = "best";
    pub const REJECTED: &str = "rejected";
    pub const ARCHIVED: &str = "archived";
}

/// 对打状态（`matches.status`）。
pub mod match_status {
    pub const RUNNING: &str = "running";
    pub const CONCLUDED: &str = "concluded";
    /// rating job 被 ControlJob cancel 时置位（对打终止，不晋级不标 rejected）。
    pub const CANCELLED: &str = "cancelled";
}

#[derive(Debug, Clone, PartialEq)]
pub struct Network {
    pub sha: String,
    pub parent_sha: Option<String>,
    pub created_at: i64,
    pub is_best: bool,
    /// candidate | best | rejected | archived
    pub status: String,
    pub notes: Option<String>,
    /// onnx | pt | nnue（对象键扩展名 = 权重格式）
    pub format: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Match {
    pub id: i64,
    pub candidate: String,
    pub opponent: String,
    /// running | concluded
    pub status: String,
    /// 六桶成对计数 [LL, LD, DD, WL, DW, WW]
    pub pairs: Hexanomial,
    pub num_games: i64,
    /// 目标成对局数（实际局数 = 值 × 2）
    pub target_games: i64,
}

impl Match {
    pub fn running(&self) -> bool {
        self.status == match_status::RUNNING
    }
}

/// eval_queue 行：流式评估作业（一个 (network, spec) 一行，增量累加）。
#[derive(Debug, Clone, PartialEq)]
pub struct EvalQueueEntry {
    pub network_sha: String,
    pub opponent_spec: String,
    /// 模型类对手的网络 sha（expectimax_nnue / mcts_resnet / policy_resnet）；
    /// 规则类对手恒空串。
    pub opponent_sha: String,
    pub target_games: i64,
    pub per_task_games: i64,
    pub attempts: i64,
    pub wins: i64,
    pub draws: i64,
    pub losses: i64,
    pub num_games: i64,
    pub created_at: i64,
    pub red_wins: i64,
    pub black_wins: i64,
    pub draw_no_capture: i64,
    pub draw_truncation: i64,
}

impl EvalQueueEntry {
    /// 累计局数达成目标即可收尾聚合。
    pub fn reached_target(&self) -> bool {
        self.num_games >= self.target_games
    }

    /// 主网络视角当前胜率（局数不足时按 0 处理）。
    pub fn win_rate(&self) -> f64 {
        if self.num_games == 0 {
            0.0
        } else {
            self.wins as f64 / self.num_games as f64
        }
    }
}

/// 评估上报增量（累加进 eval_queue 行）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvalDelta {
    pub wins: i64,
    pub draws: i64,
    pub losses: i64,
    pub games: i64,
    pub red_wins: i64,
    pub black_wins: i64,
    pub draw_no_capture: i64,
    pub draw_truncation: i64,
}

/// eval_results 行：评估聚合结果（`UNIQUE(network_sha, opponent_spec)` 覆盖去重；
/// **无外键**——规则对手不存在 networks 行）。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvalResult {
    pub id: i64,
    pub network_sha: String,
    pub opponent_spec: String,
    pub wins: i64,
    pub draws: i64,
    pub losses: i64,
    pub num_games: i64,
    pub avg_moves: f64,
    pub created_at: i64,
    pub red_wins: i64,
    pub black_wins: i64,
    pub draw_no_capture: i64,
    pub draw_truncation: i64,
}

impl EvalResult {
    pub fn win_rate(&self) -> f64 {
        if self.num_games == 0 {
            0.0
        } else {
            self.wins as f64 / self.num_games as f64
        }
    }
}

/// episodes 行（episode 登记元数据）。
#[derive(Debug, Clone, PartialEq)]
pub struct Episode {
    pub id: i64,
    pub worker_id: String,
    pub task_id: String,
    pub network_sha: String,
    pub game_count: i64,
    pub total_steps: i64,
    /// 1 红 / -1 黑 / 0 平（批级聚合；逐局四分见下）
    pub winner: i64,
    pub object_key: String,
    pub created_at: i64,
    /// 0=DATA_RESNET 1=DATA_NNUE 2=DATA_NONE
    pub kind: i64,
    pub opponent_sha: String,
    pub opponent_spec: String,
    pub match_id: i64,
    pub red_wins: i64,
    pub black_wins: i64,
    pub draw_no_capture: i64,
    pub draw_truncation: i64,
}

/// 新 episode 登记草稿（id 由数据库分配）。
#[derive(Debug, Clone, Default)]
pub struct EpisodeDraft {
    pub worker_id: String,
    pub task_id: String,
    pub network_sha: String,
    pub game_count: i64,
    pub total_steps: i64,
    pub winner: i64,
    pub object_key: String,
    pub created_at: i64,
    pub kind: i64,
    pub opponent_sha: String,
    pub opponent_spec: String,
    pub match_id: i64,
    pub red_wins: i64,
    pub black_wins: i64,
    pub draw_no_capture: i64,
    pub draw_truncation: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Worker {
    pub id: String,
    pub last_seen: i64,
    pub threads: i64,
    pub completed_games: i64,
    pub client_version: String,
    pub memory_mb: i64,
    pub health_status: String,
    pub last_error: String,
    pub note: String,
}

/// trainer（消费端）心跳快照。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Trainer {
    pub id: String,
    pub last_seen: i64,
    pub variant: String,
    /// 消费的数据类别（DataKind as i32）
    pub kind: i64,
    /// 真实消费水位：最后被训练线程取走的记录所属对象键
    pub consumed_key: String,
    pub consumed_id: i64,
    pub samples_consumed: i64,
    pub backlog: i64,
    pub buffered: i64,
    /// ok | degraded | fatal（空 = 旧客户端，按 ok 语义）
    pub health_status: String,
    pub last_error: String,
    /// 训练节奏快照 JSON（透传，WebUI 展示用）
    pub progress: String,
}

impl Trainer {
    pub fn healthy(&self) -> bool {
        self.health_status.is_empty() || self.health_status == "ok"
    }
}

/// 特征包登记行（v2 数据契约：记录批次 × 特征格式版本）。
#[derive(Debug, Clone, PartialEq)]
pub struct FeaturePack {
    pub id: i64,
    pub episode_object_key: String,
    pub pack_version: i64,
    pub object_key: String,
    pub created_at: i64,
}
