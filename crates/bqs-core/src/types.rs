use bqs_judgment::Hexanomial;
use serde::Serialize;

pub mod network_status {
    pub const CANDIDATE: &str = "candidate";
    pub const BEST: &str = "best";
    pub const REJECTED: &str = "rejected";
    pub const ARCHIVED: &str = "archived";
}

pub mod match_status {
    pub const RUNNING: &str = "running";
    pub const CONCLUDED: &str = "concluded";

    pub const CANCELLED: &str = "cancelled";
}

#[derive(Debug, Clone, PartialEq)]
pub struct Network {
    pub sha: String,
    pub parent_sha: Option<String>,
    pub created_at: i64,
    pub is_best: bool,

    pub status: String,
    pub notes: Option<String>,

    pub format: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Match {
    pub id: i64,
    pub candidate: String,
    pub opponent: String,

    pub status: String,

    pub pairs: Hexanomial,
    pub num_games: i64,

    pub target_games: i64,
}

impl Match {
    pub fn running(&self) -> bool {
        self.status == match_status::RUNNING
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvalQueueEntry {
    pub network_sha: String,
    pub opponent_spec: String,

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
    pub fn reached_target(&self) -> bool {
        self.num_games >= self.target_games
    }

    pub fn win_rate(&self) -> f64 {
        if self.num_games == 0 {
            0.0
        } else {
            self.wins as f64 / self.num_games as f64
        }
    }
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct Episode {
    pub id: i64,
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

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Trainer {
    pub id: String,
    pub last_seen: i64,
    pub variant: String,

    pub kind: i64,

    pub consumed_key: String,
    pub consumed_id: i64,
    pub samples_consumed: i64,
    pub backlog: i64,
    pub buffered: i64,

    pub health_status: String,
    pub last_error: String,

    pub progress: String,
}

impl Trainer {
    pub fn healthy(&self) -> bool {
        self.health_status.is_empty() || self.health_status == "ok"
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeaturePack {
    pub id: i64,
    pub episode_object_key: String,
    pub pack_version: i64,
    pub object_key: String,
    pub created_at: i64,
}
