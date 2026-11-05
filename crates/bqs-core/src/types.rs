use bqs_judgment::Hexanomial;

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
pub struct MatchConclusion {
    pub verdict: String,
    pub llr: f64,
    pub sprt_elo0: f64,
    pub sprt_elo1: f64,
    pub sprt_alpha: f64,
    pub sprt_beta: f64,
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

    pub conclusion: Option<MatchConclusion>,
}

impl Match {
    pub fn running(&self) -> bool {
        self.status == match_status::RUNNING
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

    pub seed_base: Option<i64>,

    pub pairs_parallel: Option<bool>,

    pub pair_seed_refresh: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EpisodeGameRow {
    pub game_index: i64,
    pub pair_index: i64,
    pub pair_position: i64,
    pub game_seed: Option<i64>,
    pub a_is_red: bool,
    pub seed_refreshed: bool,
    pub winner: Option<i64>,
    pub steps: i64,
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
    pub seed_base: Option<i64>,
    pub pairs_parallel: Option<bool>,
    pub pair_seed_refresh: Option<bool>,

    pub games: Vec<EpisodeGameRow>,
}

#[derive(Debug, Clone, Default)]
pub struct ImportEpisodeAck {
    pub id: i64,
    pub object_key: String,
    pub upload_url: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Worker {
    pub id: String,

    pub ip: String,
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
