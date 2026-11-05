use serde::Serialize;

use bqs_core::ports::{EpisodeRateEstimate, GenerationStats, KindStats, StoreCounts};
use bqs_judgment::Hexanomial;

#[derive(Debug, Serialize)]
pub struct OkView {
    #[serde(rename = "ok")]
    pub ok: bool,
}

pub use bqs_core::data_kind::display_name as data_kind_name;

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
}

impl From<StoreCounts> for CountsView {
    fn from(c: StoreCounts) -> Self {
        CountsView {
            networks: c.networks,
            candidates: c.candidates,
            matches_running: c.matches_running,
            episodes: c.episodes,
            episode_games: c.episode_games,
            workers: c.workers,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatusView {
    #[serde(rename = "variant")]
    pub variant: String,
    #[serde(rename = "initialRevealed")]
    pub initial_revealed: i32,
    #[serde(rename = "dataKind")]
    pub data_kind: String,
    #[serde(rename = "minClientVersion")]
    pub min_client_version: String,
    #[serde(rename = "sprt")]
    pub sprt: SprtView,
    #[serde(rename = "counts")]
    pub counts: CountsView,
    #[serde(rename = "shouldStop")]
    pub should_stop: bool,
    #[serde(rename = "stopReason")]
    pub stop_reason: String,
}

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
        KindStatsView {
            episodes: k.episodes,
            games: k.games,
            steps: k.steps,
        }
    }
}

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

    #[serde(rename = "ip")]
    pub ip: String,
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

    #[serde(rename = "jobId")]
    pub job_id: String,
}

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

    #[serde(rename = "seedBase")]
    pub seed_base: Option<String>,

    #[serde(rename = "pairsParallel")]
    pub pairs_parallel: Option<bool>,

    #[serde(rename = "pairSeedRefresh")]
    pub pair_seed_refresh: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct EpisodeGameView {
    #[serde(rename = "gameIndex")]
    pub game_index: i64,
    #[serde(rename = "pairIndex")]
    pub pair_index: i64,
    #[serde(rename = "pairPosition")]
    pub pair_position: i64,
    #[serde(rename = "gameSeed")]
    pub game_seed: Option<String>,
    #[serde(rename = "aIsRed")]
    pub a_is_red: bool,
    #[serde(rename = "seedRefreshed")]
    pub seed_refreshed: bool,
    #[serde(rename = "winner")]
    pub winner: Option<i64>,
    #[serde(rename = "steps")]
    pub steps: i64,
}

impl EpisodeGameView {
    pub fn from_row(g: &bqs_core::types::EpisodeGameRow) -> Self {
        EpisodeGameView {
            game_index: g.game_index,
            pair_index: g.pair_index,
            pair_position: g.pair_position,
            game_seed: g.game_seed.map(|s| (s as u64).to_string()),
            a_is_red: g.a_is_red,
            seed_refreshed: g.seed_refreshed,
            winner: g.winner,
            steps: g.steps,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct GameLookupItemView {
    #[serde(rename = "episode")]
    pub episode: EpisodeView,
    #[serde(rename = "game")]
    pub game: EpisodeGameView,
}

#[derive(Debug, Serialize)]
pub struct GameLookupPageView {
    #[serde(rename = "items")]
    pub items: Vec<GameLookupItemView>,
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

#[derive(Debug, Serialize)]
pub struct StorageProfileView {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "endpoint")]
    pub endpoint: String,
    #[serde(rename = "region")]
    pub region: String,

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

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct HiddenBagView {
    #[serde(rename = "red")]
    pub red: [i64; 7],
    #[serde(rename = "black")]
    pub black: [i64; 7],
}

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

    #[serde(rename = "redIsA")]
    pub red_is_a: bool,

    #[serde(rename = "seed")]
    pub seed: Option<String>,

    #[serde(rename = "seedRefreshed")]
    pub seed_refreshed: bool,

    #[serde(rename = "pairIndex")]
    pub pair_index: u32,

    #[serde(rename = "pairPosition")]
    pub pair_position: u32,
}

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

    #[serde(rename = "fen")]
    pub fen: String,
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
    pub seed: String,
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

    #[serde(rename = "initialFen")]
    pub initial_fen: Option<String>,

    #[serde(rename = "finalFen")]
    pub final_fen: Option<String>,
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
