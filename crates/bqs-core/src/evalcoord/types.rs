use std::sync::Arc;

use serde::Serialize;

use crate::{CoreResult, ports::NetworkStore, types::network_status};

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EvalOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opponents: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub games: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcts_sims: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_progress_n: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_progress_eps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_eps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor_win_rate: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_win_rate: Option<f64>,
}

pub trait BestMutator: Send + Sync {
    fn promote_best(&self, sha: &str) -> CoreResult<()>;
    fn reject_network(&self, sha: &str) -> CoreResult<()>;
}

pub struct StoreBestMutator {
    nets: Arc<dyn NetworkStore>,
}

impl StoreBestMutator {
    pub fn new(nets: Arc<dyn NetworkStore>) -> Self {
        Self { nets }
    }
}

impl BestMutator for StoreBestMutator {
    fn promote_best(&self, sha: &str) -> CoreResult<()> {
        self.nets.promote_best(sha)
    }

    fn reject_network(&self, sha: &str) -> CoreResult<()> {
        self.nets
            .update_network_status(sha, network_status::REJECTED)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PendingEval {
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
}

impl PendingEval {
    pub(super) fn remaining(&self) -> i64 {
        (self.target_games - self.num_games).max(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedEvalTask {
    pub task_id: String,
    pub network_sha: String,
    pub opponent_spec: String,

    pub games: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvalReportAck {
    pub accepted: bool,
    pub message: String,
    pub concluded: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalRunningJob {
    #[serde(rename = "networkSha")]
    pub network_sha: String,
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,
    #[serde(rename = "numGames")]
    pub num_games: i64,
    #[serde(rename = "targetGames")]
    pub target_games: i64,
    #[serde(rename = "inFlight")]
    pub in_flight: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalProgress {
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,

    pub versions: usize,

    pub no_progress: usize,
    #[serde(rename = "latestWinRate")]
    pub latest_win_rate: f64,
    #[serde(rename = "latestSha")]
    pub latest_sha: String,
}
