use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use bqs_core::jobs::{
    CollectSpecJson, EvalSpecJson, JobSpecJson, RatingSpecJson, SearchSpecJson, SelfPlaySpecJson,
};
use bqs_proto::{
    CollectPlayerSpec, CollectionJobSpec, EvalJobSpec, JobKind, RatingJobSpec, SeedStrategy,
    SelfPlayJobSpec, SubmitJobRequest,
};

use crate::{ApiError, ApiResult, AppState};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SearchParamsBody {
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

impl SearchParamsBody {
    fn to_proto(&self) -> Option<bqs_proto::CollectSearchParams> {
        let all_zero = self.mcts_sims == 0
            && self.mcts_max_considered_actions == 0
            && self.mcts_c_scale == 0.0
            && self.mcts_gumbel_scale == 0.0
            && self.temperature == 0.0
            && !self.playout_cap_random
            && self.expectimax_node_budget == 0
            && self.expectimax_max_depth == 0
            && self.labeler_mode.is_empty()
            && self.labeler_budget_mult == 0
            && !self.expl_enabled
            && self.expl_topk == 0
            && self.expl_temp_start == 0.0
            && self.expl_temp_end == 0.0
            && self.expl_temp_decay_steps == 0;
        if all_zero {
            return None;
        }
        Some(bqs_proto::CollectSearchParams {
            mcts_sims: self.mcts_sims,
            mcts_max_considered_actions: self.mcts_max_considered_actions,
            mcts_c_scale: self.mcts_c_scale,
            mcts_gumbel_scale: self.mcts_gumbel_scale,
            temperature: self.temperature,
            playout_cap_random: self.playout_cap_random,
            expectimax_node_budget: self.expectimax_node_budget,
            expectimax_max_depth: self.expectimax_max_depth,
            labeler_mode: self.labeler_mode.clone(),
            labeler_budget_mult: self.labeler_budget_mult,
            expl_enabled: self.expl_enabled,
            expl_topk: self.expl_topk,
            expl_temp_start: self.expl_temp_start,
            expl_temp_end: self.expl_temp_end,
            expl_temp_decay_steps: self.expl_temp_decay_steps,
        })
    }

    fn from_json(s: &Option<SearchSpecJson>) -> Option<Self> {
        let s = s.as_ref()?;
        Some(Self {
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

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PlayerBody {
    pub kind: String,
    pub network_sha: String,
    pub search: Option<SearchParamsBody>,
}

impl PlayerBody {
    fn to_proto(&self) -> CollectPlayerSpec {
        CollectPlayerSpec {
            kind: self.kind.clone(),
            network_sha: self.network_sha.clone(),
            search: self.search.as_ref().and_then(|s| s.to_proto()),
        }
    }

    fn from_json(p: &bqs_core::jobs::PlayerSpecJson) -> Self {
        Self {
            kind: p.kind.clone(),
            network_sha: p.network_sha.clone(),
            search: SearchParamsBody::from_json(&p.search),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SeedStrategyBody {
    pub kind: String,
    pub fixed_seed: u64,
    pub base_seed: u64,
    pub min_seed: u64,
    pub max_seed: u64,
}

impl SeedStrategyBody {
    fn to_proto(&self) -> Result<SeedStrategy, String> {
        let kind =
            match self.kind.as_str() {
                "fixed" => bqs_proto::SeedStrategyKind::SeedFixed as i32,
                "random" => bqs_proto::SeedStrategyKind::SeedRandom as i32,
                "incremental" => bqs_proto::SeedStrategyKind::SeedIncremental as i32,
                "range" => bqs_proto::SeedStrategyKind::SeedRange as i32,
                other => return Err(format!(
                    "unknown seedStrategy.kind \"{other}\"（可选 fixed/random/incremental/range）"
                )),
            };
        Ok(SeedStrategy {
            kind,
            fixed_seed: self.fixed_seed,
            base_seed: self.base_seed,
            min_seed: self.min_seed,
            max_seed: self.max_seed,
        })
    }

    fn from_json(s: &bqs_core::jobs::SeedStrategyJson) -> Self {
        let kind = match bqs_proto::SeedStrategyKind::try_from(s.kind) {
            Ok(bqs_proto::SeedStrategyKind::SeedFixed) => "fixed",
            Ok(bqs_proto::SeedStrategyKind::SeedRandom) => "random",
            Ok(bqs_proto::SeedStrategyKind::SeedIncremental) => "incremental",
            Ok(bqs_proto::SeedStrategyKind::SeedRange) => "range",
            _ => "fixed",
        };
        Self {
            kind: kind.into(),
            fixed_seed: s.fixed_seed,
            base_seed: s.base_seed,
            min_seed: s.min_seed,
            max_seed: s.max_seed,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CollectBody {
    pub variant: String,

    pub scenario: String,
    pub seed_strategy: SeedStrategyBody,
    pub player_a: PlayerBody,
    pub player_b: PlayerBody,

    pub data_kind: String,
    pub total_games: u64,
}

impl CollectBody {
    fn to_proto(&self) -> Result<CollectionJobSpec, String> {
        let scenario = match self.scenario.as_str() {
            "" | "standard" => bqs_proto::OpeningScenario::ScenarioStandard as i32,
            "all_revealed" => bqs_proto::OpeningScenario::ScenarioAllRevealed as i32,
            other => {
                return Err(format!(
                    "unknown scenario \"{other}\"（可选 standard/all_revealed）"
                ))
            }
        };
        let data_kind = parse_data_kind(&self.data_kind)?;
        Ok(CollectionJobSpec {
            variant: self.variant.clone(),
            scenario,
            seed_strategy: Some(self.seed_strategy.to_proto()?),
            player_a: Some(self.player_a.to_proto()),
            player_b: Some(self.player_b.to_proto()),
            data_kind,
            total_games: self.total_games,
        })
    }

    fn from_json(c: &CollectSpecJson) -> Self {
        Self {
            variant: c.variant.clone(),
            scenario: match bqs_proto::OpeningScenario::try_from(c.scenario) {
                Ok(bqs_proto::OpeningScenario::ScenarioAllRevealed) => "all_revealed".into(),
                _ => "standard".into(),
            },
            seed_strategy: SeedStrategyBody::from_json(&c.seed_strategy),
            player_a: PlayerBody::from_json(&c.player_a),
            player_b: PlayerBody::from_json(&c.player_b),
            data_kind: data_kind_name(c.data_kind).into(),
            total_games: c.total_games,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SelfPlayBody {
    pub variant: String,
    pub data_kind: String,
    pub network_sha: String,
    pub opponent_sha: String,
    pub mix_ratio: f64,
    pub search: Option<SearchParamsBody>,

    pub total_games: u64,
}

impl SelfPlayBody {
    fn to_proto(&self) -> Result<SelfPlayJobSpec, String> {
        Ok(SelfPlayJobSpec {
            variant: self.variant.clone(),
            data_kind: parse_data_kind(&self.data_kind)?,
            network_sha: self.network_sha.clone(),
            opponent_sha: self.opponent_sha.clone(),
            mix_ratio: self.mix_ratio,
            search: self.search.as_ref().and_then(|s| s.to_proto()),
            total_games: self.total_games,
        })
    }

    fn from_json(s: &SelfPlaySpecJson) -> Self {
        Self {
            variant: s.variant.clone(),
            data_kind: data_kind_name(s.data_kind).into(),
            network_sha: s.network_sha.clone(),
            opponent_sha: s.opponent_sha.clone(),
            mix_ratio: s.mix_ratio,
            search: SearchParamsBody::from_json(&s.search),
            total_games: s.total_games,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EvalBody {
    pub variant: String,

    pub network_sha: String,
    pub opponent_kind: String,
    pub opponent_network_sha: String,
    pub search: Option<SearchParamsBody>,
    pub total_games: u64,
}

impl EvalBody {
    fn to_proto(&self) -> Result<EvalJobSpec, String> {
        Ok(EvalJobSpec {
            variant: self.variant.clone(),
            network_sha: self.network_sha.clone(),
            opponent_kind: self.opponent_kind.clone(),
            opponent_network_sha: self.opponent_network_sha.clone(),
            search: self.search.as_ref().and_then(|s| s.to_proto()),
            total_games: self.total_games,
        })
    }

    fn from_json(e: &EvalSpecJson) -> Self {
        Self {
            variant: e.variant.clone(),
            network_sha: e.network_sha.clone(),
            opponent_kind: e.opponent_kind.clone(),
            opponent_network_sha: e.opponent_network_sha.clone(),
            search: SearchParamsBody::from_json(&e.search),
            total_games: e.total_games,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RatingBody {
    pub variant: String,

    pub candidate_sha: String,

    pub opponent_sha: String,

    pub total_games: u64,

    pub sprt_elo0: f64,
    pub sprt_elo1: f64,
    pub sprt_alpha: f64,
    pub sprt_beta: f64,

    pub side_a: Option<PlayerBody>,

    pub side_b: Option<PlayerBody>,
}

impl RatingBody {
    fn to_proto(&self) -> RatingJobSpec {
        RatingJobSpec {
            variant: self.variant.clone(),
            candidate_sha: self.candidate_sha.clone(),
            opponent_sha: self.opponent_sha.clone(),
            total_games: self.total_games,
            sprt_elo0: self.sprt_elo0,
            sprt_elo1: self.sprt_elo1,
            sprt_alpha: self.sprt_alpha,
            sprt_beta: self.sprt_beta,
            side_a: self.side_a.as_ref().map(|p| p.to_proto()),
            side_b: self.side_b.as_ref().map(|p| p.to_proto()),
        }
    }

    fn from_json(r: &RatingSpecJson) -> Self {
        let (side_a, side_b) = r.resolved_sides();
        Self {
            variant: r.variant.clone(),
            candidate_sha: r.candidate_sha.clone(),
            opponent_sha: r.opponent_sha.clone(),
            total_games: r.total_games,
            sprt_elo0: r.sprt_elo0,
            sprt_elo1: r.sprt_elo1,
            sprt_alpha: r.sprt_alpha,
            sprt_beta: r.sprt_beta,
            side_a: Some(PlayerBody::from_json(&side_a)),
            side_b: Some(PlayerBody::from_json(&side_b)),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SubmitJobBody {
    pub kind: String,

    pub priority: u32,
    pub requester: String,
    pub collect: Option<CollectBody>,
    pub selfplay: Option<SelfPlayBody>,
    pub eval: Option<EvalBody>,
    pub rating: Option<RatingBody>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RatingStatsView {
    pub ll: i64,
    pub ld: i64,
    pub dd: i64,
    pub wl: i64,
    pub dw: i64,
    pub ww: i64,
    pub num_games: i64,
    pub match_status: String,

    pub side_a_kind: String,
    pub side_b_kind: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    pub job_id: String,
    pub kind: String,
    pub priority: i64,
    pub state: String,
    pub total_games: u64,
    pub completed_games: u64,
    pub dispatched_games: u64,
    pub created_at: i64,
    pub updated_at: i64,

    pub spec: serde_json::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating_stats: Option<RatingStatsView>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitJobView {
    pub accepted: bool,
    pub message: String,
    pub job_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobControlView {
    pub accepted: bool,
    pub message: String,
    pub state: String,
}

fn parse_job_kind(s: &str) -> Result<JobKind, ApiError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "collect" => Ok(JobKind::JobCollect),
        "selfplay" => Ok(JobKind::JobSelfplay),
        "eval" => Ok(JobKind::JobEval),
        "rating" => Ok(JobKind::JobRating),
        other => Err(ApiError::bad_request(format!(
            "未知 job kind {other:?}（可选 collect / selfplay / eval / rating）"
        ))),
    }
}

fn kind_name(k: JobKind) -> &'static str {
    match k {
        JobKind::JobCollect => "collect",
        JobKind::JobSelfplay => "selfplay",
        JobKind::JobEval => "eval",
        JobKind::JobRating => "rating",
    }
}

fn parse_data_kind(s: &str) -> Result<i32, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "resnet" | "onnx" => Ok(bqs_proto::DataKind::DataResnet as i32),
        "nnue" => Ok(bqs_proto::DataKind::DataNnue as i32),
        other => Err(format!("未知数据类别 {other:?}（可选 resnet / nnue）")),
    }
}

fn data_kind_name(k: i32) -> &'static str {
    if k == bqs_proto::DataKind::DataNnue as i32 {
        "nnue"
    } else {
        "resnet"
    }
}

pub async fn submit_job(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<SubmitJobView>> {
    let body: SubmitJobBody = crate::decode_body(&bytes, crate::MAX_CONTROL_BODY)?;
    let requester = body.requester.clone();
    let kind = parse_job_kind(&body.kind)?;
    let req = SubmitJobRequest {
        requester: body.requester,
        kind: kind as i32,
        priority: body.priority,
        collect: match kind {
            JobKind::JobCollect => Some(
                body.collect
                    .ok_or_else(|| ApiError::bad_request("kind=collect 需要 collect 规格"))?
                    .to_proto()
                    .map_err(ApiError::bad_request)?,
            ),
            _ => None,
        },
        selfplay: match kind {
            JobKind::JobSelfplay => Some(
                body.selfplay
                    .ok_or_else(|| ApiError::bad_request("kind=selfplay 需要 selfplay 规格"))?
                    .to_proto()
                    .map_err(ApiError::bad_request)?,
            ),
            _ => None,
        },
        eval: match kind {
            JobKind::JobEval => Some(
                body.eval
                    .ok_or_else(|| ApiError::bad_request("kind=eval 需要 eval 规格"))?
                    .to_proto()
                    .map_err(ApiError::bad_request)?,
            ),
            _ => None,
        },
        rating: match kind {
            JobKind::JobRating => Some(
                body.rating
                    .ok_or_else(|| ApiError::bad_request("kind=rating 需要 rating 规格"))?
                    .to_proto(),
            ),
            _ => None,
        },
    };
    let (accepted, message, job_id) = st
        .server
        .jobs
        .submit(&req)
        .map_err(|e| ApiError::internal(e.to_string()))?;
    if accepted {
        tracing::info!(
            "[webui] job submitted requester={} kind={} id={}",
            requester,
            body.kind,
            job_id
        );
    }
    Ok(Json(SubmitJobView {
        accepted,
        message,
        job_id,
    }))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub state: Option<String>,
    pub kind: Option<String>,
}

pub async fn list_jobs(
    State(st): State<AppState>,
    Query(q): Query<ListJobsQuery>,
) -> ApiResult<Json<HashMap<String, Vec<JobView>>>> {
    let kind = match q.kind.as_deref() {
        None | Some("") => None,
        Some(k) => Some(parse_job_kind(k)? as i64),
    };
    let state = q.state.as_deref().filter(|s| !s.is_empty());
    let rows = st
        .server
        .jobs
        .list(state, kind)
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let jobs = rows
        .iter()
        .map(|r| {
            let parsed = JobSpecJson::parse(&r.spec_json);
            let spec = match &parsed {
                Some(JobSpecJson::Collect(c)) => serde_json::to_value(CollectBody::from_json(c)),
                Some(JobSpecJson::SelfPlay(s)) => serde_json::to_value(SelfPlayBody::from_json(s)),
                Some(JobSpecJson::Eval(e)) => serde_json::to_value(EvalBody::from_json(e)),
                Some(JobSpecJson::Rating(rt)) => serde_json::to_value(RatingBody::from_json(rt)),
                None => Ok(serde_json::Value::Null),
            }
            .unwrap_or(serde_json::Value::Null);

            let rating_stats = match (&parsed, JobKind::try_from(r.kind as i32)) {
                (Some(JobSpecJson::Rating(rt)), Ok(JobKind::JobRating)) if rt.match_id != 0 => st
                    .server
                    .ports
                    .matches
                    .get_match(rt.match_id)
                    .ok()
                    .flatten()
                    .map(|m| {
                        let (sa, sb) = rt.resolved_sides();
                        RatingStatsView {
                            ll: m.pairs.0[0],
                            ld: m.pairs.0[1],
                            dd: m.pairs.0[2],
                            wl: m.pairs.0[3],
                            dw: m.pairs.0[4],
                            ww: m.pairs.0[5],
                            num_games: m.num_games,
                            match_status: m.status,
                            side_a_kind: sa.kind.clone(),
                            side_b_kind: sb.kind.clone(),
                        }
                    }),
                _ => None,
            };
            JobView {
                job_id: r.job_id.clone(),
                kind: JobKind::try_from(r.kind as i32)
                    .map(kind_name)
                    .unwrap_or("unknown")
                    .to_string(),
                priority: r.priority,
                state: r.state.clone(),
                total_games: r.total_games as u64,
                completed_games: r.completed_games as u64,
                dispatched_games: r.dispatched_games as u64,
                created_at: r.created_at,
                updated_at: r.updated_at,
                spec,
                rating_stats,
            }
        })
        .collect();
    Ok(Json(HashMap::from([("jobs".to_string(), jobs)])))
}

pub async fn control_job(
    State(st): State<AppState>,
    Path((job_id, action)): Path<(String, String)>,
) -> ApiResult<Json<JobControlView>> {
    match action.as_str() {
        "pause" | "resume" | "cancel" => {}
        other => {
            return Err(ApiError::bad_request(format!(
                "未知控制动作 {other:?}（可选 pause / resume / cancel）"
            )))
        }
    }
    let (accepted, message, state) = st
        .server
        .jobs
        .control(&job_id, &action)
        .map_err(|e| ApiError::internal(e.to_string()))?;
    if !accepted {
        if message.contains("not_found") {
            return Err(ApiError::not_found(message));
        }
        return Err(ApiError::conflict(message));
    }
    tracing::info!("[webui] job {} {} → {}", job_id, action, state);
    Ok(Json(JobControlView {
        accepted,
        message,
        state,
    }))
}
