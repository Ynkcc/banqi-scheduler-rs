use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use bqs_core::data_kind;
use bqs_core::jobs::{
    CollectSpecJson, JobSpecJson, PlayerSpecJson, RatingSpecJson, SearchSpecJson, SeedStrategyJson,
    SelfPlaySpecJson,
};
use bqs_proto::{
    CollectionJobSpec, JobKind, RatingJobSpec, SeedStrategy, SelfPlayJobSpec, SubmitJobRequest,
};

use crate::{ApiError, ApiResult, AppState};

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
        let kind = match self.kind.as_str() {
            "fixed" => bqs_proto::SeedStrategyKind::SeedFixed as i32,
            "random" => bqs_proto::SeedStrategyKind::SeedRandom as i32,
            "incremental" => bqs_proto::SeedStrategyKind::SeedIncremental as i32,
            "range" => bqs_proto::SeedStrategyKind::SeedRange as i32,
            other => {
                return Err(format!(
                    "unknown seedStrategy.kind \"{other}\"（可选 fixed/random/incremental/range）"
                ));
            }
        };
        Ok(SeedStrategy {
            kind,
            fixed_seed: self.fixed_seed,
            base_seed: self.base_seed,
            min_seed: self.min_seed,
            max_seed: self.max_seed,
        })
    }

    fn from_json(s: &SeedStrategyJson) -> Self {
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
    pub player_a: PlayerSpecJson,
    pub player_b: PlayerSpecJson,

    pub data_kind: String,
    pub total_games: u64,

    pub pairs_parallel: Option<bool>,

    pub pair_seed_refresh: Option<bool>,

    pub start_fens: Vec<String>,
}

impl CollectBody {
    fn to_proto(&self) -> Result<CollectionJobSpec, String> {
        let scenario = match self.scenario.as_str() {
            "" | "standard" => bqs_proto::OpeningScenario::ScenarioStandard as i32,
            "all_revealed" => bqs_proto::OpeningScenario::ScenarioAllRevealed as i32,
            other => {
                return Err(format!(
                    "unknown scenario \"{other}\"（可选 standard/all_revealed）"
                ));
            }
        };
        let data_kind = data_kind::parse(&self.data_kind).map(data_kind::to_proto)?;
        Ok(CollectionJobSpec {
            variant: self.variant.clone(),
            scenario,
            seed_strategy: Some(self.seed_strategy.to_proto()?),
            player_a: Some(self.player_a.to_proto()),
            player_b: Some(self.player_b.to_proto()),
            data_kind,
            total_games: self.total_games,
            pairs_parallel: self.pairs_parallel,
            pair_seed_refresh: self.pair_seed_refresh,
            start_fens: self.start_fens.clone(),
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
            player_a: c.player_a.clone(),
            player_b: c.player_b.clone(),
            data_kind: data_kind::display_name(c.data_kind as i64).into(),
            total_games: c.total_games,
            pairs_parallel: c.pairs_parallel,
            pair_seed_refresh: c.pair_seed_refresh,
            start_fens: c.start_fens.clone(),
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
    pub search: Option<SearchSpecJson>,

    pub total_games: u64,
}

impl SelfPlayBody {
    fn to_proto(&self) -> Result<SelfPlayJobSpec, String> {
        Ok(SelfPlayJobSpec {
            variant: self.variant.clone(),
            data_kind: data_kind::parse(&self.data_kind).map(data_kind::to_proto)?,
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
            data_kind: data_kind::display_name(s.data_kind as i64).into(),
            network_sha: s.network_sha.clone(),
            opponent_sha: s.opponent_sha.clone(),
            mix_ratio: s.mix_ratio,
            search: s.search.clone(),
            total_games: s.total_games,
        }
    }
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

    pub side_a: Option<PlayerSpecJson>,

    pub side_b: Option<PlayerSpecJson>,

    pub start_fens: Vec<String>,

    pub scenario: String,
}

impl RatingBody {
    fn to_proto(&self) -> Result<RatingJobSpec, String> {
        let scenario = match self.scenario.as_str() {
            "" | "standard" => bqs_proto::OpeningScenario::ScenarioStandard as i32,
            "all_revealed" => bqs_proto::OpeningScenario::ScenarioAllRevealed as i32,
            other => {
                return Err(format!(
                    "unknown scenario \"{other}\"（可选 standard/all_revealed）"
                ));
            }
        };
        Ok(RatingJobSpec {
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
            start_fens: self.start_fens.clone(),
            scenario,
        })
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
            side_a: Some(side_a),
            side_b: Some(side_b),
            start_fens: r.start_fens.clone(),
            scenario: match bqs_proto::OpeningScenario::try_from(r.scenario) {
                Ok(bqs_proto::OpeningScenario::ScenarioAllRevealed) => "all_revealed".into(),
                _ => "standard".into(),
            },
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
    pub rating: Option<RatingBody>,
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
        "rating" => Ok(JobKind::JobRating),
        other => Err(ApiError::bad_request(format!(
            "未知 job kind {other:?}（可选 collect / selfplay / rating）"
        ))),
    }
}

fn kind_name(k: JobKind) -> &'static str {
    match k {
        JobKind::JobCollect => "collect",
        JobKind::JobSelfplay => "selfplay",
        JobKind::JobRating => "rating",
    }
}

fn take_spec<T>(spec: Option<T>, kind: &str) -> Result<T, ApiError> {
    spec.ok_or_else(|| ApiError::bad_request(format!("kind={kind} 需要 {kind} 规格")))
}

fn spec_echo(parsed: Option<&JobSpecJson>) -> serde_json::Value {
    match parsed {
        Some(JobSpecJson::Collect(c)) => serde_json::to_value(CollectBody::from_json(c)),
        Some(JobSpecJson::SelfPlay(s)) => serde_json::to_value(SelfPlayBody::from_json(s)),
        Some(JobSpecJson::Rating(rt)) => serde_json::to_value(RatingBody::from_json(rt)),
        None => Ok(serde_json::Value::Null),
    }
    .unwrap_or(serde_json::Value::Null)
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
                take_spec(body.collect, "collect")?
                    .to_proto()
                    .map_err(ApiError::bad_request)?,
            ),
            _ => None,
        },
        selfplay: match kind {
            JobKind::JobSelfplay => Some(
                take_spec(body.selfplay, "selfplay")?
                    .to_proto()
                    .map_err(ApiError::bad_request)?,
            ),
            _ => None,
        },
        rating: match kind {
            JobKind::JobRating => Some(
                take_spec(body.rating, "rating")?
                    .to_proto()
                    .map_err(ApiError::bad_request)?,
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
#[serde(rename_all = "camelCase")]
pub struct ListJobsQuery {
    pub state: Option<String>,

    pub kind: Option<String>,

    pub job_id: Option<String>,

    pub limit: Option<u64>,

    pub offset: Option<u64>,
}

const LIST_JOBS_DEFAULT_LIMIT: u64 = 100;
const LIST_JOBS_MAX_LIMIT: u64 = 1000;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobListView {
    pub jobs: Vec<JobView>,

    pub total: u64,
    pub limit: u64,
    pub offset: u64,
}

fn job_view(st: &AppState, r: &bqs_core::ports::JobRow) -> JobView {
    let parsed = JobSpecJson::parse(&r.spec_json);
    let spec = spec_echo(parsed.as_ref());

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
}

pub async fn list_jobs(
    State(st): State<AppState>,
    Query(q): Query<ListJobsQuery>,
) -> ApiResult<Json<JobListView>> {
    let kind = match q.kind.as_deref() {
        None | Some("") => None,
        Some(k) => Some(parse_job_kind(k)? as i64),
    };
    let (limit, limit_out) = match q.limit {
        None => (
            Some(LIST_JOBS_DEFAULT_LIMIT as i64),
            LIST_JOBS_DEFAULT_LIMIT,
        ),
        Some(0) => (None, 0),
        Some(n) => (
            Some(n.min(LIST_JOBS_MAX_LIMIT) as i64),
            n.min(LIST_JOBS_MAX_LIMIT),
        ),
    };
    let offset = q.offset.unwrap_or(0).min(i64::MAX as u64) as i64;
    let page = st
        .server
        .jobs
        .list(&bqs_core::ports::JobQuery {
            job_id: q.job_id.clone().filter(|s| !s.is_empty()),
            state: q.state.clone().filter(|s| !s.is_empty()),
            kind,
            limit,
            offset,
        })
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let jobs = page.items.iter().map(|r| job_view(&st, r)).collect();
    Ok(Json(JobListView {
        jobs,
        total: page.total.max(0) as u64,
        limit: limit_out,
        offset: q.offset.unwrap_or(0),
    }))
}

pub async fn get_job(
    State(st): State<AppState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<JobView>> {
    let row = st
        .server
        .jobs
        .get(&job_id)
        .map_err(|e| ApiError::internal(e.to_string()))?
        .ok_or_else(|| ApiError::not_found(format!("job_not_found:{job_id}")))?;
    Ok(Json(job_view(&st, &row)))
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
            )));
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
