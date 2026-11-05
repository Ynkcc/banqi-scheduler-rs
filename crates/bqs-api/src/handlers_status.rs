use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::Json;

use bqs_core::dispatch::TaskStateStats;
use bqs_core::types::Network;
use bqs_judgment::sprt::{judge, llr, new_bounds};
use bqs_proto::RegisterNetworkRequest;

use crate::views::{
    EpisodeRateEstimateView, KindStatsView, MatchConclusionView, MatchView, NetworkView, OkView,
    StatusView, TaskView, WorkerView,
};
use crate::{read_endpoint, write_endpoint, ApiError, ApiResult, AppState, MAX_CONTROL_BODY};

fn finite_or(x: f64, fallback: f64) -> f64 {
    if x.is_finite() {
        x
    } else {
        fallback
    }
}

pub fn to_network_view(n: &Network) -> NetworkView {
    NetworkView {
        sha: n.sha.clone(),
        parent_sha: n.parent_sha.clone().unwrap_or_default(),
        created_at: n.created_at,
        is_best: n.is_best,
        status: n.status.clone(),
        notes: n.notes.clone().unwrap_or_default(),
    }
}

fn worker_stats(
    server: &bqs_core::server::Server,
) -> (
    Option<BTreeMap<String, KindStatsView>>,
    Option<BTreeMap<String, KindStatsView>>,
    Option<BTreeMap<String, EpisodeRateEstimateView>>,
) {
    let to_views = |m: BTreeMap<String, bqs_core::ports::KindStats>| {
        m.into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect::<BTreeMap<_, _>>()
    };
    match (
        server.ports.episodes.episode_stats_by_worker(3600),
        server.ports.episodes.episode_stats_by_worker(86_400),
        server.ports.episodes.episode_rate_estimates(86_400, 24),
    ) {
        (Ok(a), Ok(b), Ok(c)) => (Some(to_views(a)), Some(to_views(b)), {
            Some(c.into_iter().map(|(k, v)| (k, v.into())).collect())
        }),
        (a, b, c) => {
            if let Err(e) = &a {
                tracing::warn!("[webui] episode_stats_by_worker(1h)：{e}");
            }
            if let Err(e) = &b {
                tracing::warn!("[webui] episode_stats_by_worker(24h)：{e}");
            }
            if let Err(e) = &c {
                tracing::warn!("[webui] episode_rate_estimates：{e}");
            }
            (
                a.ok().map(to_views),
                b.ok().map(to_views),
                c.ok()
                    .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect()),
            )
        }
    }
}

pub async fn status(State(st): State<AppState>) -> ApiResult<Json<StatusView>> {
    read_endpoint(&st, |st| -> Result<StatusView, ApiError> {
        let server = &st.server;
        let counts: crate::views::CountsView = server.ports.stats.counts()?.into();
        let ctl = &server.ctl;
        let (should_stop, stop_reason) = ctl.should_stop();
        Ok(StatusView {
            variant: server.cfg.selfplay.variant.clone(),
            initial_revealed: ctl.initial_revealed(),
            data_kind: ctl.data_kind().as_str().to_string(),
            min_client_version: server.cfg.server.min_client_version.clone(),
            counts,
            should_stop,
            stop_reason,
        })
    })
    .await
}

pub async fn networks(State(st): State<AppState>) -> ApiResult<Json<Vec<NetworkView>>> {
    read_endpoint(&st, |st| -> Result<Vec<NetworkView>, ApiError> {
        let items = st.server.ports.networks.list_networks()?;
        Ok(items.iter().map(to_network_view).collect())
    })
    .await
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterNetworkBody {
    pub sha: String,

    #[serde(default)]
    pub parent_sha: String,

    #[serde(default)]
    pub notes: String,

    #[serde(default)]
    pub format: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterNetworkView {
    pub accepted: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_task_hint: Option<String>,
}

pub async fn register_network(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<RegisterNetworkView>> {
    let body: RegisterNetworkBody = crate::decode_body(&bytes, MAX_CONTROL_BODY)?;
    let sha = body.sha.trim().to_string();
    if sha.is_empty() {
        return Err(ApiError::bad_request("sha 不能为空"));
    }
    let parent_sha = body.parent_sha.trim().to_string();
    let notes = body.notes.trim().to_string();
    let format = body.format.trim().to_string();
    read_endpoint(&st, move |st| -> Result<RegisterNetworkView, ApiError> {
        let ack = st.server.register_network(RegisterNetworkRequest {
            sha,
            parent_sha,
            upload_url: String::new(),
            notes,
            format,
        })?;
        tracing::info!(
            "[webui] manual register network accepted={} message={}",
            ack.accepted,
            ack.message
        );
        Ok(RegisterNetworkView {
            accepted: ack.accepted,
            message: ack.message,
            match_task_hint: (!ack.match_task_hint.is_empty()).then_some(ack.match_task_hint),
        })
    })
    .await
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignUploadBody {
    pub sha: String,
    pub format: String,
    #[serde(default)]
    pub content_length: i64,
    #[serde(default)]
    pub content_sha256: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignUploadView {
    pub upload_url: String,
    pub object_key: String,
}

pub async fn sign_upload(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<SignUploadView>> {
    let body: SignUploadBody = crate::decode_body(&bytes, MAX_CONTROL_BODY)?;
    let sha = body.sha.trim().to_string();
    let format = body.format.trim().to_ascii_lowercase();
    if sha.len() != 64 {
        return Err(ApiError::bad_request(format!(
            "invalid_sha_len={} (want 64 hex chars)",
            sha.len()
        )));
    }
    if !bqs_core::consts::WEIGHT_FORMATS.contains(&format.as_str()) {
        return Err(ApiError::bad_request(format!(
            "unsupported_format=\"{}\" (want onnx/pt/nnue)",
            body.format.trim()
        )));
    }
    let object_key = format!("networks/{sha}.{format}");
    let object_key_inner = object_key.clone();
    let upload_url = read_endpoint(&st, move |st| -> Result<String, ApiError> {
        let url = st.server.signer.presign_put(
            &object_key_inner,
            bqs_core::consts::PRESIGN_TTL.as_secs() as i64,
        )?;
        tracing::info!(
            "[webui] sign network upload key={} len={}",
            object_key_inner,
            body.content_length
        );
        Ok(url)
    })
    .await?;
    Ok(Json(SignUploadView {
        upload_url: upload_url.0,
        object_key,
    }))
}

pub async fn matches(State(st): State<AppState>) -> ApiResult<Json<Vec<MatchView>>> {
    read_endpoint(&st, |st| -> Result<Vec<MatchView>, ApiError> {
        let server = &st.server;
        let items = server.ports.matches.list_matches()?;

        let mut side_kinds: BTreeMap<i64, (String, String)> = BTreeMap::new();

        let mut sprt_by_match: BTreeMap<i64, (f64, f64, f64, f64)> = BTreeMap::new();
        for j in server
            .jobs
            .list(&bqs_core::ports::JobQuery {
                kind: Some(bqs_proto::JobKind::JobRating as i64),
                ..bqs_core::ports::JobQuery::all()
            })
            .map(|p| p.items)
            .unwrap_or_default()
        {
            if let Some(bqs_core::jobs::JobSpecJson::Rating(spec)) =
                bqs_core::jobs::JobSpecJson::parse(&j.spec_json)
            {
                if spec.match_id != 0 {
                    let (a, b) = spec.resolved_sides();
                    side_kinds.insert(spec.match_id, (a.kind.clone(), b.kind.clone()));
                    sprt_by_match.insert(
                        spec.match_id,
                        (
                            spec.sprt_elo0,
                            spec.sprt_elo1,
                            spec.sprt_alpha,
                            spec.sprt_beta,
                        ),
                    );
                }
            }
        }
        let mut out = Vec::with_capacity(items.len());
        for m in items {
            let kinds = side_kinds.get(&m.id);
            let display = |sha: &str, kind: Option<&String>| -> String {
                if sha.is_empty() {
                    kind.cloned().unwrap_or_default()
                } else {
                    sha.to_string()
                }
            };

            let sprt_params = sprt_by_match.get(&m.id).copied();
            let bounds = sprt_params
                .map(|(_, _, alpha, beta)| {
                    let b = new_bounds(alpha, beta);
                    bqs_judgment::Bounds {
                        lower: finite_or(b.lower, 0.0),
                        upper: finite_or(b.upper, 0.0),
                    }
                })
                .unwrap_or(bqs_judgment::Bounds {
                    lower: 0.0,
                    upper: 0.0,
                });
            let mut mv = MatchView {
                id: m.id,
                candidate: display(&m.candidate, kinds.map(|k| &k.0)),
                opponent: display(&m.opponent, kinds.map(|k| &k.1)),
                status: m.status.clone(),
                pairs: m.pairs,
                num_games: m.num_games,
                target_games: m.target_games,
                score: None,
                llr: 0.0,
                lower: bounds.lower,
                upper: bounds.upper,
                verdict: bqs_judgment::Verdict::Continue.as_str().into(),
                conclusion: m.conclusion.as_ref().map(|c| MatchConclusionView {
                    verdict: c.verdict.clone(),
                    llr: finite_or(c.llr, 0.0),
                    sprt_elo0: c.sprt_elo0,
                    sprt_elo1: c.sprt_elo1,
                    sprt_alpha: c.sprt_alpha,
                    sprt_beta: c.sprt_beta,
                }),
            };
            if let Some((elo0, elo1, _, _)) = sprt_params {
                if m.pairs.total() > 0 {
                    match llr(m.pairs, elo0, elo1) {
                        Ok(l) => {
                            mv.llr = finite_or(l, 0.0);
                            if let Ok((score, _)) = m.pairs.stats() {
                                mv.score = Some(finite_or(score, 0.0));
                            }
                        }
                        Err(e) => tracing::warn!("[webui] llr match={}：{e}", m.id),
                    }
                    if let Ok((verdict, _)) = judge(m.pairs, elo0, elo1, bounds) {
                        mv.verdict = verdict.as_str().into();
                    }
                }
            }
            out.push(mv);
        }
        Ok(out)
    })
    .await
}

pub async fn workers(State(st): State<AppState>) -> ApiResult<Json<Vec<WorkerView>>> {
    read_endpoint(&st, |st| -> Result<Vec<WorkerView>, ApiError> {
        let server = &st.server;
        let items = server.ports.workers.list_workers()?;

        let (stats_1h, stats_24h, est_rates) = worker_stats(server);
        let stats_1h = stats_1h.unwrap_or_default();
        let stats_24h = stats_24h.unwrap_or_default();
        let est_rates = est_rates.unwrap_or_default();
        let now = server.clock.now();
        let mut out = Vec::with_capacity(items.len());
        for wk in items {
            let s1h = stats_1h.get(&wk.id).copied().unwrap_or_default();
            let s24h = stats_24h.get(&wk.id).copied().unwrap_or_default();
            let est = est_rates.get(&wk.id).copied().unwrap_or_default();
            let (mut est_per_hour, mut est_per_thread) = (0.0_f64, 0.0_f64);
            if est.span_seconds > 0 {
                let span = est.span_seconds.max(60) as f64;
                est_per_hour = est.games as f64 / (span / 3600.0);
                if wk.threads > 0 {
                    est_per_thread = est_per_hour / wk.threads as f64;
                }
            }
            out.push(WorkerView {
                id: wk.id.clone(),
                ip: wk.ip.clone(),
                note: wk.note.clone(),
                last_seen: wk.last_seen,
                online: bqs_core::is_online(wk.last_seen, now, st.online_window_secs),
                threads: wk.threads,
                completed_games: wk.completed_games,
                client_version: wk.client_version.clone(),
                memory_mb: wk.memory_mb,
                health_status: wk.health_status.clone(),
                last_error: wk.last_error.clone(),
                episodes_1h: s1h.episodes,
                games_1h: s1h.games,
                episodes_24h: s24h.episodes,
                games_24h: s24h.games,
                est_games_per_hour: est_per_hour,
                est_per_thread_hour: est_per_thread,
                est_samples: est.episodes,
            });
        }
        Ok(out)
    })
    .await
}

const MAX_WORKER_NOTE: usize = 64;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerNoteRequest {
    note: String,
}

pub async fn worker_note(
    State(st): State<AppState>,
    Path(id): Path<String>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    write_endpoint(
        &st,
        bytes,
        MAX_CONTROL_BODY,
        move |st, req: WorkerNoteRequest| -> Result<OkView, ApiError> {
            let note = req.note.trim().to_string();

            if note.chars().count() > MAX_WORKER_NOTE {
                return Err(ApiError::bad_request(format!(
                    "备注最长 {MAX_WORKER_NOTE} 字符"
                )));
            }
            let workers = &st.server.ports.workers;

            let exists = workers.list_workers()?.iter().any(|w| w.id == id);
            if !exists {
                return Err(ApiError::not_found(format!("worker not found: {id}")));
            }
            workers.set_worker_note(&id, &note)?;
            tracing::info!("[workers] worker={id} note={note:?}");
            Ok(OkView { ok: true })
        },
    )
    .await
}

pub async fn worker_delete(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<OkView>> {
    read_endpoint(&st, move |st| -> Result<OkView, ApiError> {
        let workers = &st.server.ports.workers;
        let wk = workers
            .list_workers()?
            .into_iter()
            .find(|w| w.id == id)
            .ok_or_else(|| ApiError::not_found(format!("worker not found: {id}")))?;
        if bqs_core::is_online(wk.last_seen, st.server.clock.now(), st.online_window_secs) {
            return Err(ApiError::bad_request(format!("worker {id} 在线，禁止删除")));
        }
        let deleted = workers.delete_worker(&id)?;
        if deleted {
            tracing::info!("[workers] 删除离线 worker={id} last_seen={}", wk.last_seen);
        }
        Ok(OkView { ok: true })
    })
    .await
}

fn to_task_view(t: bqs_core::dispatch::RunningTask) -> TaskView {
    TaskView {
        task_id: t.task_id,
        worker_id: t.worker_id,
        kind: t.kind.as_str_name().to_string(),
        match_id: t.match_id,
        network_sha: t.network_sha,
        opponent_sha: t.opponent_sha,
        opponent_spec: t.opponent_spec,
        games: t.games as i32,
        created_at: t.assigned_at,
        job_id: t.job_id,
    }
}

pub async fn tasks(State(st): State<AppState>) -> ApiResult<Json<Vec<TaskView>>> {
    read_endpoint(&st, |st| {
        Ok(st
            .server
            .disp
            .running_tasks_webui()
            .into_iter()
            .map(to_task_view)
            .collect())
    })
    .await
}

pub async fn task_stats(State(st): State<AppState>) -> ApiResult<Json<TaskStateStats>> {
    read_endpoint(&st, |st| Ok(st.server.disp.task_state_snapshot())).await
}
