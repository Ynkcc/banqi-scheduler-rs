use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::Json;

use bqs_core::dispatch::TaskStateStats;
use bqs_core::types::Network;
use bqs_judgment::sprt::{judge, llr, new_bounds};

use crate::views::{
    EpisodeRateEstimateView, EvalConfigView, KindStatsView, MatchView, NetworkView, OkView,
    SprtView, StatusView, TaskView, WorkerView,
};
use crate::{decode_body, ApiError, ApiResult, AppState, MAX_CONTROL_BODY};

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

pub fn to_eval_config_view(st: &AppState) -> EvalConfigView {
    let cfg = &st.server.cfg.eval;
    let (mut enabled, mut opponents, mut stop_win_rate) =
        (cfg.enabled, cfg.opponents.clone(), cfg.stop_win_rate);
    if let Some(o) = st.server.ctl.eval_override() {
        if let Some(v) = o.enabled {
            enabled = v;
        }
        if let Some(v) = o.opponents {
            opponents = v;
        }
        if let Some(v) = o.stop_win_rate {
            stop_win_rate = v;
        }
    }
    EvalConfigView {
        enabled,
        opponents,
        games: cfg.games,
        task_games: cfg.games_per_task,
        mcts_sims: cfg.mcts_sims,
        every_n_promotions: cfg.every_n_promotions,
        no_progress_n: cfg.no_progress_n,
        no_progress_eps: cfg.no_progress_eps,
        rollback_eps: cfg.rollback_eps,
        floor_win_rate: cfg.floor_win_rate,
        stop_win_rate,
        mode: crate::views::eval_search_mode_name(cfg.mcts_sims),
        pending: st.server.evalc.pending_eval() as i64,
    }
}

pub async fn status(State(st): State<AppState>) -> ApiResult<Json<StatusView>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<StatusView, ApiError> {
        let best = server.ports.networks.get_best()?;
        let counts: crate::views::CountsView = server.ports.eval.counts()?.into();
        let ctl = &server.ctl;
        let (should_stop, stop_reason) = ctl.should_stop();
        Ok(StatusView {
            variant: server.cfg.selfplay.variant.clone(),

            paused: ctl.manual_paused(),
            initial_revealed: ctl.initial_revealed(),
            data_kind: ctl.data_kind().as_str().to_string(),
            min_client_version: server.cfg.server.min_client_version.clone(),
            sprt: SprtView {
                elo0: server.cfg.sprt.elo0,
                elo1: server.cfg.sprt.elo1,
                alpha: server.cfg.sprt.alpha,
                beta: server.cfg.sprt.beta,
            },
            best: best.as_ref().map(to_network_view),
            counts,
            eval: to_eval_config_view(&st),
            should_stop,
            stop_reason,
            auto_paused: ctl.auto_paused(),
            auto_pause_reason: ctl.auto_pause_reason(),
        })
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(view))
}

pub async fn networks(State(st): State<AppState>) -> ApiResult<Json<Vec<NetworkView>>> {
    let server = st.server.clone();
    let items = tokio::task::spawn_blocking(move || server.ports.networks.list_networks())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(items.iter().map(to_network_view).collect()))
}

pub async fn promote(
    State(st): State<AppState>,
    Path(sha): Path<String>,
) -> ApiResult<Json<OkView>> {
    let server = st.server.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        let n = server
            .ports
            .networks
            .get_network(&sha)?
            .ok_or_else(|| ApiError::not_found(format!("network not found: {sha}")))?;
        if n.is_best {
            return Err(ApiError::conflict(format!("already best: {sha}")));
        }
        server.ports.networks.promote_best(&sha)?;
        tracing::info!("[webui] manual promote sha={sha} status={}", n.status);
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(OkView { ok: true }))
}

pub async fn matches(State(st): State<AppState>) -> ApiResult<Json<Vec<MatchView>>> {
    let server = st.server.clone();
    let items = tokio::task::spawn_blocking(move || -> Result<Vec<MatchView>, ApiError> {
        let sprt = &server.cfg.sprt;
        let bounds = new_bounds(sprt.alpha, sprt.beta);
        let items = server.ports.matches.list_matches()?;
        let mut out = Vec::with_capacity(items.len());
        for m in items {
            let mut mv = MatchView {
                id: m.id,
                candidate: m.candidate.clone(),
                opponent: m.opponent.clone(),
                status: m.status.clone(),
                pairs: m.pairs,
                num_games: m.num_games,
                target_games: m.target_games,
                score: None,
                llr: 0.0,
                lower: bounds.lower,
                upper: bounds.upper,
                verdict: bqs_judgment::Verdict::Continue.as_str().into(),
            };
            if m.pairs.total() > 0 {
                match llr(m.pairs, sprt.elo0, sprt.elo1) {
                    Ok(l) => {
                        mv.llr = l;
                        if let Ok((score, _)) = m.pairs.stats() {
                            mv.score = Some(score);
                        }
                    }
                    Err(e) => tracing::warn!("[webui] llr match={}：{e}", m.id),
                }
                if let Ok((verdict, _)) = judge(m.pairs, sprt.elo0, sprt.elo1, bounds) {
                    mv.verdict = verdict.as_str().into();
                }
            }
            out.push(mv);
        }
        Ok(out)
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(items))
}

pub async fn workers(State(st): State<AppState>) -> ApiResult<Json<Vec<WorkerView>>> {
    let server = st.server.clone();
    let online_window = st.online_window_secs;
    let items = tokio::task::spawn_blocking(move || -> Result<Vec<WorkerView>, ApiError> {
        let items = server.ports.workers.list_workers()?;

        let (stats_1h, stats_24h, est_rates) = worker_stats(&server);
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
                note: wk.note.clone(),
                last_seen: wk.last_seen,
                online: now - wk.last_seen < online_window,
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
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(items))
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
    let req: WorkerNoteRequest = decode_body(&bytes, MAX_CONTROL_BODY)?;
    let note = req.note.trim().to_string();

    if note.chars().count() > MAX_WORKER_NOTE {
        return Err(ApiError::bad_request(format!(
            "备注最长 {MAX_WORKER_NOTE} 字符"
        )));
    }
    let server = st.server.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        let exists = server
            .ports
            .workers
            .list_workers()?
            .iter()
            .any(|w| w.id == id);
        if !exists {
            return Err(ApiError::not_found(format!("worker not found: {id}")));
        }
        server.ports.workers.set_worker_note(&id, &note)?;
        tracing::info!("[workers] worker={id} note={note:?}");
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(OkView { ok: true }))
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
    }
}

pub async fn tasks(State(st): State<AppState>) -> ApiResult<Json<Vec<TaskView>>> {
    let server = st.server.clone();
    let items = tokio::task::spawn_blocking(move || server.disp.running_tasks_webui())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    Ok(Json(items.into_iter().map(to_task_view).collect()))
}

pub async fn task_stats(State(st): State<AppState>) -> ApiResult<Json<TaskStateStats>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || server.disp.task_state_snapshot())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    Ok(Json(view))
}
