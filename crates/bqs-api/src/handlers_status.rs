//! handlers_status.rs — 总览与列表端点：/api/status、/api/networks、
//! /api/networks/{sha}/promote、/api/matches、/api/workers、/api/workers/{id}/note、
//! /api/tasks、/api/task-stats。（Go: internal/api/handlers_status.go）

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::Json;

use bqs_core::dispatch::TaskStateStats;
use bqs_core::types::Network;
use bqs_judgment::sprt::{judge, llr, new_bounds};

use crate::views::{
    EpisodeRateEstimateView, EvalConfigView, KindStatsView, MatchView, NetworkView,
    OkView, SprtView, StatusView, TaskView, WorkerView,
};
use crate::{decode_body, ApiError, ApiResult, AppState, MAX_CONTROL_BODY};

/// store 行 → 展示视图（status.best / networks 共用；Go: toNetworkView）。
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

/// worker 产出统计三查询（Go: EpisodeStatsByWorker ×2 + EpisodeRateEstimates）。
/// 查询失败返回 None，调用方降级为空映射（Go 版语义：辅助信息不拖垮整页）。
fn worker_stats(
    server: &bqs_core::server::Server,
) -> (
    Option<BTreeMap<String, KindStatsView>>,
    Option<BTreeMap<String, KindStatsView>>,
    Option<BTreeMap<String, EpisodeRateEstimateView>>,
) {
    let to_views = |m: BTreeMap<String, bqs_core::ports::KindStats>| {
        m.into_iter().map(|(k, v)| (k, v.into())).collect::<BTreeMap<_, _>>()
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
                c.ok().map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect()),
            )
        }
    }
}

/// 评估配置视图组装（Go: toEvalConfigView：在线覆盖优先生效，mode 取配置文件值）。
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

/// GET /api/status — 聚合总览（variant / 课程阶段 / paused / autoPaused /
/// autoPauseReason / sprt 参数 / best / 计数 / eval 配置 / shouldStop / stopReason）。
pub async fn status(State(st): State<AppState>) -> ApiResult<Json<StatusView>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<StatusView, ApiError> {
        let best = server.ports.networks.get_best()?;
        let counts: crate::views::CountsView = server.ports.eval.counts()?.into();
        let ctl = &server.ctl;
        let (should_stop, stop_reason) = ctl.should_stop();
        Ok(StatusView {
            variant: server.cfg.selfplay.variant.clone(),
            // Go: rt.Paused 只含人工开关；自动暂停单独走 autoPaused。
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

/// GET /api/networks — 全部网络列表（best 优先，bqs-store 查询保证）。
pub async fn networks(State(st): State<AppState>) -> ApiResult<Json<Vec<NetworkView>>> {
    let server = st.server.clone();
    let items = tokio::task::spawn_blocking(move || server.ports.networks.list_networks())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(items.iter().map(to_network_view).collect()))
}

/// POST /api/networks/{sha}/promote — 手动晋级 best（409 = 已是 best，404 = 未知 sha）。
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

/// GET /api/matches — 对打列表，附 LLR / 边界 / 判定 / 得分率（bqs_judgment 计算）。
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

/// GET /api/workers — worker 列表：在线判定（lastSeen < now-window）、健康状态、
/// 窗口产出与稳态速率估算（估算查询失败降级为 0，不拖垮整页）。
pub async fn workers(State(st): State<AppState>) -> ApiResult<Json<Vec<WorkerView>>> {
    let server = st.server.clone();
    let online_window = st.online_window_secs;
    let items = tokio::task::spawn_blocking(move || -> Result<Vec<WorkerView>, ApiError> {
        let items = server.ports.workers.list_workers()?;
        // 产出效率：聚合失败降级为 0（Go 版语义：辅助信息不拖垮整页）
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
                // 跨度过短（<60s）时截断，避免快速连发批把估算推到失真量级
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

/// 备注长度上限（字符数，rune 口径；Go: maxWorkerNote）。
const MAX_WORKER_NOTE: usize = 64;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerNoteRequest {
    note: String,
}

/// PUT /api/workers/{id}/note — 设置 worker 备注（TrimSpace；空串 = 清除；
/// 未知 worker → 404；超长 → 400）。
pub async fn worker_note(
    State(st): State<AppState>,
    Path(id): Path<String>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    let req: WorkerNoteRequest = decode_body(&bytes, MAX_CONTROL_BODY)?;
    let note = req.note.trim().to_string();
    // 长度按字符数（rune）计，与 Go len([]rune(note)) 一致
    if note.chars().count() > MAX_WORKER_NOTE {
        return Err(ApiError::bad_request(format!("备注最长 {MAX_WORKER_NOTE} 字符")));
    }
    let server = st.server.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        // worker 须已存在（至少上报过一次心跳），否则 404（Go: sql.ErrNoRows → 404）
        let exists = server.ports.workers.list_workers()?.iter().any(|w| w.id == id);
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

/// RunningTask → taskView 投影（Go: toTaskView；kind 用 proto 名，与 pb String() 同口径）。
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

/// GET /api/tasks — 内存任务快照（未结束任务，assigned_at 倒序）。
pub async fn tasks(State(st): State<AppState>) -> ApiResult<Json<Vec<TaskView>>> {
    let server = st.server.clone();
    let items = tokio::task::spawn_blocking(move || server.disp.running_tasks_webui())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    Ok(Json(items.into_iter().map(to_task_view).collect()))
}

/// GET /api/task-stats — 任务状态机快照（assigned/completed/expired 计数、
/// 最近过期任务与最老在途任务年龄；serde 形态由 core 类型锁定）。
pub async fn task_stats(State(st): State<AppState>) -> ApiResult<Json<TaskStateStats>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || server.disp.task_state_snapshot())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    Ok(Json(view))
}
