use axum::Json;
use axum::extract::State;
use serde::Deserialize;

use bqs_core::evalcoord::EvalOverride;

use crate::views::{
    EvalConfigGetView, EvalEffectiveView, EvalResultView, EvalTriggerView, EvalView, OkView,
};
use crate::{ApiError, ApiResult, AppState, MAX_CONTROL_BODY, read_endpoint, write_endpoint};

const EVAL_LATEST_LIMIT: i64 = 200;
const EVAL_TREND_LIMIT: i64 = 50;

fn to_eval_result_view(e: &bqs_core::types::EvalResult) -> EvalResultView {
    EvalResultView {
        network_sha: e.network_sha.clone(),
        opponent_spec: e.opponent_spec.clone(),
        wins: e.wins,
        draws: e.draws,
        losses: e.losses,
        red_wins: e.red_wins,
        black_wins: e.black_wins,
        draw_no_capture: e.draw_no_capture,
        draw_truncation: e.draw_truncation,
        num_games: e.num_games,
        win_rate: e.win_rate(),
        avg_moves: e.avg_moves,
        created_at: e.created_at,
    }
}

pub async fn eval(State(st): State<AppState>) -> ApiResult<Json<EvalView>> {
    read_endpoint(&st, |st| -> Result<EvalView, ApiError> {
        let server = &st.server;
        let latest: Vec<EvalResultView> = server
            .ports
            .eval
            .list_eval_results(EVAL_LATEST_LIMIT)?
            .iter()
            .map(to_eval_result_view)
            .collect();

        let opponents = server.ports.eval.list_eval_opponents()?;
        let mut trends = Vec::with_capacity(opponents.len());
        for opp in opponents {
            let points = server
                .ports
                .eval
                .eval_trend(&opp, EVAL_TREND_LIMIT)?
                .iter()
                .map(to_eval_result_view)
                .collect();
            let mut tv = crate::views::EvalTrendView {
                opponent: opp.clone(),
                versions: 0,
                no_progress: 0,
                latest_win_rate: 0.0,
                points,
            };

            if let Some(prog) = server.evalc.eval_progress_for(&opp, 0) {
                tv.versions = prog.versions as i64;
                tv.no_progress = prog.no_progress as i64;
                tv.latest_win_rate = prog.latest_win_rate;
            }
            trends.push(tv);
        }

        let running = server
            .evalc
            .running_jobs()
            .into_iter()
            .map(|j| crate::views::EvalRunningView {
                network_sha: j.network_sha,
                spec: j.opponent_spec,
                num_games: j.num_games,
                target_games: j.target_games,
                in_flight: j.in_flight,
            })
            .collect();

        let (should_stop, stop_reason) = server.ctl.should_stop();
        Ok(EvalView {
            config: crate::handlers_status::to_eval_config_view(st),
            should_stop,
            stop_reason,
            latest,
            trends,
            running,
        })
    })
    .await
}

pub async fn eval_config_get(State(st): State<AppState>) -> ApiResult<Json<EvalConfigGetView>> {
    let server = st.server.clone();
    let ov = server.ctl.eval_override();

    let cfg = &server.cfg.eval;
    let mut eff = EvalEffectiveView {
        enabled: cfg.enabled,
        opponents: cfg.opponents.clone(),
        stop_win_rate: cfg.stop_win_rate,
    };
    crate::handlers_status::apply_eval_override(
        ov.as_ref(),
        &mut eff.enabled,
        &mut eff.opponents,
        &mut eff.stop_win_rate,
    );
    Ok(Json(EvalConfigGetView {
        override_: ov,
        effective: eff,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvalConfigRequest {
    clear: Option<bool>,
    enabled: Option<bool>,
    opponents: Option<Vec<String>>,
    stop_win_rate: Option<f64>,
}

pub async fn eval_config_put(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    write_endpoint(
        &st,
        bytes,
        MAX_CONTROL_BODY,
        |st, req: EvalConfigRequest| -> Result<OkView, ApiError> {
            let ctl = &st.server.ctl;
            if req.clear == Some(true) {
                ctl.set_eval_override(EvalOverride::default())
                    .map_err(ApiError::from)?;
                tracing::info!("[webui] eval_config 已清除覆盖（回落配置文件）");
                return Ok(OkView { ok: true });
            }

            let ov = EvalOverride {
                enabled: req.enabled,
                opponents: req.opponents.clone(),
                stop_win_rate: req.stop_win_rate,
                ..Default::default()
            };
            if let Some(opps) = &ov.opponents {
                bqs_core::evalopponents::validate_eval_opponents(opps)
                    .map_err(ApiError::bad_request)?;
            }
            ctl.set_eval_override(ov)
                .map_err(|e| ApiError::bad_request(e.to_string()))?;
            tracing::info!(
                "[webui] eval_config 已更新: enabled={:?} opponents={:?} stopWinRate={:?}",
                req.enabled,
                req.opponents,
                req.stop_win_rate
            );
            Ok(OkView { ok: true })
        },
    )
    .await
}

pub async fn trigger(State(st): State<AppState>) -> ApiResult<Json<EvalTriggerView>> {
    let server = st.server.clone();
    let n = tokio::task::spawn_blocking(move || server.evalc.trigger_manual())
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    let n = n.map_err(ApiError::conflict)?;
    let msg = if n == 0 {
        "当前对手阶梯均已在队 / 在飞 / 已有结果，无新增评估".to_string()
    } else {
        format!("已入队 {n} 条评估")
    };
    tracing::info!("[webui] 手动触发评估：{msg}");
    Ok(Json(EvalTriggerView {
        ok: true,
        triggered: n,
        message: msg,
    }))
}
