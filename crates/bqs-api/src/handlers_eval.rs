//! handlers_eval.rs — 绝对强度评估端点：/api/eval 趋势视图、在线覆盖
//! （/api/eval-config GET/PUT）、手动触发（/api/eval/trigger）。
//! （Go: internal/api/handlers_eval.go）

use axum::extract::State;
use axum::Json;
use serde::Deserialize;

use bqs_core::evalcoord::EvalOverride;

use crate::views::{
    EvalConfigGetView, EvalEffectiveView, EvalResultView, EvalTriggerView, EvalView, OkView,
};
use crate::{decode_body, ApiError, ApiResult, AppState, MAX_CONTROL_BODY};

// EvalCoordinator 查询经 `srv.evalc.*` 就地调用（薄接入，签名见 bqs-core/src/evalcoord.rs）。

// 绝对强度评估：明细与趋势各取多少条（趋势用于画版本曲线，无需全量）
const EVAL_LATEST_LIMIT: i64 = 200;
const EVAL_TREND_LIMIT: i64 = 50;

/// store 行 → 展示视图（Go: toEvalResultView）。
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

/// GET /api/eval — 趋势视图：按对手分组的版本序列（升序）+ 最新明细 +
/// 「连续无提升」计数 + 评估配置 + 进行中作业。
pub async fn eval(State(st): State<AppState>) -> ApiResult<Json<EvalView>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<EvalView, ApiError> {
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
            // 趋势点：按对手的版本序列（升序）
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
            // Go 侧 window 恒传 0：末尾连续无提升计数按全序列口径
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
            config: crate::handlers_status::to_eval_config_view(&st),
            should_stop,
            stop_reason,
            latest,
            trends,
            running,
        })
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(view))
}

/// GET /api/eval-config — 在线覆盖与生效值快照（null 字段 = 回落配置文件）。
pub async fn eval_config_get(State(st): State<AppState>) -> ApiResult<Json<EvalConfigGetView>> {
    let server = st.server.clone();
    let ov = server.ctl.eval_override();
    // 生效值：配置文件值被在线覆盖逐字段替换（Go: handleEvalConfigGet）
    let cfg = &server.cfg.eval;
    let mut eff = EvalEffectiveView {
        enabled: cfg.enabled,
        opponents: cfg.opponents.clone(),
        stop_win_rate: cfg.stop_win_rate,
    };
    if let Some(o) = &ov {
        if let Some(v) = o.enabled {
            eff.enabled = v;
        }
        if let Some(v) = &o.opponents {
            eff.opponents = v.clone();
        }
        if let Some(v) = o.stop_win_rate {
            eff.stop_win_rate = v;
        }
    }
    Ok(Json(EvalConfigGetView { override_: ov, effective: eff }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvalConfigRequest {
    clear: Option<bool>,
    enabled: Option<bool>,
    opponents: Option<Vec<String>>,
    stop_win_rate: Option<f64>,
}

/// PUT /api/eval-config — 更新在线覆盖（启用开关 / 对手阶梯 / 达标停机阈值）。
/// null 字段 = 回落配置文件值；opponents = [] 清空阶梯；clear = 全清。
pub async fn eval_config_put(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    let req: EvalConfigRequest = decode_body(&bytes, MAX_CONTROL_BODY)?;
    let server = st.server.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        if req.clear == Some(true) {
            // 全清覆盖（回落配置文件）：全 None 的覆盖项即「无覆盖」载体
            server
                .ctl
                .set_eval_override(EvalOverride::default())
                .map_err(ApiError::from)?;
            tracing::info!("[webui] eval_config 已清除覆盖（回落配置文件）");
            return Ok(());
        }
        // 显式 null 与缺省字段语义相同（该项回落配置文件值）
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
        server
            .ctl
            .set_eval_override(ov)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        tracing::info!(
            "[webui] eval_config 已更新: enabled={:?} opponents={:?} stopWinRate={:?}",
            req.enabled,
            req.opponents,
            req.stop_win_rate
        );
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(OkView { ok: true }))
}

/// POST /api/eval/trigger — 手动触发评估当前 best（绕过每 N 次晋级节流）；
/// 前置不满足（评估未启用 / 无 best 网络）→ 409 回显原因（trigger_manual 的 Err(String)）。
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
    Ok(Json(EvalTriggerView { ok: true, triggered: n, message: msg }))
}
