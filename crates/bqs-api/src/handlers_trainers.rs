//! handlers_trainers.rs — trainer 消费健康端点（/api/trainers）与可远程调整训练
//! 超参端点（/api/train-config）。（Go: internal/api/handlers_trainers.go）

use axum::extract::State;
use axum::Json;
use serde::Deserialize;

use crate::views::{
    balance_mode_name, data_kind_name, OkView, TrainConfigView, TrainerView, TrainersView,
};
use crate::{decode_body, ApiError, ApiResult, AppState, MAX_TRAIN_CONFIG_BODY};

/// GET /api/trainers — trainer 消费健康总览（平衡策略配置 + 自动暂停状态 +
/// 各 trainer 快照，含服务端权威积压）。
pub async fn trainers(State(st): State<AppState>) -> ApiResult<Json<TrainersView>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<TrainersView, ApiError> {
        let cfg = &server.cfg.balance;
        // 在线判定窗口：balance.trainer_online_seconds 优先，<=0 回落 worker 窗口
        let online_window = if cfg.trainer_online_seconds > 0 {
            cfg.trainer_online_seconds
        } else {
            st.online_window_secs
        };
        let items = server.ports.trainers.list_trainers()?;
        let now = server.clock.now();
        let mut out = Vec::with_capacity(items.len());
        for t in items {
            // 服务端权威积压（查询失败降级为 0：展示是辅助信息，不拖垮整页）
            let (registered_max_id, unconsumed_batches) =
                server.ports.trainers.kind_watermark(t.kind, t.consumed_id).unwrap_or((0, 0));
            out.push(TrainerView {
                id: t.id.clone(),
                last_seen: t.last_seen,
                online: now - t.last_seen <= online_window,
                variant: t.variant.clone(),
                data_kind: data_kind_name(t.kind).to_string(),
                consumed_key: t.consumed_key.clone(),
                consumed_id: t.consumed_id,
                registered_max_id,
                unconsumed_batches,
                samples_consumed: t.samples_consumed,
                backlog: t.backlog,
                buffered: t.buffered,
                health_status: t.health_status.clone(),
                last_error: t.last_error.clone(),
                progress: t.progress.clone(),
            });
        }
        Ok(TrainersView {
            config: crate::views::BalanceConfigView {
                enabled: cfg.enabled,
                mode: balance_mode_name(cfg.mode).to_string(),
                trainer_online_seconds: cfg.trainer_online_seconds,
                stalled_seconds: cfg.stalled_seconds,
                backlog_pause_batches: cfg.backlog_pause_batches,
                pause_reanalysis: cfg.pause_reanalysis,
            },
            auto_paused: server.ctl.auto_paused(),
            auto_pause_reason: server.ctl.auto_pause_reason(),
            trainers: out,
        })
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(view))
}

/// GET /api/train-config — 当前覆盖项 + 可调字段清单（前端动态渲染表单）。
pub async fn train_config_get(State(st): State<AppState>) -> ApiResult<Json<TrainConfigView>> {
    let overrides = st.server.ctl.train_config_overrides();
    let fields = bqs_core::trainconfig::TrainConfig::fields()
        .into_iter()
        .map(|f| crate::views::TrainConfigFieldView {
            name: f.name,
            kind: f.kind,
            min: f.min,
            max: f.max,
            has_max: f.has_max,
            gt_zero: f.gt_zero,
            r#enum: f.enum_values,
        })
        .collect();
    Ok(Json(TrainConfigView { overrides, fields }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrainConfigRequest {
    overrides: std::collections::BTreeMap<String, String>,
}

/// PUT /api/train-config — **全量替换**训练配置覆盖项；字段名与取值由白名单校验
/// （Control 侧权威裁定），非法输入直接 400 回显原因（不落库半套配置）。
pub async fn train_config_put(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    let req: TrainConfigRequest = decode_body(&bytes, MAX_TRAIN_CONFIG_BODY)?;
    let n = req.overrides.len();
    st.server
        .ctl
        .set_train_config_overrides(req.overrides)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    tracing::info!("[webui] train_config 已更新 {n} 项");
    Ok(Json(OkView { ok: true }))
}
