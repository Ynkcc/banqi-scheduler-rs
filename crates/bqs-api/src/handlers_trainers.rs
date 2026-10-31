use axum::Json;
use axum::extract::State;
use serde::Deserialize;

use crate::views::{
    OkView, TrainConfigView, TrainerView, TrainersView, balance_mode_name, data_kind_name,
};
use crate::{ApiError, ApiResult, AppState, MAX_TRAIN_CONFIG_BODY, decode_body, read_endpoint};

pub async fn trainers(State(st): State<AppState>) -> ApiResult<Json<TrainersView>> {
    read_endpoint(&st, |st| -> Result<TrainersView, ApiError> {
        let server = &st.server;
        let cfg = &server.cfg.balance;

        let online_window = if cfg.trainer_online_seconds > 0 {
            cfg.trainer_online_seconds
        } else {
            st.online_window_secs
        };
        let items = server.ports.trainers.list_trainers()?;
        let now = server.clock.now();
        let mut out = Vec::with_capacity(items.len());
        for t in items {
            let (registered_max_id, unconsumed_batches) = server
                .ports
                .trainers
                .kind_watermark(t.kind, t.consumed_id)
                .unwrap_or((0, 0));
            out.push(TrainerView {
                id: t.id.clone(),
                last_seen: t.last_seen,
                online: bqs_core::is_online(t.last_seen, now, online_window),
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
            },
            auto_paused: server.ctl.auto_paused(),
            auto_pause_reason: server.ctl.auto_pause_reason(),
            trainers: out,
        })
    })
    .await
}

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
