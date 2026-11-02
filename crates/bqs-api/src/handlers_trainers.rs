use axum::Json;
use axum::extract::State;
use serde::Deserialize;

use crate::views::{OkView, TrainConfigView};
use crate::{ApiResult, AppState, MAX_TRAIN_CONFIG_BODY, decode_body};

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
        .map_err(|e| crate::ApiError::bad_request(e.to_string()))?;
    tracing::info!("[webui] train_config 已更新 {n} 项");
    Ok(Json(OkView { ok: true }))
}
