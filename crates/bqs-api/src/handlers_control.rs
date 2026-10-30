use axum::Json;
use axum::extract::State;
use serde::Deserialize;

use crate::views::OkView;
use crate::{ApiError, ApiResult, AppState, MAX_CONTROL_BODY, write_endpoint};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlRequest {
    paused: Option<bool>,
    initial_revealed: Option<i32>,

    data_kind: Option<String>,

    clear_stop: Option<bool>,
}

pub async fn control(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    write_endpoint(
        &st,
        bytes,
        MAX_CONTROL_BODY,
        |st, req: ControlRequest| -> Result<OkView, ApiError> {
            let ctl = &st.server.ctl;
            if let Some(paused) = req.paused {
                ctl.set_manual_paused(paused)
                    .map_err(|e| ApiError::internal(e.to_string()))?;
                tracing::info!("[webui] pause_self_play={paused}");
            }
            if let Some(n) = req.initial_revealed {
                ctl.set_initial_revealed(n)
                    .map_err(|e| ApiError::bad_request(e.to_string()))?;
                tracing::info!("[webui] initial_revealed={n}");
            }
            if let Some(kind) = req.data_kind {
                let kind = parse_data_kind(&kind)?;
                ctl.set_data_kind(kind)
                    .map_err(|e| ApiError::internal(e.to_string()))?;
                tracing::info!("[webui] data_kind={}", kind.as_str());
            }
            if req.clear_stop == Some(true) {
                ctl.clear_stop()
                    .map_err(|e| ApiError::internal(e.to_string()))?;
                tracing::info!("[webui] should_stop 已清除（确认续训）");
            }
            Ok(OkView { ok: true })
        },
    )
    .await
}

fn parse_data_kind(s: &str) -> Result<bqs_config::DataKind, ApiError> {
    bqs_core::data_kind::parse(s).map_err(|e| ApiError::bad_request(format!("{e}")))
}
