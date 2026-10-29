//! handlers_control.rs — 运行时控制端点（/api/control）。
//! （Go: internal/api/handlers.go handleControl）

use axum::extract::State;
use axum::Json;
use serde::Deserialize;

use crate::views::OkView;
use crate::{decode_body, ApiError, ApiResult, AppState, MAX_CONTROL_BODY};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlRequest {
    paused: Option<bool>,
    initial_revealed: Option<i32>,
    /// resnet / nnue（onnx 为 resnet 旧别名）
    data_kind: Option<String>,
    /// 清除绝对强度判据置位的停机信号（确认续训时使用）
    clear_stop: Option<bool>,
}

/// POST /api/control — 逐字段应用控制项（{"paused":bool}|{"initialRevealed":int}|
/// {"dataKind":"resnet"|"nnue"}|{"clearStop":true}），一次请求可叠加多项。
pub async fn control(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    let req: ControlRequest = decode_body(&bytes, MAX_CONTROL_BODY)?;
    let ctl = &st.server.ctl;
    if let Some(paused) = req.paused {
        ctl.set_manual_paused(paused)
            .map_err(|e| ApiError::internal(e.to_string()))?;
        tracing::info!("[webui] pause_self_play={paused}");
    }
    if let Some(n) = req.initial_revealed {
        // Go: SetInitialRevealed 校验失败 → 400（负值拒绝）
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
        tracing::info!("[webui] eval should_stop 已清除（确认续训）");
    }
    Ok(Json(OkView { ok: true }))
}

/// 解析数据类别标识（Go: scheduler.ParseDataKind；"onnx" 是 resnet 旧别名；
/// 未知取值报错，不静默回退）。
fn parse_data_kind(s: &str) -> Result<bqs_config::DataKind, ApiError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "resnet" | "onnx" => Ok(bqs_config::DataKind::Resnet),
        "nnue" => Ok(bqs_config::DataKind::Nnue),
        _ => Err(ApiError::bad_request(format!(
            "未知数据类别 {s:?}（可选 resnet / nnue，对应 mcts_resnet / expectimax_nnue 栈；none 为 eval-only 对局记录，不可配置）"
        ))),
    }
}
