use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Bytes;
use axum::response::{IntoResponse, Response};
use axum::http::StatusCode;
use axum::Json;
use serde::de::DeserializeOwned;

use bqs_core::server::Server;

pub const MAX_BODY_LIMIT: usize = 16 * 1024;
pub const MAX_CONTROL_BODY: usize = 4096;
pub const MAX_TRAIN_CONFIG_BODY: usize = 16 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub server: Arc<Server>,
    pub storage: Arc<bqs_storage::Manager>,

    pub online_window_secs: i64,

    pub webui_dist: Option<Arc<PathBuf>>,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: msg.into(),
        }
    }
    pub fn bad_gateway(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: msg.into(),
        }
    }
}

impl From<bqs_core::CoreError> for ApiError {
    fn from(e: bqs_core::CoreError) -> Self {
        ApiError::internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error": self.message });
        (self.status, Json(body)).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

pub fn decode_body<T: DeserializeOwned>(bytes: &Bytes, max: usize) -> Result<T, ApiError> {
    if bytes.len() > max {
        return Err(ApiError::bad_request("http: request body too large"));
    }
    serde_json::from_slice(bytes)
        .map_err(|e| ApiError::bad_request(format!("invalid json body: {e}")))
}

pub async fn read_endpoint<T, F>(st: &AppState, f: F) -> ApiResult<Json<T>>
where
    F: FnOnce(&AppState) -> Result<T, ApiError> + Send + 'static,
    T: Send + 'static,
{
    let st = st.clone();
    let out = tokio::task::spawn_blocking(move || f(&st))
        .await
        .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(out))
}

pub async fn write_endpoint<T, B, F>(
    st: &AppState,
    bytes: Bytes,
    max: usize,
    f: F,
) -> ApiResult<Json<T>>
where
    B: DeserializeOwned + Send + 'static,
    F: FnOnce(&AppState, B) -> Result<T, ApiError> + Send + 'static,
    T: Send + 'static,
{
    let body: B = decode_body(&bytes, max)?;
    read_endpoint(st, move |st| f(st, body)).await
}

pub fn int_query(query: Option<&str>, key: &str) -> i64 {
    let Some(q) = query else { return 0 };
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == key {
            return v.parse().unwrap_or(0);
        }
    }
    0
}

pub fn to_profile_view(p: &bqs_storage::StorageProfile) -> crate::views::StorageProfileView {
    crate::views::StorageProfileView {
        name: p.name.clone(),
        endpoint: p.endpoint.clone(),
        region: p.region.clone(),
        access_key: mask_key(&p.access_key),
        has_secret: !p.secret_key.is_empty(),
        bucket: p.bucket.clone(),
        path_style: p.path_style,
    }
}

pub fn mask_key(k: &str) -> String {
    if k.is_empty() {
        String::new()
    } else if k.len() <= 8 {
        "****".into()
    } else {
        format!("{}****{}", &k[..4], &k[k.len() - 4..])
    }
}

pub fn valid_profile_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}
