use std::path::Path;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post, put};
use axum::Router;

use crate::{
    handlers_control, handlers_episodes, handlers_jobs, handlers_replay, handlers_status,
    handlers_storage, handlers_trainers,
    support::{ApiError, AppState, MAX_BODY_LIMIT},
};

pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/api/status", get(handlers_status::status))
        .route("/api/networks", get(handlers_status::networks))
        .route(
            "/api/networks/{sha}/promote",
            post(handlers_status::promote),
        )
        .route("/api/matches", get(handlers_status::matches))
        .route("/api/workers", get(handlers_status::workers))
        .route("/api/workers/{id}/note", put(handlers_status::worker_note))
        .route("/api/trainers", get(handlers_trainers::trainers))
        .route("/api/episodes", get(handlers_episodes::episodes))
        .route(
            "/api/episodes/{id}/replay",
            get(handlers_replay::episode_replay_list),
        )
        .route(
            "/api/episodes/{id}/replay/{index}",
            get(handlers_replay::episode_replay_game),
        )
        .route("/api/data-stats", get(handlers_episodes::data_stats))
        .route("/api/tasks", get(handlers_status::tasks))
        .route("/api/task-stats", get(handlers_status::task_stats))
        .route(
            "/api/train-config",
            get(handlers_trainers::train_config_get).put(handlers_trainers::train_config_put),
        )
        .route("/api/storage", get(handlers_storage::storage_get))
        .route(
            "/api/storage/active",
            get(handlers_storage::storage_active_get),
        )
        .route(
            "/api/storage/profiles/{name}",
            get(handlers_storage::profile_get)
                .put(handlers_storage::profile_put)
                .delete(handlers_storage::profile_delete),
        )
        .route("/api/storage/activate", post(handlers_storage::activate))
        .route(
            "/api/jobs",
            get(handlers_jobs::list_jobs).post(handlers_jobs::submit_job),
        )
        .route("/api/jobs/{id}/{action}", post(handlers_jobs::control_job))
        .route("/api/control", post(handlers_control::control))
        .layer(DefaultBodyLimit::max(MAX_BODY_LIMIT))
        .with_state(state.clone());

    Router::new()
        .merge(api)
        .fallback(any(static_handler))
        .with_state(state)
}

fn content_type(name: &str) -> &'static str {
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

async fn static_handler(State(st): State<AppState>, req: Request<axum::body::Body>) -> Response {
    if req.method() != Method::GET && req.method() != Method::HEAD {
        return ApiError::bad_request("method not allowed").into_response();
    }
    let path = req.uri().path();
    if path.starts_with("/api/") {
        return ApiError::not_found(format!("unknown api path: {path}")).into_response();
    }
    let rel = path.trim_start_matches('/');

    let safe = !rel.split('/').any(|seg| seg == "..");
    if safe {
        let name = if rel.is_empty() { "index.html" } else { rel };
        if let Some(dist) = &st.webui_dist {
            if let Ok(data) = tokio::fs::read(dist.join(name)).await {
                return (
                    [(axum::http::header::CONTENT_TYPE, content_type(name))],
                    data,
                )
                    .into_response();
            }
            if let Ok(index) = tokio::fs::read(dist.join("index.html")).await {
                return (
                    [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
                    index,
                )
                    .into_response();
            }
        }
    }
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "webui 未构建：在 webui/ 执行 npm install && npm run build 后重新编译调度器",
    )
        .into_response()
}
