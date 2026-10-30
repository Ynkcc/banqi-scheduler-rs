//! 【所有权：edge 成员】
//!
//! WebUI JSON API（axum，同进程 HTTP，默认仅本机，无鉴权勿暴露公网）。
//!
//! 参考实现（行为真源）：`banqi-scheduler/internal/api/`（server.go / handlers*.go /
//! storage.go / helpers.go），JSON 字段名以 Go 版视图 tag 为准（`/api/status` 契约
//! 由 Go 版 `contract_test.go` 锁定，本 crate 以 `tests::contract` 快照对照）——
//! **前端零改动可用**。
//!
//! 路由（与 Go 版一致）：
//! - 只读：/api/status / networks / matches / eval / workers / trainers /
//!   episodes（分页）/ episodes/{id}/replay(/{index})（回放）/ data-stats /
//!   tasks / task-stats / train-config / storage(+active, profiles/{name})
//! - 控制：POST /api/networks/{sha}/promote、POST /api/control、
//!   PUT /api/train-config、PUT|DELETE /api/storage/profiles/{name}、
//!   POST /api/storage/activate、PUT /api/workers/{id}/note、POST /api/eval/trigger、
//!   GET|PUT /api/eval-config
//! - 静态：`server.webui_dist` 目录托管 + SPA 回落 index.html（未配置 → 503 提示）
//!
//! 约定：handlers 依赖 `bqs_core::Server` 的子系统 pub 字段读取查询方法
//! （`srv.ports.*` / `srv.evalc.*` / `srv.disp.*` / `srv.ctl`）；需要新增 store 查询
//! 时先与 main 协调扩展 `bqs_core::ports`（勿绕过端口）。

pub mod handlers_control;
pub mod handlers_episodes;
pub mod handlers_jobs;
pub mod handlers_eval;
pub mod handlers_replay;
pub mod handlers_status;
pub mod handlers_storage;
pub mod handlers_trainers;
pub mod replay_build;
pub mod views;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post, put};
use axum::{Json, Router};
use serde::de::DeserializeOwned;

use bqs_core::server::Server;

/// 请求体大小上限：控制请求 4096 / 训练配置 16384（Go: maxControlBody /
/// maxTrainConfigBody）；axum 无逐路由读限，取两者最大值全局收紧。
pub const MAX_BODY_LIMIT: usize = 16 * 1024;
pub const MAX_CONTROL_BODY: usize = 4096;
pub const MAX_TRAIN_CONFIG_BODY: usize = 16 * 1024;

/// API 状态束：`Manager` 不进 Server，与门面并列表达（装配由 bqs-server 完成）。
#[derive(Clone)]
pub struct AppState {
    pub server: Arc<Server>,
    pub storage: Arc<bqs_storage::Manager>,
    /// worker 在线判定窗口（秒；Go: onlineWindow = server.worker_online_seconds）。
    pub online_window_secs: i64,
    /// WebUI 静态资源目录（None = 不托管，命中回落路径一律 503）。
    pub webui_dist: Option<Arc<PathBuf>>,
}

/// 统一错误应答：`{"error": msg}`（Go: writeError）；msg 直接展示给运维。
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn internal(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: msg.into() }
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: msg.into() }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: msg.into() }
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::CONFLICT, message: msg.into() }
    }
    pub fn bad_gateway(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_GATEWAY, message: msg.into() }
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

/// 解析 JSON 请求体（统一大小上限 + 统一 400 错误回包，对齐 Go decodeBody）。
pub fn decode_body<T: DeserializeOwned>(bytes: &Bytes, max: usize) -> Result<T, ApiError> {
    if bytes.len() > max {
        return Err(ApiError::bad_request("http: request body too large"));
    }
    serde_json::from_slice(bytes)
        .map_err(|e| ApiError::bad_request(format!("invalid json body: {e}")))
}

/// URL 查询参数解析（Go: r.URL.Query().Get + ParseInt，解析失败按 0 处理）。
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

/// `bqs_storage::StorageProfile` → 对外掩码视图（secret 永不出域）。
pub fn to_profile_view(p: &bqs_storage::StorageProfile) -> views::StorageProfileView {
    views::StorageProfileView {
        name: p.name.clone(),
        endpoint: p.endpoint.clone(),
        region: p.region.clone(),
        access_key: mask_key(&p.access_key),
        has_secret: !p.secret_key.is_empty(),
        bucket: p.bucket.clone(),
        path_style: p.path_style,
    }
}

/// accessKey 掩码（Go: maskKey：<=8 全掩码，否则前 4 + **** + 后 4）。
pub fn mask_key(k: &str) -> String {
    if k.is_empty() {
        String::new()
    } else if k.len() <= 8 {
        "****".into()
    } else {
        format!("{}****{}", &k[..4], &k[k.len() - 4..])
    }
}

/// profile 名白名单（Go: profileNameRe `^[A-Za-z0-9_-]{1,32}$`；避免引入 regex，
/// 手写等价判定）。
pub fn valid_profile_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

// ---------------------------------------------------------------------------
// 路由表（与 Go internal/api/server.go 的 mux 注册逐一对应）
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    let api = Router::new()
        // 只读
        .route("/api/status", get(handlers_status::status))
        .route("/api/networks", get(handlers_status::networks))
        .route("/api/networks/{sha}/promote", post(handlers_status::promote))
        .route("/api/matches", get(handlers_status::matches))
        .route("/api/eval", get(handlers_eval::eval))
        .route("/api/eval/trigger", post(handlers_eval::trigger))
        .route(
            "/api/eval-config",
            get(handlers_eval::eval_config_get).put(handlers_eval::eval_config_put),
        )
        .route("/api/workers", get(handlers_status::workers))
        .route("/api/workers/{id}/note", put(handlers_status::worker_note))
        .route("/api/trainers", get(handlers_trainers::trainers))
        .route("/api/episodes", get(handlers_episodes::episodes))
        // episode 回放（Go: replay.go；对象拉取 + gzip + proto 解码 + god 视角重建）
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
        .route("/api/storage/active", get(handlers_storage::storage_active_get))
        .route(
            "/api/storage/profiles/{name}",
            get(handlers_storage::profile_get)
                .put(handlers_storage::profile_put)
                .delete(handlers_storage::profile_delete),
        )
        .route("/api/storage/activate", post(handlers_storage::activate))
        // 统一 Job 机制（collect / selfplay / eval job 的提交 / 进度 / 控制）
        .route("/api/jobs", get(handlers_jobs::list_jobs).post(handlers_jobs::submit_job))
        .route("/api/jobs/{id}/{action}", post(handlers_jobs::control_job))
        // 控制
        .route("/api/control", post(handlers_control::control))
        .layer(DefaultBodyLimit::max(MAX_BODY_LIMIT))
        .with_state(state.clone());

    Router::new().merge(api).fallback(any(static_handler)).with_state(state)
}

// ---------------------------------------------------------------------------
// 静态托管 + SPA 回落（Go: handleStatic；embed 产物 → 磁盘目录）
// ---------------------------------------------------------------------------

/// 扩展名 → Content-Type（Go: mime.TypeByExtension 的 WebUI 产物子集）。
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

/// 静态文件 + SPA 回落：非 /api 路径先按名取文件，未命中回落 index.html；
/// 未配置目录（或目录缺 index.html）→ 503 文本提示（Go 版语义）。
async fn static_handler(
    State(st): State<AppState>,
    req: Request<axum::body::Body>,
) -> Response {
    if req.method() != Method::GET && req.method() != Method::HEAD {
        return ApiError::bad_request("method not allowed").into_response();
    }
    let path = req.uri().path();
    if path.starts_with("/api/") {
        return ApiError::not_found(format!("unknown api path: {path}")).into_response();
    }
    let rel = path.trim_start_matches('/');
    // 防目录穿越（Go: path.Clean；这里直接拒绝含 .. 的段）
    let safe = !rel.split('/').any(|seg| seg == "..");
    if safe {
        let name = if rel.is_empty() { "index.html" } else { rel };
        if let Some(dist) = &st.webui_dist {
            if let Ok(data) = tokio::fs::read(dist.join(name)).await {
                return ([(axum::http::header::CONTENT_TYPE, content_type(name))], data)
                    .into_response();
            }
            if let Ok(index) = tokio::fs::read(dist.join("index.html")).await {
                return ([(
                    axum::http::header::CONTENT_TYPE,
                    "text/html; charset=utf-8",
                )], index)
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

// ---------------------------------------------------------------------------
// 契约快照测试（对照 Go contract_test.go 的字段集；fake 状态序列化后断言字段名）
// ---------------------------------------------------------------------------

#[cfg(test)]
mod contract {
    use super::*;
    use crate::views::*;

    /// 视图序列化后的顶层键集合。
    fn keys<T: serde::Serialize>(v: &T) -> Vec<String> {
        match serde_json::to_value(v).unwrap() {
            serde_json::Value::Object(map) => map.keys().cloned().collect(),
            other => panic!("期望 JSON 对象，实际 {other}"),
        }
    }

    fn assert_keys(got: &[String], want: &[&str]) {
        for k in want {
            assert!(got.iter().any(|g| g == k), "缺少字段 {k}（前端按名字取值，缺字段是破坏性变更）");
        }
        for g in got {
            assert!(
                want.contains(&g.as_str()),
                "出现未预期的字段 {g}（契约收敛后新增字段应同步更新快照测试）"
            );
        }
    }

    /// /api/status 顶层契约（Go contract_test.go TestStatusContractSnapshot）。
    #[test]
    fn status_top_level() {
        let v = StatusView {
            variant: "4x8".into(),
            paused: false,
            initial_revealed: 0,
            data_kind: "resnet".into(),
            min_client_version: String::new(),
            sprt: SprtView { elo0: 0.0, elo1: 15.0, alpha: 0.05, beta: 0.05 },
            best: None,
            counts: CountsView {
                networks: 0,
                candidates: 0,
                matches_running: 0,
                episodes: 0,
                episode_games: 0,
                workers: 0,
                eval_results: 0,
            },
            eval: EvalConfigView {
                enabled: false,
                opponents: vec![],
                games: 1000,
                task_games: 0,
                mcts_sims: 0,
                every_n_promotions: 1,
                no_progress_n: 3,
                no_progress_eps: 0.035,
                rollback_eps: 0.04,
                floor_win_rate: 0.0,
                stop_win_rate: 0.0,
                mode: "纯策略 argmax".into(),
                pending: 0,
            },
            should_stop: false,
            stop_reason: String::new(),
            auto_paused: false,
            auto_pause_reason: String::new(),
        };
        assert_keys(
            &keys(&v),
            &[
                "variant", "paused", "initialRevealed", "dataKind", "minClientVersion",
                "sprt", "best", "counts", "eval", "shouldStop", "stopReason",
                "autoPaused", "autoPauseReason",
            ],
        );
        assert_keys(&keys(&v.sprt), &["elo0", "elo1", "alpha", "beta"]);
        assert_keys(
            &keys(&v.eval),
            &[
                "enabled", "opponents", "games", "taskGames", "mctsSims",
                "everyNPromotions", "noProgressN", "noProgressEps", "rollbackEps",
                "floorWinRate", "stopWinRate", "mode", "pending",
            ],
        );
        assert_keys(
            &keys(&v.counts),
            &[
                "networks", "candidates", "matchesRunning", "episodes",
                "episodeGames", "workers", "evalResults",
            ],
        );
    }

    /// /api/status.best（networkView）与 /api/networks 条目契约。
    #[test]
    fn network_view() {
        let v = NetworkView {
            sha: "abc".into(),
            parent_sha: String::new(),
            created_at: 1,
            is_best: true,
            status: "best".into(),
            notes: String::new(),
        };
        assert_keys(&keys(&v), &["sha", "parentSha", "createdAt", "isBest", "status", "notes"]);
    }

    /// /api/matches 条目契约（pairs 为 6 元数组，Hexanomial 序列化）。
    #[test]
    fn match_view() {
        let v = MatchView {
            id: 1,
            candidate: "c".into(),
            opponent: "o".into(),
            status: "running".into(),
            pairs: bqs_judgment::Hexanomial([0; 6]),
            num_games: 0,
            target_games: 400,
            score: None,
            llr: 0.0,
            lower: -2.944,
            upper: 2.944,
            verdict: "continue".into(),
        };
        assert_keys(
            &keys(&v),
            &[
                "id", "candidate", "opponent", "status", "pairs", "numGames",
                "targetGames", "score", "llr", "lower", "upper", "verdict",
            ],
        );
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(json["pairs"].as_array().unwrap().len(), 6);
        assert!(json["score"].is_null());
    }

    /// /api/workers 条目契约。
    #[test]
    fn worker_view() {
        let v = WorkerView {
            id: "w".into(),
            note: String::new(),
            last_seen: 0,
            online: true,
            threads: 8,
            completed_games: 0,
            client_version: "0.1.0".into(),
            memory_mb: 1024,
            health_status: "ok".into(),
            last_error: String::new(),
            episodes_1h: 0,
            games_1h: 0,
            episodes_24h: 0,
            games_24h: 0,
            est_games_per_hour: 0.0,
            est_per_thread_hour: 0.0,
            est_samples: 0,
        };
        assert_keys(
            &keys(&v),
            &[
                "id", "note", "lastSeen", "online", "threads", "completedGames",
                "clientVersion", "memoryMb", "healthStatus", "lastError",
                "episodes1h", "games1h", "episodes24h", "games24h",
                "estGamesPerHour", "estPerThreadHour", "estSamples",
            ],
        );
    }

    /// /api/eval 契约（config/latest/trends/running + 趋势点字段）。
    #[test]
    fn eval_view() {
        let point = EvalResultView {
            network_sha: "v1".into(),
            opponent_spec: "rule:capture_first".into(),
            wins: 600,
            draws: 0,
            losses: 400,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
            num_games: 1000,
            win_rate: 0.6,
            avg_moves: 44.4,
            created_at: 1,
        };
        let trend = EvalTrendView {
            opponent: "rule:capture_first".into(),
            versions: 1,
            no_progress: 0,
            latest_win_rate: 0.6,
            points: vec![point.clone()],
        };
        let v = EvalView {
            config: EvalConfigView {
                enabled: true,
                opponents: vec![],
                games: 1000,
                task_games: 0,
                mcts_sims: 0,
                every_n_promotions: 1,
                no_progress_n: 3,
                no_progress_eps: 0.035,
                rollback_eps: 0.04,
                floor_win_rate: 0.0,
                stop_win_rate: 0.0,
                mode: "纯策略 argmax".into(),
                pending: 0,
            },
            should_stop: false,
            stop_reason: String::new(),
            latest: vec![point.clone()],
            trends: vec![trend],
            running: vec![],
        };
        assert_keys(
            &keys(&v),
            &["config", "shouldStop", "stopReason", "latest", "trends", "running"],
        );
        assert_keys(
            &keys(&point),
            &[
                "networkSha", "opponentSpec", "wins", "draws", "losses", "redWins",
                "blackWins", "drawNoCapture", "drawTruncation", "numGames", "winRate",
                "avgMoves", "createdAt",
            ],
        );
        let trend = &v.trends[0];
        assert_keys(&keys(trend), &["opponent", "versions", "noProgress", "latestWinRate", "points"]);
    }

    /// /api/trainers 契约（balance 配置 + trainer 快照）。
    #[test]
    fn trainers_view() {
        let tv = TrainerView {
            id: "tr-1".into(),
            last_seen: 0,
            online: true,
            variant: "4x8".into(),
            data_kind: "resnet".into(),
            consumed_key: String::new(),
            consumed_id: 0,
            registered_max_id: 0,
            unconsumed_batches: 0,
            samples_consumed: 0,
            backlog: 0,
            buffered: 0,
            health_status: "ok".into(),
            last_error: String::new(),
            progress: String::new(),
        };
        let v = TrainersView {
            config: BalanceConfigView {
                enabled: true,
                mode: "pause".into(),
                trainer_online_seconds: 120,
                stalled_seconds: 600,
                backlog_pause_batches: 0,
                pause_reanalysis: false,
            },
            auto_paused: false,
            auto_pause_reason: String::new(),
            trainers: vec![tv],
        };
        assert_keys(&keys(&v), &["config", "autoPaused", "autoPauseReason", "trainers"]);
        assert_keys(
            &keys(&v.config),
            &[
                "enabled", "mode", "trainerOnlineSeconds", "stalledSeconds",
                "backlogPauseBatches", "pauseReanalysis",
            ],
        );
        assert_keys(
            &keys(&v.trainers[0]),
            &[
                "id", "lastSeen", "online", "variant", "dataKind", "consumedKey",
                "consumedId", "registeredMaxId", "unconsumedBatches", "samplesConsumed",
                "backlog", "buffered", "healthStatus", "lastError", "progress",
            ],
        );
    }

    /// /api/train-config 契约（overrides + fields，含 enum,omitempty 语义）。
    #[test]
    fn train_config_view() {
        let with_enum = TrainConfigFieldView {
            name: "VALUE_TARGET_MODE".into(),
            kind: "enum".into(),
            min: 0.0,
            max: 0.0,
            has_max: false,
            gt_zero: false,
            r#enum: vec!["mcts".into()],
        };
        let no_enum = TrainConfigFieldView {
            name: "TRAIN_BATCH".into(),
            kind: "int".into(),
            min: 1.0,
            max: 0.0,
            has_max: false,
            gt_zero: false,
            r#enum: vec![],
        };
        let v = TrainConfigView { overrides: Default::default(), fields: vec![with_enum, no_enum] };
        assert_keys(&keys(&v), &["overrides", "fields"]);
        let json = serde_json::to_value(&v).unwrap();
        let f0 = &json["fields"][0];
        assert!(f0.get("enum").is_some(), "enum 字段存在时应输出");
        assert!(json["fields"][1].get("enum").is_none(), "空 enum 应被省略（omitempty）");
    }

    /// /api/episodes 条目与分页信封契约。
    #[test]
    fn episode_views() {
        let ev = EpisodeView {
            id: 1,
            worker_id: "w".into(),
            task_id: "t".into(),
            network_sha: "s".into(),
            game_count: 16,
            total_steps: 100,
            winner: 1,
            data_kind: "resnet".into(),
            object_key: "episodes/s/x.epb.gz".into(),
            opponent_sha: String::new(),
            opponent_spec: String::new(),
            match_id: 0,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
            created_at: 0,
        };
        assert_keys(
            &keys(&ev),
            &[
                "id", "workerId", "taskId", "networkSha", "gameCount", "totalSteps",
                "winner", "dataKind", "objectKey", "opponentSha", "opponentSpec",
                "matchId", "redWins", "blackWins", "drawNoCapture", "drawTruncation",
                "createdAt",
            ],
        );
        let page = EpisodePageView { items: vec![], next_before: 0, has_more: false };
        assert_keys(&keys(&page), &["items", "nextBefore", "hasMore"]);
    }

    /// /api/storage 契约（掩码语义：secret 永不出域）。
    #[test]
    fn storage_views() {
        assert_eq!(mask_key("abcd1234efgh"), "abcd****efgh");
        assert_eq!(mask_key("short"), "****");
        assert_eq!(mask_key(""), "");
        assert!(valid_profile_name("r2-prod_1"));
        assert!(!valid_profile_name("bad/name"));
        assert!(!valid_profile_name(""));
        assert!(!valid_profile_name(&"x".repeat(33)));
        let pv = StorageProfileView {
            name: "r2".into(),
            endpoint: "http://127.0.0.1:1".into(),
            region: "auto".into(),
            access_key: "abcd****efgh".into(),
            has_secret: true,
            bucket: "banqi".into(),
            path_style: true,
        };
        assert_keys(
            &keys(&pv),
            &["name", "endpoint", "region", "accessKey", "hasSecret", "bucket", "pathStyle"],
        );
        let sv = StorageView { active: "default".into(), profiles: vec![] };
        assert_keys(&keys(&sv), &["active", "profiles"]);
    }

    /// /api/eval-config GET 契约（override + effective）。
    #[test]
    fn eval_config_get_view() {
        let v = EvalConfigGetView {
            override_: None,
            effective: EvalEffectiveView {
                enabled: false,
                opponents: vec![],
                stop_win_rate: 0.0,
            },
        };
        assert_keys(&keys(&v), &["override", "effective"]);
        assert_keys(&keys(&v.effective), &["enabled", "opponents", "stopWinRate"]);
    }

    /// eval / eval trigger 响应契约。
    #[test]
    fn eval_trigger_view() {
        let v = EvalTriggerView { ok: true, triggered: 2, message: "已入队 2 条评估".into() };
        assert_keys(&keys(&v), &["ok", "triggered", "message"]);
    }

    /// episode 回放契约（对齐 banqi-frontend types.ts 的
    /// ReplayList/ReplayGameSummary/ReplayGame/ReplayStep/HiddenBag，camelCase）。
    #[test]
    fn replay_views() {
        use crate::views::*;

        let bag = HiddenBagView { red: [5, 2, 2, 2, 2, 2, 1], black: [0; 7] };
        assert_keys(&keys(&bag), &["red", "black"]);

        let summary = ReplayGameSummaryView {
            index: 0,
            steps: 8,
            game_length: 8,
            winner: Some(1),
            termination: "decisive".into(),
            has_seed: true,
        };
        assert_keys(
            &keys(&summary),
            &["index", "steps", "gameLength", "winner", "termination", "hasSeed"],
        );

        let list = ReplayListView {
            id: 7,
            kind: "nnue".into(),
            variant: "4x8".into(),
            game_count: 1,
            games: vec![summary],
        };
        assert_keys(&keys(&list), &["id", "kind", "variant", "gameCount", "games"]);

        let step = ReplayStepView {
            ply: 0,
            player: "Red".into(),
            action: 3,
            from: 3,
            to: 3,
            kind: "flip".into(),
            is_chance: true,
            revealed: "R_Gen".into(),
            captured: "B_Can".into(),
            move_counter: 0,
            scores: [60, 60],
            board: vec!["Hidden".into()],
            revealed_at: vec![false],
            hidden_bag: bag,
            board_source: "snapshot".into(),
            producer: 0,
            search_value: 0.25,
            player_value: -0.5,
            hidden_count: 32,
            search_depth: 4,
            search_nodes: 1024,
            chosen_value_gap: 0.0,
            expl_random: false,
            labeler_fallback: false,
            labeler_action: Some(3),
            policy_actions: Some(vec![3]),
            policy_values: Some(vec![1.0]),
        };
        let step_json = serde_json::to_value(&step).unwrap();
        for k in [
            "ply", "player", "action", "from", "to", "kind", "isChance", "revealed",
            "captured", "moveCounter", "scores", "board", "revealedAt", "hiddenBag",
            "boardSource", "producer", "searchValue", "playerValue", "hiddenCount",
            "searchDepth", "searchNodes", "chosenValueGap", "explRandom",
            "labelerFallback", "labelerAction", "policyActions", "policyValues",
        ] {
            assert!(step_json.get(k).is_some(), "ReplayStep 缺字段 {k}");
        }
        // omitempty 语义：false/空字符串省略（对齐 Go json tag）
        let mut quiet = ReplayStepView { is_chance: false, revealed: String::new(), captured: String::new(), policy_actions: None, policy_values: None, labeler_action: None, ..step.clone() };
        quiet.is_chance = false;
        quiet.revealed = String::new();
        quiet.captured = String::new();
        let quiet_json = serde_json::to_value(&quiet).unwrap();
        assert!(quiet_json.get("isChance").is_none(), "isChance=false 应省略");
        assert!(quiet_json.get("revealed").is_none(), "revealed 空 应省略");
        assert!(quiet_json.get("captured").is_none(), "captured 空 应省略");
        // 可空诊断字段：None 序列化为 null（TS: number[] | null）
        quiet.policy_actions = None;
        quiet.policy_values = None;
        quiet.labeler_action = None;
        let null_json = serde_json::to_value(&quiet).unwrap();
        assert!(null_json["policyActions"].is_null());
        assert!(null_json["policyValues"].is_null());
        assert!(null_json["labelerAction"].is_null());

        let game = ReplayGameView {
            index: 0,
            steps: 8,
            game_length: 8,
            winner: Some(1),
            termination: "decisive".into(),
            seed: 42,
            has_seed: true,
            side_a: "NNUE abcdef123456".into(),
            side_b: "规则评估器（capture_first）".into(),
            red_is_a: true,
            a_side_only: false,
            initial_board: vec!["Hidden".into()],
            initial_shown: vec![false],
            initial_bag: bag,
            final_board: vec!["Empty".into()],
            final_shown: vec![true],
            final_bag: bag,
            moves: vec![step],
            labeler_agree: 1.0,
            labeler_total: 8,
            expl_count: 0,
        };
        assert_keys(
            &keys(&game),
            &[
                "index", "steps", "gameLength", "winner", "termination", "seed",
                "hasSeed", "sideA", "sideB", "redIsA", "aSideOnly", "initialBoard",
                "initialShown", "initialBag", "finalBoard", "finalShown", "finalBag",
                "moves", "labelerAgree", "labelerTotal", "explCount",
            ],
        );
    }
}
