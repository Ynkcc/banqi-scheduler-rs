//! handlers_episodes.rs — episode 数据端点：/api/episodes 分页列表与数据流统计
//! （/api/data-stats）。（Go: internal/api/handlers_episodes.go）

use axum::extract::State;
use axum::http::Uri;
use axum::Json;


use bqs_core::types::Episode;

use crate::views::{data_kind_name, DataStatsView, EpisodePageView, EpisodeView};
use crate::{int_query, ApiError, ApiResult, AppState};

const DEFAULT_EPISODE_LIMIT: i64 = 100;
const MAX_EPISODE_LIMIT: i64 = 500;

/// episode 行 → 展示视图（episodes 与 data-stats 共用；Go: toEpisodeView）。
fn to_episode_view(e: &Episode) -> EpisodeView {
    EpisodeView {
        id: e.id,
        worker_id: e.worker_id.clone(),
        task_id: e.task_id.clone(),
        network_sha: e.network_sha.clone(),
        game_count: e.game_count,
        total_steps: e.total_steps,
        winner: e.winner,
        data_kind: data_kind_name(e.kind).to_string(),
        object_key: e.object_key.clone(),
        created_at: e.created_at,
        opponent_sha: e.opponent_sha.clone(),
        opponent_spec: e.opponent_spec.clone(),
        match_id: e.match_id,
        red_wins: e.red_wins,
        black_wins: e.black_wins,
        draw_no_capture: e.draw_no_capture,
        draw_truncation: e.draw_truncation,
    }
}

/// GET /api/episodes?before=&limit= — 元数据按 id 倒序分页
/// （limit 越界或缺省回落 100，上限 500；hasMore = 取满 limit）。
pub async fn episodes(State(st): State<AppState>, uri: Uri) -> ApiResult<Json<EpisodePageView>> {
    let before = int_query(uri.query(), "before");
    let mut limit = int_query(uri.query(), "limit");
    if limit <= 0 || limit > MAX_EPISODE_LIMIT {
        limit = DEFAULT_EPISODE_LIMIT;
    }
    let server = st.server.clone();
    let page = tokio::task::spawn_blocking(move || -> Result<EpisodePageView, ApiError> {
        let items = server.ports.episodes.list_episodes_page(before, limit)?;
        let next = items.last().map(|e| e.id).unwrap_or(before);
        Ok(EpisodePageView {
            items: items.iter().map(to_episode_view).collect(),
            next_before: next,
            has_more: items.len() as i64 == limit,
        })
    })
    .await
    .map_err(|e| crate::ApiError::internal(format!("join: {e}")))??;
    Ok(Json(page))
}

/// GET /api/data-stats — 数据流入实时总览：累计 / 近 1h / 近 24h 按数据类别细分 +
/// 最近登记的 episode + 本代数据量（fresh-only 训练窗口）。
pub async fn data_stats(State(st): State<AppState>) -> ApiResult<Json<DataStatsView>> {
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<DataStatsView, ApiError> {
        // 三窗口聚合（键 = kind 原值，api 层做 kind → 名称映射）
        let stats = server.ports.episodes.episode_data_stats()?;
        let to_views = |m: std::collections::BTreeMap<i64, bqs_core::ports::KindStats>| {
            m.into_iter()
                .map(|(k, v)| (data_kind_name(k).to_string(), v.into()))
                .collect()
        };
        // 最近登记的 episode（Go: ListEpisodes(ctx, 0, 12)）
        let recent = server
            .ports
            .episodes
            .list_episodes_page(0, 12)?
            .iter()
            .map(to_episode_view)
            .collect();
        Ok(DataStatsView {
            total: to_views(stats.total),
            last_1h: to_views(stats.last_1h),
            last_24h: to_views(stats.last_24h),
            recent,
            generation: server.ports.episodes.generation_data_stats()?.into(),
        })
    })
    .await
    .map_err(|e| crate::ApiError::internal(format!("join: {e}")))??;
    Ok(Json(view))
}
