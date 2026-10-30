use axum::Json;
use axum::extract::State;
use axum::http::Uri;

use bqs_core::types::Episode;

use crate::views::{DataStatsView, EpisodePageView, EpisodeView, data_kind_name};
use crate::{ApiError, ApiResult, AppState, int_query, read_endpoint};

use bqs_store::clamp_episode_page_limit;

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

pub async fn episodes(State(st): State<AppState>, uri: Uri) -> ApiResult<Json<EpisodePageView>> {
    let before = int_query(uri.query(), "before");
    let limit = clamp_episode_page_limit(int_query(uri.query(), "limit"));
    read_endpoint(&st, move |st| -> Result<EpisodePageView, ApiError> {
        let items = st.server.ports.episodes.list_episodes_page(before, limit)?;
        let next = items.last().map(|e| e.id).unwrap_or(before);
        Ok(EpisodePageView {
            items: items.iter().map(to_episode_view).collect(),
            next_before: next,
            has_more: items.len() as i64 == limit,
        })
    })
    .await
}

pub async fn data_stats(State(st): State<AppState>) -> ApiResult<Json<DataStatsView>> {
    read_endpoint(&st, |st| -> Result<DataStatsView, ApiError> {
        let server = &st.server;
        let stats = server.ports.episodes.episode_data_stats()?;
        let to_views = |m: std::collections::BTreeMap<i64, bqs_core::ports::KindStats>| {
            m.into_iter()
                .map(|(k, v)| (data_kind_name(k).to_string(), v.into()))
                .collect()
        };
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
}
