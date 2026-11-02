use axum::extract::{Path, State};
use axum::http::Uri;
use axum::Json;

use bqs_core::types::Episode;

use crate::views::{
    data_kind_name, DataStatsView, EpisodeGameView, EpisodePageView, EpisodeView,
    GameLookupItemView, GameLookupPageView,
};
use crate::{int_query, read_endpoint, ApiError, ApiResult, AppState};

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
        seed_base: e.seed_base.map(|s| (s as u64).to_string()),
        pairs_parallel: e.pairs_parallel,
        pair_seed_refresh: e.pair_seed_refresh,
    }
}

fn u64_query(query: Option<&str>, key: &str) -> Option<u64> {
    let q = query?;
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == key {
            return v.parse().ok();
        }
    }
    None
}

pub async fn episode_games(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<EpisodeGameView>>> {
    let id: i64 = id
        .parse()
        .map_err(|_| ApiError::bad_request("invalid episode id"))?;
    read_endpoint(&st, move |st| -> Result<Vec<EpisodeGameView>, ApiError> {
        if st.server.ports.episodes.get_episode(id)?.is_none() {
            return Err(ApiError::not_found(format!("episode {id} 不存在")));
        }
        let rows = st.server.ports.episodes.get_episode_games(id)?;
        Ok(rows.iter().map(EpisodeGameView::from_row).collect())
    })
    .await
}

pub async fn lookup_game(
    State(st): State<AppState>,
    uri: Uri,
) -> ApiResult<Json<GameLookupPageView>> {
    let Some(seed) = u64_query(uri.query(), "seed") else {
        return Err(ApiError::bad_request("缺少 seed 查询参数"));
    };
    read_endpoint(&st, move |st| -> Result<GameLookupPageView, ApiError> {
        let hits = st.server.ports.episodes.find_games_by_seed(seed)?;
        let mut items = Vec::with_capacity(hits.len());
        for (ep, game) in hits {
            items.push(GameLookupItemView {
                episode: to_episode_view(&ep),
                game: EpisodeGameView::from_row(&game),
            });
        }
        Ok(GameLookupPageView { items })
    })
    .await
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
