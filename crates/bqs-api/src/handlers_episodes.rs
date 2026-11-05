use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::Json;
use serde::{Deserialize, Serialize};

use bqs_core::types::Episode;

use crate::views::{
    data_kind_name, DataStatsView, EpisodeGameView, EpisodePageView, EpisodeView,
    GameLookupItemView, GameLookupPageView, MatchPairCategoryView, MatchPairItemView,
    MatchPairsView,
};
use crate::{int_query, read_endpoint, ApiError, ApiResult, AppState, MAX_CONTROL_BODY};

#[derive(Debug, Deserialize)]
pub struct ImportEpisodeBody {
    #[serde(default)]
    pub network_sha: String,

    #[serde(default = "default_import_kind")]
    pub kind: i64,
    pub game_count: i64,
    #[serde(default)]
    pub total_steps: i64,

    #[serde(default)]
    pub winner: i64,
    #[serde(default)]
    pub match_id: i64,
    #[serde(default)]
    pub opponent_sha: String,
    #[serde(default)]
    pub opponent_spec: String,
    #[serde(default)]
    pub note: String,
}

fn default_import_kind() -> i64 {
    1
}

#[derive(Debug, Serialize)]
pub struct ImportEpisodeView {
    pub id: i64,
    pub object_key: String,
    pub upload_url: String,
    pub replay_url: String,
}

pub async fn import_episode(
    State(st): State<AppState>,
    bytes: Bytes,
) -> ApiResult<Json<ImportEpisodeView>> {
    let body: ImportEpisodeBody = crate::decode_body(&bytes, MAX_CONTROL_BODY)?;
    let ack = st
        .server
        .import_episode(
            body.network_sha.trim(),
            body.kind,
            body.game_count,
            body.total_steps,
            body.winner,
            body.match_id,
            body.opponent_sha.trim(),
            body.opponent_spec.trim(),
            body.note.trim(),
        )
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    Ok(Json(ImportEpisodeView {
        id: ack.id,
        object_key: ack.object_key,
        upload_url: ack.upload_url,
        replay_url: format!("/scheduler.html#/replay?episode={}", ack.id),
    }))
}

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
    let match_id = int_query(uri.query(), "match");
    read_endpoint(&st, move |st| -> Result<EpisodePageView, ApiError> {
        let items = if match_id > 0 {
            st.server
                .ports
                .episodes
                .list_episodes_by_match(match_id, before, limit)?
        } else {
            st.server.ports.episodes.list_episodes_page(before, limit)?
        };
        let next = items.last().map(|e| e.id).unwrap_or(before);
        Ok(EpisodePageView {
            items: items.iter().map(to_episode_view).collect(),
            next_before: next,
            has_more: items.len() as i64 == limit,
        })
    })
    .await
}

fn candidate_result_char(g: &bqs_core::types::EpisodeGameRow) -> char {
    let w = g.winner.unwrap_or(0);
    if w == 0 {
        return 'D';
    }
    let a_wins = if g.a_is_red { w > 0 } else { w < 0 };
    if a_wins {
        'W'
    } else {
        'L'
    }
}

fn pair_category(results: &str) -> &'static str {
    let b = results.as_bytes();
    match (b[0], b[1]) {
        (b'L', b'L') => "LL",
        (b'L', b'D') | (b'D', b'L') => "LD",
        (b'D', b'D') => "DD",
        (b'W', b'L') | (b'L', b'W') => "WL",
        (b'D', b'W') | (b'W', b'D') => "DW",
        (b'W', b'W') => "WW",
        _ => "--",
    }
}

pub async fn match_pairs(
    State(st): State<AppState>,
    Path(id): Path<i64>,
) -> ApiResult<Json<MatchPairsView>> {
    read_endpoint(&st, move |st| -> Result<MatchPairsView, ApiError> {
        if id <= 0 {
            return Err(ApiError::bad_request("match id 无效"));
        }
        let eps = st
            .server
            .ports
            .episodes
            .list_episodes_by_match(id, 0, 100_000)?;

        const ORDER: [&str; 6] = ["LL", "LD", "DD", "WL", "DW", "WW"];
        let mut buckets: std::collections::BTreeMap<&'static str, Vec<MatchPairItemView>> =
            std::collections::BTreeMap::new();
        for ep in &eps {
            let games = st.server.ports.episodes.get_episode_games(ep.id)?;
            let mut by_pair: std::collections::BTreeMap<
                i64,
                Vec<&bqs_core::types::EpisodeGameRow>,
            > = std::collections::BTreeMap::new();
            for g in &games {
                by_pair.entry(g.pair_index).or_default().push(g);
            }
            for (pair_index, mut gs) in by_pair {
                gs.sort_by_key(|g| g.pair_position);
                if gs.len() != 2 {
                    continue;
                }
                let results = format!(
                    "{}{}",
                    candidate_result_char(gs[0]),
                    candidate_result_char(gs[1])
                );
                let category = pair_category(&results);
                buckets
                    .entry(category)
                    .or_default()
                    .push(MatchPairItemView {
                        episode_id: ep.id,
                        pair_index,
                        game_indexes: vec![gs[0].game_index, gs[1].game_index],
                        results,
                    });
            }
        }
        Ok(MatchPairsView {
            match_id: id,
            pairs: ORDER
                .iter()
                .map(|c| {
                    let items = buckets.remove(*c).unwrap_or_default();
                    MatchPairCategoryView {
                        category: (*c).to_string(),
                        count: items.len() as i64,
                        items,
                    }
                })
                .collect(),
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
