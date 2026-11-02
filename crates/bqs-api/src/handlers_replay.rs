use axum::extract::{Path, State};
use axum::Json;
use prost::Message;

use bqs_proto::EpisodeBatch;

use crate::views::{data_kind_name, ReplayGameSummaryView, ReplayListView};
use crate::{read_endpoint, ApiError, ApiResult, AppState};

pub(crate) fn fetch_episode_batch(
    st: &AppState,
    object_key: &str,
) -> Result<EpisodeBatch, ApiError> {
    let signer = st.server.signer.clone();
    let key = object_key.to_string();
    let raw = bqs_storage::fetch::fetch_gzip_object(&*signer, &key)
        .map_err(|e| ApiError::bad_gateway(e.to_string()))?;
    EpisodeBatch::decode(raw.as_slice())
        .map_err(|e| ApiError::bad_gateway(format!("decode {key}: {e}")))
}

pub(crate) fn termination_label(t: u32) -> &'static str {
    match t {
        1 => "decisive",
        2 => "quiet_draw",
        3 => "max_steps",
        4 => "aborted",
        _ => "unspecified",
    }
}

pub(crate) fn red_is_a_of(rec: &bqs_proto::GameRecord) -> bool {
    rec.a_is_red.unwrap_or_else(|| match rec.producers.first() {
        Some(&p) if p != 0xFF => p == 0,
        _ => true,
    })
}

pub(crate) fn describe_side(sha: &str, spec: &str) -> String {
    let short = |s: &str| -> String {
        if s.chars().count() > 12 {
            s.chars().take(12).collect()
        } else {
            s.to_string()
        }
    };
    match (sha, spec) {
        ("rule", _) => "规则评估器（哨兵预热）".into(),
        (_, spec) if !spec.is_empty() => format!("规则评估器（{spec}）"),
        (sha, _) if !sha.is_empty() => format!("NNUE {}", short(sha)),
        _ => "镜像对手（行动未单列）".into(),
    }
}

pub(crate) fn replay_game(
    batch: &EpisodeBatch,
    ep: &bqs_core::types::Episode,
    index: usize,
) -> Result<crate::views::ReplayGameView, String> {
    crate::replay_build::build_game(batch, ep, index)
}

pub async fn episode_replay_list(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ReplayListView>> {
    let id: i64 = id
        .parse()
        .map_err(|_| ApiError::bad_request("invalid episode id"))?;
    read_endpoint(&st, move |st| -> Result<ReplayListView, ApiError> {
        let Some(ep) = st.server.ports.episodes.get_episode(id)? else {
            return Err(ApiError::not_found("episode not found"));
        };
        let batch = fetch_episode_batch(st, &ep.object_key)?;
        let games: Vec<ReplayGameSummaryView> = batch
            .records
            .iter()
            .enumerate()
            .map(|(i, rec)| {
                let gi = rec.game_index.map(|g| g as usize).unwrap_or(i);
                ReplayGameSummaryView {
                    index: i,
                    steps: rec.steps as usize,
                    game_length: rec.steps as usize,
                    winner: rec.winner,
                    termination: termination_label(rec.termination_reason).to_string(),
                    has_seed: rec.game_seed.is_some(),

                    red_is_a: red_is_a_of(rec),
                    seed: rec.game_seed.map(|s| s.to_string()),
                    seed_refreshed: rec.seed_refreshed.unwrap_or(false),
                    pair_index: rec.pair_index.unwrap_or((gi / 2) as u32),
                    pair_position: rec.pair_position.unwrap_or((gi % 2) as u32),
                }
            })
            .collect();
        Ok(ReplayListView {
            id: ep.id,
            kind: data_kind_name(batch.kind as i64).to_string(),
            variant: batch.variant.clone(),
            game_count: ep.game_count,
            games,
        })
    })
    .await
}

pub async fn episode_replay_game(
    State(st): State<AppState>,
    Path((id, index)): Path<(String, String)>,
) -> ApiResult<Json<crate::views::ReplayGameView>> {
    let id: i64 = id
        .parse()
        .map_err(|_| ApiError::bad_request("invalid episode id"))?;
    let index: i64 = index
        .parse()
        .map_err(|_| ApiError::bad_request("invalid game index"))?;
    if index < 0 {
        return Err(ApiError::bad_request("invalid game index"));
    }
    read_endpoint(
        &st,
        move |st| -> Result<crate::views::ReplayGameView, ApiError> {
            let Some(ep) = st.server.ports.episodes.get_episode(id)? else {
                return Err(ApiError::not_found("episode not found"));
            };
            let batch = fetch_episode_batch(st, &ep.object_key)?;
            let g = replay_game(&batch, &ep, index as usize).map_err(ApiError::bad_request)?;
            Ok(g)
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn termination_labels_match_go() {
        for (t, want) in [
            (0, "unspecified"),
            (1, "decisive"),
            (2, "quiet_draw"),
            (3, "max_steps"),
            (4, "aborted"),
            (9, "unspecified"),
        ] {
            assert_eq!(termination_label(t), want);
        }
    }

    #[test]
    fn describe_side_matches_go() {
        assert_eq!(describe_side("rule", ""), "规则评估器（哨兵预热）");
        assert_eq!(
            describe_side("", "capture_first"),
            "规则评估器（capture_first）"
        );
        assert_eq!(describe_side("abcdef1234567890", ""), "NNUE abcdef123456");
        assert_eq!(describe_side("", ""), "镜像对手（行动未单列）");

        assert_eq!(describe_side("rule", "x"), "规则评估器（哨兵预热）");
    }

    #[test]
    fn red_is_a_prefers_record_field_and_producer_fallback() {
        let mut rec = bqs_proto::GameRecord::default();
        rec.a_is_red = Some(false);
        assert!(!red_is_a_of(&rec), "a_is_red=Some(false) 应判定 A 执黑");
        rec.a_is_red = Some(true);
        assert!(red_is_a_of(&rec));

        let mut old = bqs_proto::GameRecord::default();
        old.producers = vec![1];
        assert!(!red_is_a_of(&old));
        old.producers = vec![0];
        assert!(red_is_a_of(&old));

        let mut rule = bqs_proto::GameRecord::default();
        rule.producers = vec![0xFF];
        assert!(red_is_a_of(&rule));

        let mut tricky = bqs_proto::GameRecord::default();
        tricky.a_is_red = Some(false);
        tricky.players = vec![1, u8::MAX];
        assert!(!red_is_a_of(&tricky));
    }
}
