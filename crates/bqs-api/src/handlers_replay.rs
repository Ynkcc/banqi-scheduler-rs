use axum::Json;
use axum::extract::{Path, State};
use prost::Message;

use bq_record::GameLabelPack;
use bq_record::types::GameRecordBatch;

use crate::views::{ReplayGameSummaryView, ReplayListView, data_kind_name};
use crate::{ApiError, ApiResult, AppState, read_endpoint};

pub(crate) fn fetch_episode_batch(
    st: &AppState,
    object_key: &str,
) -> Result<GameRecordBatch, ApiError> {
    let signer = st.server.signer.clone();
    let key = object_key.to_string();
    let raw = bqs_storage::fetch::fetch_gzip_object(&*signer, &key)
        .map_err(|e| ApiError::bad_gateway(e.to_string()))?;
    GameRecordBatch::decode(raw.as_slice())
        .map_err(|e| ApiError::bad_gateway(format!("decode {key}: {e}")))
}

pub(crate) fn fetch_label_pack(
    st: &AppState,
    label_object_key: &str,
) -> Result<Option<GameLabelPack>, ApiError> {
    if label_object_key.is_empty() {
        return Ok(None);
    }
    let signer = st.server.signer.clone();
    let key = label_object_key.to_string();
    match bqs_storage::fetch::fetch_gzip_object(&*signer, &key) {
        Ok(raw) => match bq_record::codec::decode_label_pack(&raw) {
            Ok(pack) => Ok(Some(pack)),
            Err(e) => {
                tracing::warn!("[replay] 标签包解码失败 {key}: {e}（按缺失降级）");
                Ok(None)
            }
        },
        Err(e) => {
            tracing::warn!("[replay] 标签包拉取失败 {key}: {e}（按缺失降级）");
            Ok(None)
        }
    }
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

pub(crate) fn red_is_a_of(rec: &bq_record::GameRecord) -> bool {
    let producers = rec
        .provenance
        .as_ref()
        .map(|p| p.producers.as_slice())
        .unwrap_or(&[]);
    rec.identity
        .as_ref()
        .and_then(|i| i.a_is_red)
        .unwrap_or_else(|| match producers.first() {
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
    batch: &GameRecordBatch,
    labels: Option<&GameLabelPack>,
    ep: &bqs_core::types::Episode,
    index: usize,
) -> Result<crate::views::ReplayGameView, String> {
    crate::replay_build::build_game(batch, labels, ep, index)
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
                let identity = rec.identity.as_ref();
                let outcome = rec.outcome.as_ref();
                let gi = identity
                    .and_then(|x| x.game_index)
                    .map(|g| g as usize)
                    .unwrap_or(i);
                ReplayGameSummaryView {
                    index: i,
                    steps: rec.steps(),
                    game_length: rec.steps(),
                    winner: outcome.and_then(|o| o.winner),
                    termination: termination_label(
                        outcome.map(|o| o.termination_reason).unwrap_or(0),
                    )
                    .to_string(),
                    has_seed: identity.and_then(|x| x.game_seed).is_some(),

                    red_is_a: red_is_a_of(rec),
                    seed: identity.and_then(|x| x.game_seed).map(|s| s.to_string()),
                    seed_refreshed: identity.and_then(|x| x.seed_refreshed).unwrap_or(false),
                    pair_index: identity
                        .and_then(|x| x.pair_index)
                        .unwrap_or((gi / 2) as u32),
                    pair_position: identity
                        .and_then(|x| x.pair_position)
                        .unwrap_or((gi % 2) as u32),
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
            let labels = fetch_label_pack(st, &ep.label_object_key)?;
            let g = replay_game(&batch, labels.as_ref(), &ep, index as usize).map_err(|e| {
                if e.contains("超出范围") {
                    ApiError::bad_request(e)
                } else {
                    ApiError::internal(format!("replay 渲染失败（record 数据矛盾）: {e}"))
                }
            })?;
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
        let mut rec = bq_record::GameRecord::new("4x8", bq_record::RecordSource::SourceCollector);
        rec.identity.as_mut().unwrap().a_is_red = Some(false);
        assert!(!red_is_a_of(&rec), "a_is_red=Some(false) 应判定 A 执黑");
        rec.identity.as_mut().unwrap().a_is_red = Some(true);
        assert!(red_is_a_of(&rec));

        let mut old = bq_record::GameRecord::new("4x8", bq_record::RecordSource::SourceCollector);
        old.provenance = Some(bq_record::Provenance {
            producers: vec![1],
            ..Default::default()
        });
        assert!(!red_is_a_of(&old));
        old.provenance.as_mut().unwrap().producers = vec![0];
        assert!(red_is_a_of(&old));

        let mut rule = bq_record::GameRecord::new("4x8", bq_record::RecordSource::SourceCollector);
        rule.provenance = Some(bq_record::Provenance {
            producers: vec![0xFF],
            ..Default::default()
        });
        assert!(red_is_a_of(&rule));

        let mut tricky =
            bq_record::GameRecord::new("4x8", bq_record::RecordSource::SourceCollector);
        tricky.identity.as_mut().unwrap().a_is_red = Some(false);
        tricky.provenance = Some(bq_record::Provenance {
            producers: vec![1, u8::MAX],
            ..Default::default()
        });
        assert!(!red_is_a_of(&tricky));
    }
}
