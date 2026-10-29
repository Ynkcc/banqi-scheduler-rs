//! handlers_replay.rs — episode 回放端点：局列表与单局上帝视角重建
//! （Go: internal/api/replay.go；JSON 契约真源 = banqi-frontend
//! src/scheduler/types.ts，camelCase 逐字段一致，前端零改动）。
//!
//! 数据链路：episode id → DB 查 object_key → 预签名 GET → 限长下载（256MB）
//! → gzip 解压（bqs_storage::fetch）→ EpisodeBatch proto 解码 → 逐 GameRecord
//! 转换（单局重建委托 `replay_build::build_game`，复用共享回放库 bq-replay）。
//!
//! 错误语义（对齐 Go 版）：id/index 非法 400；episode 不存在 404；拉取链路
//! （预签名/下载/解压/解码）全部 502（Go fetchEpisodeBatch 错误统一 502）；
//! 单局重建失败 400（记录损坏 / 序号越界不可回放）。

use axum::extract::{Path, State};
use axum::Json;
use prost::Message;

use bqs_proto::EpisodeBatch;

use crate::views::{data_kind_name, ReplayGameSummaryView, ReplayListView};
use crate::{ApiError, ApiResult, AppState};

/// 拉取 + 解压 + 解码（同步；调用方须置于 `spawn_blocking`）。
/// 错误统一映射 502（对齐 Go 版：fetchEpisodeBatch 的全部错误归 502）。
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

/// 终局原因 → 展示名（Go: replay.go terminationLabel / darkchess.go
/// terminationName，两处一致）。
pub(crate) fn termination_label(t: u32) -> &'static str {
    match t {
        1 => "decisive",
        2 => "quiet_draw",
        3 => "max_steps",
        4 => "aborted",
        _ => "unspecified",
    }
}

/// 对局一方的人类可读描述（Go: describeSide）。sha="rule" = 规则哨兵；
/// spec 非空 = 规则对手；sha 非空 = 模型；两者皆空 = 镜像引擎。
pub(crate) fn describe_side(sha: &str, spec: &str) -> String {
    let short = |s: &str| -> String {
        if s.chars().count() > 12 { s.chars().take(12).collect() } else { s.to_string() }
    };
    match (sha, spec) {
        ("rule", _) => "规则评估器（哨兵预热）".into(),
        (_, spec) if !spec.is_empty() => format!("规则评估器（{spec}）"),
        (sha, _) if !sha.is_empty() => format!("NNUE {}", short(sha)),
        _ => "镜像对手（行动未单列）".into(),
    }
}

/// 单局重建（Go: replayGame → replay.ReplayNnue）。委托共享回放库。
pub(crate) fn replay_game(
    batch: &EpisodeBatch,
    ep: &bqs_core::types::Episode,
    index: usize,
) -> Result<crate::views::ReplayGameView, String> {
    crate::replay_build::build_game(batch, ep, index)
}

/// GET /api/episodes/{id}/replay — 列出该 episode 对象内全部对局的轻量摘要。
pub async fn episode_replay_list(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ReplayListView>> {
    let id: i64 = id.parse().map_err(|_| ApiError::bad_request("invalid episode id"))?;
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<ReplayListView, ApiError> {
        // Go: store.GetEpisode → 无记录 404
        let Some(ep) = server.ports.episodes.get_episode(id)? else {
            return Err(ApiError::not_found("episode not found"));
        };
        let batch = fetch_episode_batch(&st, &ep.object_key)?;
        let games: Vec<ReplayGameSummaryView> = batch
            .records
            .iter()
            .enumerate()
            .map(|(i, rec)| ReplayGameSummaryView {
                index: i,
                steps: rec.steps as usize,
                game_length: rec.steps as usize,
                winner: rec.winner,
                termination: termination_label(rec.termination_reason).to_string(),
                has_seed: rec.game_seed.is_some(),
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
    .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    Ok(Json(view?))
}

/// GET /api/episodes/{id}/replay/{index} — 指定局的完整步序列（上帝视角）。
pub async fn episode_replay_game(
    State(st): State<AppState>,
    Path((id, index)): Path<(String, String)>,
) -> ApiResult<Json<crate::views::ReplayGameView>> {
    let id: i64 = id.parse().map_err(|_| ApiError::bad_request("invalid episode id"))?;
    let index: i64 =
        index.parse().map_err(|_| ApiError::bad_request("invalid game index"))?;
    if index < 0 {
        return Err(ApiError::bad_request("invalid game index"));
    }
    let server = st.server.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<crate::views::ReplayGameView, ApiError> {
        let Some(ep) = server.ports.episodes.get_episode(id)? else {
            return Err(ApiError::not_found("episode not found"));
        };
        let batch = fetch_episode_batch(&st, &ep.object_key)?;
        let g = replay_game(&batch, &ep, index as usize)
            .map_err(ApiError::bad_request)?;
        Ok(g)
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))?;
    Ok(Json(view?))
}

// ---------------------------------------------------------------------------
// 单测：终局标签与对局双方描述（Go 语义对齐）
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn termination_labels_match_go() {
        for (t, want) in [(0, "unspecified"), (1, "decisive"), (2, "quiet_draw"), (3, "max_steps"), (4, "aborted"), (9, "unspecified")] {
            assert_eq!(termination_label(t), want);
        }
    }

    #[test]
    fn describe_side_matches_go() {
        assert_eq!(describe_side("rule", ""), "规则评估器（哨兵预热）");
        assert_eq!(describe_side("", "capture_first"), "规则评估器（capture_first）");
        assert_eq!(describe_side("abcdef1234567890", ""), "NNUE abcdef123456");
        assert_eq!(describe_side("", ""), "镜像对手（行动未单列）");
        // 规则哨兵带 spec 时仍按 sha="rule" 优先（Go switch 顺序）
        assert_eq!(describe_side("rule", "x"), "规则评估器（哨兵预热）");
    }
}
