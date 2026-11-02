use bqs_proto::*;

use crate::{
    assembler::episode_key, reports::lookup_report_task, types::EpisodeDraft, CoreResult,
};

use super::Server;

impl Server {
    pub fn report_episode(&self, meta: EpisodeMeta) -> CoreResult<EpisodeAck> {
        let task = match lookup_report_task(&self.disp, &meta.task_id, &meta.worker_id) {
            Ok(t) => t,
            Err(rejected) => {
                return Ok(EpisodeAck {
                    accepted: rejected.accepted,
                    message: rejected.message,
                    ..Default::default()
                });
            }
        };

        if !task.network_sha.is_empty() && task.network_sha != meta.network_sha {
            return Ok(EpisodeAck {
                accepted: false,
                message: format!(
                    "network_sha_mismatch task={} got={}",
                    task.network_sha, meta.network_sha
                ),
                ..Default::default()
            });
        }

        if let Some(existing) = self.ports.episodes.get_episode_by_task(&meta.task_id)? {
            let url = self
                .signer
                .presign_put(&existing.object_key, self.ttl_secs())?;
            tracing::info!(
                "[episode] 幂等重发 worker={} task={} -> 复用 {}",
                meta.worker_id,
                meta.task_id,
                existing.object_key
            );
            return Ok(EpisodeAck {
                accepted: true,
                upload_url: url,
                object_key: existing.object_key,
                message: "duplicate_replay".into(),
                ..Default::default()
            });
        }
        let obj_id = crate::sources::new_id();

        let sha_for_key = if meta.network_sha.is_empty() {
            "anonymous"
        } else {
            &meta.network_sha
        };
        let key = episode_key(sha_for_key, &obj_id);
        let url = self.signer.presign_put(&key, self.ttl_secs())?;
        let draft = EpisodeDraft {
            worker_id: meta.worker_id.clone(),
            task_id: meta.task_id.clone(),
            network_sha: meta.network_sha.clone(),
            game_count: meta.game_count as i64,
            total_steps: meta.total_steps as i64,
            winner: meta.winner as i64,
            object_key: key.clone(),
            created_at: self.clock.now(),
            kind: meta.kind as i64,

            opponent_sha: task.opponent_sha.clone(),
            opponent_spec: task.opponent_spec.clone(),
            match_id: task.match_id,

            red_wins: meta.red_wins as i64,
            black_wins: meta.black_wins as i64,
            draw_no_capture: meta.draw_no_capture as i64,
            draw_truncation: meta.draw_truncation as i64,

            seed_base: meta.seed_base.map(|s| s as i64),
            pairs_parallel: meta.pairs_parallel,
            pair_seed_refresh: meta.pair_seed_refresh,
            games: meta
                .games
                .iter()
                .map(|g| crate::types::EpisodeGameRow {
                    game_index: g.game_index as i64,
                    pair_index: g.pair_index as i64,
                    pair_position: g.pair_position as i64,
                    game_seed: g.game_seed.map(|s| s as i64),
                    a_is_red: g.a_is_red,
                    seed_refreshed: g.seed_refreshed,
                    winner: g.winner.map(|w| w as i64),
                    steps: g.steps as i64,
                })
                .collect(),
        };
        self.ports.episodes.insert_episode(draft)?;
        tracing::info!(
            "[episode] worker={} task={} network={} opponent={}{} match={} games={} steps={} -> {}",
            meta.worker_id,
            meta.task_id,
            meta.network_sha,
            task.opponent_spec,
            task.opponent_sha,
            task.match_id,
            meta.game_count,
            meta.total_steps,
            key
        );

        self.disp.mark_done(&meta.task_id);

        if let Err(e) = self
            .jobs
            .record_report(&meta.task_id, meta.game_count as i64)
        {
            tracing::warn!("[job] ⚠️ episode 上报聚合失败 task={}: {e}", meta.task_id);
        }
        Ok(EpisodeAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
    }

    pub fn report_match_result(&self, r: MatchResult) -> CoreResult<MatchResultAck> {
        self.router.route(&r)
    }
}
