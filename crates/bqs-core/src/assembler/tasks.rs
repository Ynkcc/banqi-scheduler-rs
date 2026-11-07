use bq_record::DataKind;
use bqs_proto::{CollectionJobSpec, GenerationSpec, SelfPlayParams, TaskRequest, TaskResponse};

use crate::{featurepack::PendingDerive, types::Network, CoreResult};

use super::{data_kind_for_format, producer::network_key, TaskAssembler};

impl TaskAssembler {
    pub fn collect_task(
        &self,
        task_id: &str,
        req: &TaskRequest,
        spec: &CollectionJobSpec,
        games: u64,
        seed: u64,
        deadline_unix: u64,
        a_net: Option<&Network>,
        b_net: Option<&Network>,
    ) -> CoreResult<TaskResponse> {
        let player_a = spec.player_a.clone().map(|p| self.player_with_search(p));
        let player_b = spec.player_b.clone().map(|p| self.player_with_search(p));
        let (a_sha, a_key) = match a_net {
            Some(n) => (n.sha.clone(), network_key(&n.sha, &n.format)),
            None => (String::new(), String::new()),
        };
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            network_sha: a_sha.clone(),
            network_sha_remote: a_sha.clone(),
            network_key: a_key.clone(),
            params: Some(SelfPlayParams {
                extra_config: self.extra_config(),
                ..Default::default()
            }),
            generation: Some(GenerationSpec {
                variant: spec.variant.clone(),
                scenario: spec.scenario,
                seed: Some(seed),
                data_kind: spec.data_kind,
                games,
                deadline_unix,

                pairs_parallel: spec.pairs_parallel,
                pair_seed_refresh: spec.pair_seed_refresh,
                start_fens: spec.start_fens.clone(),
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(
                    bqs_proto::Plays { player_a, player_b },
                )),
            }),
            ..Default::default()
        };
        if let Some(a) = a_net {
            self.with_network_download(req, &mut resp, &a.sha, &a_key)?;
        }
        if let Some(opp) = b_net {
            let opp_key = network_key(&opp.sha, &opp.format);
            resp.opponent_sha = opp.sha.clone();
            resp.opponent_key = opp_key.clone();
            match self.signer.presign_get(&opp_key, self.ttl_secs()) {
                Ok(url) => resp.opponent_url = url,
                Err(e) => tracing::warn!(
                    "[task] presign opponent {} 失败（本地有缓存的 worker 不受影响）: {e}",
                    opp_key
                ),
            }
        }
        Ok(resp)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rating_task(
        &self,
        task_id: &str,
        variant: &str,
        side_a: &bqs_proto::CollectPlayerSpec,
        side_b: &bqs_proto::CollectPlayerSpec,
        a_net: Option<&Network>,
        b_net: Option<&Network>,
        games: i64,
        seed: u64,
        start_fens: Vec<String>,
        scenario: i32,
        req: &TaskRequest,
    ) -> CoreResult<TaskResponse> {
        let data_kind = a_net
            .or(b_net)
            .map(|n| data_kind_for_format(&n.format))
            .unwrap_or(DataKind::DataResnet as i32);
        let (a_sha, a_key) = match a_net {
            Some(n) => (n.sha.clone(), network_key(&n.sha, &n.format)),
            None => (String::new(), String::new()),
        };
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            network_sha: a_sha.clone(),
            network_sha_remote: a_sha.clone(),
            network_key: a_key.clone(),
            generation: Some(GenerationSpec {
                variant: variant.to_string(),
                scenario,
                data_kind,
                games: games as u64,

                seed: Some(seed),

                pairs_parallel: Some(true),
                pair_seed_refresh: Some(false),
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(
                    bqs_proto::Plays {
                        player_a: Some(self.player_with_search(side_a.clone())),
                        player_b: Some(self.player_with_search(side_b.clone())),
                    },
                )),
                start_fens,
                ..Default::default()
            }),
            ..Default::default()
        };
        if let Some(a) = a_net {
            self.with_network_download(req, &mut resp, &a.sha, &a_key)?;
        }
        if let Some(opp) = b_net {
            let opp_key = network_key(&opp.sha, &opp.format);
            resp.opponent_sha = opp.sha.clone();
            resp.opponent_key = opp_key.clone();
            match self.signer.presign_get(&opp_key, self.ttl_secs()) {
                Ok(url) => resp.opponent_url = url,
                Err(e) => tracing::warn!(
                    "[task] presign opponent {} 失败（本地有缓存的 worker 不受影响）: {e}",
                    opp_key
                ),
            }
        }
        Ok(resp)
    }

    pub fn derive_task(&self, task_id: &str, item: &PendingDerive) -> CoreResult<TaskResponse> {
        let episode_url = self
            .signer
            .presign_get(&item.episode_object_key, self.ttl_secs())?;
        Ok(TaskResponse {
            task_id: task_id.to_string(),
            generation: Some(GenerationSpec {
                variant: self.cfg.selfplay.variant.clone(),
                behavior: Some(bqs_proto::generation_spec::Behavior::Derive(
                    bqs_proto::DeriveFeatures {
                        episode_object_key: item.episode_object_key.clone(),
                        episode_url,
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}
