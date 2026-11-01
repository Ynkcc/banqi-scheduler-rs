use bqs_proto::{
    CollectPlayerSpec, CollectSearchParams, CollectionJobSpec, DataKind, GenerationSpec, Plays,
    SelfPlayParams, TaskRequest, TaskResponse,
};

use crate::{
    featurepack::PendingDerive, reanalysis::PendingReanalysis, types::Network, CoreError,
    CoreResult,
};

use super::{
    data_kind_for_format, producer::network_key, stack_data_kind_for_weight_format, TaskAssembler,
};

pub fn player_kind_for_format(format: &str) -> &'static str {
    match format {
        "nnue" => "expectimax_nnue",
        _ => "mcts_resnet",
    }
}

fn model_player(sha: &str, format: &str, search: Option<CollectSearchParams>) -> CollectPlayerSpec {
    CollectPlayerSpec {
        kind: player_kind_for_format(format).to_string(),
        network_sha: sha.to_string(),
        search,
    }
}

fn rule_player(kind: &str) -> CollectPlayerSpec {
    CollectPlayerSpec {
        kind: kind.to_string(),
        network_sha: String::new(),
        search: None,
    }
}

impl TaskAssembler {
    pub fn selfplay_task(
        &self,
        task_id: &str,
        req: &TaskRequest,
        producer: &Network,
        rule_opponent: &str,
        search: Option<CollectSearchParams>,
        games: i64,
    ) -> CoreResult<TaskResponse> {
        let key = network_key(&producer.sha, &producer.format);
        let data_kind = data_kind_for_format(&producer.format);

        let search = Some(search.unwrap_or_else(|| self.default_search()));
        let player_a = model_player(&producer.sha, &producer.format, search.clone());
        let player_b = if rule_opponent.is_empty() {
            player_a.clone()
        } else {
            rule_player(rule_opponent)
        };
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            network_sha: producer.sha.clone(),
            network_sha_remote: producer.sha.clone(),
            network_key: key.clone(),
            params: Some(SelfPlayParams {
                extra_config: self.extra_config(),
                ..Default::default()
            }),
            generation: Some(GenerationSpec {
                variant: self.cfg.selfplay.variant.clone(),
                data_kind,
                games: games as u64,
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(Plays {
                    player_a: Some(player_a),
                    player_b: Some(player_b),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.with_network_download(req, &mut resp, &producer.sha, &key)?;
        if rule_opponent.is_empty() {
            resp.opponent_sha = producer.sha.clone();
            resp.opponent_key = key.clone();
            match self.signer.presign_get(&key, self.ttl_secs()) {
                Ok(url) => resp.opponent_url = url,
                Err(e) => tracing::warn!(
                    "[task] presign opponent {} 失败（本地有缓存的 worker 不受影响）: {e}",
                    key
                ),
            }
        }
        Ok(resp)
    }

    pub fn gatekeeper_task(
        &self,
        task_id: &str,
        req: &TaskRequest,
        candidate: &Network,
        opponent_sha: &str,
        opponent: Option<&Network>,
        games: i64,
    ) -> CoreResult<TaskResponse> {
        let key = network_key(&candidate.sha, &candidate.format);
        let data_kind = data_kind_for_format(&candidate.format);
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            network_sha: candidate.sha.clone(),
            network_sha_remote: candidate.sha.clone(),
            network_key: key.clone(),
            generation: Some(GenerationSpec {
                variant: self.cfg.selfplay.variant.clone(),
                data_kind,
                games: games as u64,
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(Plays {
                    player_a: Some(model_player(
                        &candidate.sha,
                        &candidate.format,
                        Some(self.default_search()),
                    )),
                    player_b: Some(model_player(
                        opponent_sha,
                        "nnue",
                        Some(self.default_search()),
                    )),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.with_network_download(req, &mut resp, &candidate.sha, &key)?;
        if let Some(opp) = opponent {
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

    pub fn sentinel_task(&self, task_id: &str, req: &TaskRequest) -> TaskResponse {
        let games = self.games_for(req.threads);
        TaskResponse {
            task_id: task_id.to_string(),
            params: Some(SelfPlayParams {
                extra_config: self.extra_config(),
                ..Default::default()
            }),
            generation: Some(GenerationSpec {
                variant: self.cfg.selfplay.variant.clone(),
                data_kind: DataKind::DataNnue as i32,
                games: games as u64,
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(Plays {
                    player_a: Some(rule_player("rule:capture_first")),
                    player_b: Some(rule_player("rule:capture_first")),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn reanalysis_task(
        &self,
        task_id: &str,
        item: &PendingReanalysis,
        req: &TaskRequest,
    ) -> CoreResult<TaskResponse> {
        let best = self
            .nets
            .get_best()?
            .ok_or_else(|| CoreError::Internal("no best network registered".into()))?;
        let data_kind = stack_data_kind_for_weight_format(&best.format)
            .map_err(|e| CoreError::Internal(format!("best 网络 format 无法分派重搜栈: {e}")))?;
        let key = network_key(&best.sha, &best.format);

        let mut search = self.default_search();
        if item.mcts_sims > 0 {
            search.mcts_sims = item.mcts_sims as u32;
            search.fast_mcts_sims = 0;
        }
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            network_sha: best.sha.clone(),
            network_sha_remote: best.sha.clone(),
            network_key: key.clone(),
            params: Some(SelfPlayParams {
                mcts_sims: item.mcts_sims,
                search: Some(search),
                ..Default::default()
            }),
            generation: Some(GenerationSpec {
                variant: self.cfg.selfplay.variant.clone(),
                data_kind,
                games: item.positions as u64,
                behavior: Some(bqs_proto::generation_spec::Behavior::Reanalysis(
                    bqs_proto::Reanalysis {
                        payload: item.payload.clone(),
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.with_network_download(req, &mut resp, &best.sha, &key)?;
        Ok(resp)
    }

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
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(Plays {
                    player_a,
                    player_b,
                })),
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

    pub fn rating_task(
        &self,
        task_id: &str,
        variant: &str,
        side_a: &bqs_proto::CollectPlayerSpec,
        side_b: &bqs_proto::CollectPlayerSpec,
        a_net: Option<&Network>,
        b_net: Option<&Network>,
        games: i64,
        start_fens: Vec<String>,
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
                data_kind,
                games: games as u64,
                behavior: Some(bqs_proto::generation_spec::Behavior::Plays(Plays {
                    player_a: Some(self.player_with_search(side_a.clone())),
                    player_b: Some(self.player_with_search(side_b.clone())),
                })),
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
