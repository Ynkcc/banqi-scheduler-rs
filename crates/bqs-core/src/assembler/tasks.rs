use bqs_proto::{CollectionJobSpec, DataKind, SelfPlayParams, TaskKind, TaskRequest, TaskResponse};

use crate::{
    CoreError, CoreResult, consts,
    evalcoord::PendingEval,
    featurepack::PendingDerive,
    reanalysis::PendingReanalysis,
    types::Network,
};

use super::{TaskAssembler, data_kind_for_format, producer::{ProducerSelect, SelfplayMode, network_key}, stack_data_kind_for_weight_format};

impl TaskAssembler {
    pub fn selfplay_task(
        &self,
        task_id: &str,
        req: &TaskRequest,
        sel: &ProducerSelect,
    ) -> CoreResult<(TaskResponse, i64)> {
        let producer = sel.producer.as_ref().ok_or_else(|| {
            CoreError::Internal("selfplay_task: producer 缺失（应先检查 mode != None）".into())
        })?;
        let key = network_key(&producer.sha, &producer.format);

        let data_kind = data_kind_for_format(&producer.format);
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskSelfplay as i32,
            network_sha: producer.sha.clone(),
            network_sha_remote: producer.sha.clone(),
            games: self.games_for(req.threads) as i32,
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                extra_config: self.extra_config(),
                data_kind,
                ..Default::default()
            }),
            network_key: key.clone(),
            ..Default::default()
        };

        let mut match_id = 0i64;
        if sel.mode == SelfplayMode::Mix {
            if let Some(m) = &sel.m {
                if producer.sha != m.opponent {
                    let opp = self.nets.get_network(&m.opponent)?;
                    if let Some(opp) = opp {
                        match_id = m.id;
                        resp.opponent_sha = m.opponent.clone();
                        resp.opponent_key = network_key(&opp.sha, &opp.format);

                        match self.signer.presign_get(&resp.opponent_key, self.ttl_secs()) {
                            Ok(url) => resp.opponent_url = url,
                            Err(e) => tracing::warn!(
                                "[task] presign opponent {} 失败（本地有缓存的 worker 不受影响）: {e}",
                                resp.opponent_key
                            ),
                        }
                    }
                }
            }
        } else if !self.cfg.selfplay.train_match_opponent.is_empty() {
            resp.opponent_spec = self.cfg.selfplay.train_match_opponent.clone();
        }
        self.with_network_download(req, &mut resp, &producer.sha, &key)?;
        Ok((resp, match_id))
    }

    pub fn sentinel_task(&self, task_id: &str, req: &TaskRequest) -> TaskResponse {
        TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskSelfplay as i32,
            network_sha: consts::RULE_SENTINEL_SHA.to_string(),
            network_sha_remote: consts::RULE_SENTINEL_SHA.to_string(),
            games: self.games_for(req.threads) as i32,
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                extra_config: self.extra_config(),
                data_kind: DataKind::DataNnue as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn eval_task(
        &self,
        task_id: &str,
        job: &PendingEval,
        games: i64,
        req: &TaskRequest,
    ) -> CoreResult<TaskResponse> {
        let net = self.nets.get_network(&job.network_sha)?.ok_or_else(|| {
            CoreError::Internal(format!("eval network not found sha={}", job.network_sha))
        })?;
        let key = network_key(&net.sha, &net.format);
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskEval as i32,
            network_sha: job.network_sha.clone(),
            network_sha_remote: job.network_sha.clone(),
            opponent_spec: job.opponent_spec.clone(),
            games: games as i32,
            network_key: key.clone(),
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),

                mcts_sims: self.cfg.eval.mcts_sims,

                data_kind: DataKind::DataResnet as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        self.with_network_download(req, &mut resp, &job.network_sha, &key)?;

        if !job.opponent_sha.is_empty() {
            let opp = self.nets.get_network(&job.opponent_sha)?.ok_or_else(|| {
                CoreError::Internal(format!(
                    "eval opponent network not found sha={}",
                    job.opponent_sha
                ))
            })?;
            let opp_key = network_key(&opp.sha, &opp.format);
            resp.opponent_sha = opp.sha.clone();
            resp.opponent_key = opp_key.clone();
            match self.signer.presign_get(&opp_key, self.ttl_secs()) {
                Ok(url) => resp.opponent_url = url,
                Err(e) => tracing::warn!("预签名对手 URL 失败 key={opp_key}: {e}"),
            }
        }
        Ok(resp)
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
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskReanalysis as i32,
            network_sha: best.sha.clone(),
            network_sha_remote: best.sha.clone(),
            games: item.positions as i32,
            network_key: key.clone(),
            reanalysis_payload: item.payload.clone(),
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                mcts_sims: item.mcts_sims,

                data_kind,
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
        spec: &CollectionJobSpec,
        games: u64,
        seed: u64,
    ) -> TaskResponse {
        TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskCollect as i32,
            games: games as i32,
            collect_spec: Some(spec.clone()),
            collect_games: games,
            collect_seed: seed,
            params: Some(SelfPlayParams {
                variant: spec.variant.clone(),
                extra_config: self.extra_config(),
                data_kind: spec.data_kind,
                ..Default::default()
            }),
            ..Default::default()
        }
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
            kind: TaskKind::TaskRating as i32,
            network_sha: a_sha.clone(),
            network_sha_remote: a_sha.clone(),
            games: games as i32,
            network_key: a_key.clone(),
            rating_side_a: Some(side_a.clone()),
            rating_side_b: Some(side_b.clone()),
            params: Some(SelfPlayParams {
                variant: variant.to_string(),
                data_kind,
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
            kind: TaskKind::TaskDeriveFeatures as i32,
            episode_object_key: item.episode_object_key.clone(),
            episode_url,
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}
