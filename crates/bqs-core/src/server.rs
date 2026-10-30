use std::collections::HashMap;
use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::*;

use crate::{
    CoreError, CoreResult,
    assembler::{TaskAssembler, episode_key, network_key},
    balance::{BalanceMonitor, TrainerHeartbeatInput},
    control::Control,
    dispatch::TaskDispatcher,
    evalcoord::EvalCoordinator,
    featurepack::{DEFAULT_FEATURE_PACK_VERSION, DeriveQueue, PendingDerive, feature_pack_key},
    gatekeeper::Gatekeeper,
    jobs::{JobManager, JobSpecJson},
    ports::FeaturePackDraft,
    ports::{Clock, ObjectSigner, Ports},
    reanalysis::{PendingReanalysis, ReanaQueue},
    reports::{ReportRouter, lookup_report_task},
    sources::Sources,
    types::EpisodeDraft,
    variants,
};

pub struct Server {
    pub cfg: Arc<Config>,

    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,

    pub disp: Arc<TaskDispatcher>,
    pub evalc: Arc<EvalCoordinator>,
    pub gate: Arc<Gatekeeper>,
    pub balance: Arc<BalanceMonitor>,
    pub reana: Arc<ReanaQueue>,
    pub derive: Arc<DeriveQueue>,
    pub jobs: Arc<JobManager>,
    pub ctl: Arc<Control>,

    pub asm: Arc<TaskAssembler>,

    pub router: Arc<ReportRouter>,

    pub sources: Vec<Box<dyn crate::sources::TaskSource>>,
}

pub struct ServerDeps {
    pub cfg: Config,
    pub ports: Ports,
    pub signer: Arc<dyn ObjectSigner>,
    pub clock: Arc<dyn Clock>,
}

impl Server {
    pub fn assemble(deps: ServerDeps) -> Arc<Self> {
        let cfg = Arc::new(deps.cfg);
        let ports = deps.ports;
        let signer = deps.signer;
        let clock = deps.clock;

        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), clock.clone()));
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        let evalc = Arc::new(EvalCoordinator::new(
            &ports,
            cfg.clone(),
            clock.clone(),
            ctl.clone(),
            disp.clone(),
        ));
        let gate = Arc::new(Gatekeeper::new(&ports, cfg.clone(), evalc.clone()));
        let balance = Arc::new(BalanceMonitor::new(
            &ports,
            cfg.clone(),
            clock.clone(),
            ctl.clone(),
        ));
        let reana = Arc::new(ReanaQueue::new(&ports, cfg.clone(), clock.clone()));
        let derive = Arc::new(DeriveQueue::new(&ports, cfg.clone(), clock.clone()));
        let jobs = Arc::new(JobManager::new(
            ports.jobs.clone(),
            disp.clone(),
            ports.networks.clone(),
            ports.matches.clone(),
            ctl.clone(),
            cfg.clone(),
            clock.clone(),
        ));

        evalc.set_job_hook(&jobs);

        let asm = Arc::new(TaskAssembler::new(
            ports.networks.clone(),
            signer.clone(),
            cfg.clone(),
            ctl.clone(),
        ));
        let router = Arc::new(ReportRouter::new(
            evalc.clone(),
            gate.clone(),
            disp.clone(),
            jobs.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            cfg.clone(),
        ));
        let sources = Sources::assemble(
            jobs.clone(),
            evalc.clone(),
            reana.clone(),
            derive.clone(),
            ports.matches.clone(),
            ports.networks.clone(),
            disp.clone(),
            asm.clone(),
            ctl.clone(),
            cfg.clone(),
        );

        evalc.ensure_best_evaluated();

        if let Err(e) = jobs.ensure_selfplay_job() {
            panic!("[jobs] selfplay job 引导失败，拒绝启动: {e}");
        }

        Arc::new(Self {
            cfg,
            ports,
            signer,
            clock,
            disp,
            evalc,
            gate,
            balance,
            reana,
            derive,
            jobs,
            ctl,
            asm,
            router,
            sources,
        })
    }

    fn ttl_secs(&self) -> i64 {
        crate::consts::PRESIGN_TTL.as_secs() as i64
    }

    pub fn get_info(&self, _req: GetInfoRequest) -> CoreResult<GetInfoReply> {
        let variant = &self.cfg.selfplay.variant;
        let meta = variants::lookup(variant).ok_or_else(|| {
            CoreError::Internal(format!(
                "变体 {variant} 未在 bq-env 变体表登记（应启动即失败）"
            ))
        })?;
        let (should_stop, stop_reason) = self.ctl.should_stop();
        Ok(GetInfoReply {
            variant: variant.clone(),
            variant_meta: Some(VariantMeta {
                id: meta.id.clone(),
                label: meta.label.clone(),
                rows: meta.rows,
                cols: meta.cols,
                piece_counts: meta.piece_counts.clone(),
                piece_values: meta.piece_values.clone(),
                initial_health: meta.initial_health,
                max_consecutive_moves_for_draw: meta.max_consecutive_moves_for_draw,
                symmetries: meta.symmetries.clone(),
            }),
            should_stop,
            stop_reason,
        })
    }

    pub fn get_task(&self, req: TaskRequest) -> CoreResult<TaskResponse> {
        self.disp.prune();

        let none = |message: &str| TaskResponse {
            kind: TaskKind::TaskNone as i32,
            message: message.to_string(),
            ..Default::default()
        };

        let min = &self.cfg.server.min_client_version;
        if !min.is_empty()
            && !req.client_version.is_empty()
            && client_version_too_old(&req.client_version, min)
        {
            return Ok(none("client_version_too_old"));
        }
        if !req.client_version.is_empty() {
            tracing::info!(
                worker = %req.worker_id, version = %req.client_version,
                threads = req.threads, memory_mb = req.memory_mb, "[task] GetTask"
            );
        }

        self.balance.evaluate(self.clock.now());

        for src in &self.sources {
            if let Some(resp) = src.try_task(&req)? {
                return Ok(resp);
            }
        }

        if self.ctl.manual_paused() {
            return Ok(none("self_play_paused"));
        }

        Ok(none("no_task_available"))
    }

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

    pub fn heartbeat(&self, req: HeartbeatRequest) -> CoreResult<HeartbeatReply> {
        self.ports.workers.touch_worker(
            &req.worker_id,
            req.current_threads as i64,
            req.completed_games as i64,
            &req.client_version,
            req.memory_mb,
            &req.health_status,
            &req.last_error,
        )?;
        if !req.client_version.is_empty() {
            if let Ok(Some(v)) = self.ports.workers.worker_version(&req.worker_id) {
                if !v.is_empty() && v != req.client_version {
                    tracing::info!(
                        "[worker] {} version changed {} -> {}",
                        req.worker_id,
                        v,
                        req.client_version
                    );
                }
            }
        }
        let best = self.ports.networks.get_best()?;

        let mut reply = HeartbeatReply {
            pause_self_play: self.ctl.effective_paused(),
            ..Default::default()
        };
        if let Some(best) = best {
            reply.best_network = best.sha;
        }

        reply.cancel_task_ids = self.jobs.cancel_task_ids_for_worker(&req.worker_id);
        if !reply.cancel_task_ids.is_empty() {
            tracing::info!(
                "[job] 心跳下发在途取消 worker={} tasks={:?}",
                req.worker_id,
                reply.cancel_task_ids
            );
        }
        Ok(reply)
    }

    pub fn submit_job(&self, req: SubmitJobRequest) -> CoreResult<SubmitJobReply> {
        let (accepted, message, job_id) = self.jobs.submit(&req)?;
        if accepted && req.kind == JobKind::JobEval as i32 {
            if let Some(e) = &req.eval {
                let sha = if e.network_sha.is_empty() {
                    self.ports
                        .networks
                        .get_best()?
                        .map(|b| b.sha)
                        .unwrap_or_default()
                } else {
                    e.network_sha.clone()
                };
                if let Err(err) =
                    self.evalc
                        .ensure_pending(&sha, &e.opponent_kind, &e.opponent_network_sha)
                {
                    self.jobs.control(&job_id, "cancel")?;
                    return Err(err);
                }
            }
        }
        Ok(SubmitJobReply {
            accepted,
            message,
            job_id,
        })
    }

    pub fn list_jobs(&self, req: ListJobsRequest) -> CoreResult<ListJobsReply> {
        let kind = req.kind_filter.map(|k| k as i64);
        let rows = self.jobs.list(
            if req.state_filter.is_empty() {
                None
            } else {
                Some(req.state_filter.as_str())
            },
            kind,
        )?;
        let jobs = rows.iter().map(|r| self.job_status(r)).collect();
        Ok(ListJobsReply { jobs })
    }

    pub fn control_job(&self, req: JobControlRequest) -> CoreResult<JobControlReply> {
        let (accepted, message, state) = self.jobs.control(&req.job_id, &req.control)?;
        Ok(JobControlReply {
            accepted,
            message,
            state,
        })
    }

    fn job_status(&self, r: &crate::ports::JobRow) -> JobStatus {
        let kind = JobKind::try_from(r.kind as i32).unwrap_or(JobKind::JobCollect);
        let (collect, selfplay, eval, rating) = match JobSpecJson::parse(&r.spec_json) {
            Some(JobSpecJson::Collect(c)) => (Some(c.to_proto()), None, None, None),
            Some(JobSpecJson::SelfPlay(s)) => (None, Some(s.to_proto()), None, None),
            Some(JobSpecJson::Eval(e)) => (None, None, Some(e.to_proto()), None),
            Some(JobSpecJson::Rating(rt)) => (None, None, None, Some(rt.to_proto())),
            None => (None, None, None, None),
        };
        JobStatus {
            job_id: r.job_id.clone(),
            kind: kind as i32,
            priority: r.priority as u32,
            state: r.state.clone(),
            total_games: r.total_games as u64,
            completed_games: r.completed_games as u64,
            dispatched_games: r.dispatched_games as u64,
            created_at: r.created_at,
            updated_at: r.updated_at,
            collect,
            selfplay,
            eval,
            rating,
        }
    }

    pub fn get_network(&self, req: NetworkRequest) -> CoreResult<NetworkInfo> {
        let n = if req.sha.is_empty() {
            self.ports.networks.get_best()?
        } else {
            self.ports.networks.get_network(&req.sha)?
        };
        let Some(n) = n else {
            return Ok(NetworkInfo {
                sha: req.sha,
                ..Default::default()
            });
        };
        let key = network_key(&n.sha, &n.format);
        let url = self.signer.presign_get(&key, self.ttl_secs())?;
        Ok(NetworkInfo {
            sha: n.sha.clone(),
            download_url: url,
            created_at: n.created_at,
            is_best: n.is_best,
            game_data:
                serde_json::json!({ "parent_sha": n.parent_sha.clone().unwrap_or_default() })
                    .to_string(),
            key,
        })
    }

    pub fn register_network(&self, req: RegisterNetworkRequest) -> CoreResult<RegisterNetworkAck> {
        let ack = |accepted: bool, message: String| RegisterNetworkAck {
            accepted,
            message,
            match_task_hint: String::new(),
        };
        let best = self.ports.networks.get_best()?;
        if best.as_ref().is_some_and(|b| b.sha == req.sha) {
            return Ok(ack(false, "sha_equals_current_best".into()));
        }
        let format = match normalize_format(&req.format) {
            Ok(f) => f,
            Err(msg) => return Ok(ack(false, msg)),
        };
        let created = self.ports.networks.register_network(
            &req.sha,
            (!req.parent_sha.is_empty()).then_some(req.parent_sha.as_str()),
            (!req.notes.is_empty()).then_some(req.notes.as_str()),
            &format,
        )?;
        if !created {
            return Ok(ack(false, format!("duplicate_sha:{}", req.sha)));
        }
        match best {
            None => {
                self.ports.networks.promote_best(&req.sha)?;
                tracing::info!("[network] first network promoted sha={}", req.sha);

                self.evalc.on_new_best(&req.sha)?;
                Ok(ack(true, "first_network_promoted".into()))
            }
            Some(best) => {
                let match_id = self.ports.matches.create_match(
                    &req.sha,
                    &best.sha,
                    self.cfg.selfplay.gatekeeper_games as i64,
                )?;
                tracing::info!(
                    "[gatekeeper] match={} created candidate={} vs best={} target_pairs={}",
                    match_id,
                    req.sha,
                    best.sha,
                    self.cfg.selfplay.gatekeeper_games
                );
                let mut a = ack(true, String::new());
                a.match_task_hint = format!("gatekeeper match {match_id} vs {}", best.sha);
                Ok(a)
            }
        }
    }

    pub fn sign_network_upload(
        &self,
        req: SignNetworkUploadRequest,
    ) -> CoreResult<SignNetworkUploadAck> {
        let ack = |accepted: bool, message: String| SignNetworkUploadAck {
            accepted,
            message,
            upload_url: String::new(),
            object_key: String::new(),
        };
        let sha = req.sha.trim();
        if sha.len() != 64 {
            return Ok(ack(
                false,
                format!("invalid_sha_len={} (want 64 hex chars)", sha.len()),
            ));
        }
        let format = match normalize_format(&req.format) {
            Ok(f) => f,
            Err(msg) => return Ok(ack(false, msg)),
        };
        let key = network_key(sha, &format);
        let url = self.signer.presign_put(&key, self.ttl_secs())?;
        tracing::info!(
            "[network] sign upload trainer={} key={} len={}",
            req.trainer_id,
            key,
            req.content_length
        );
        Ok(SignNetworkUploadAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
    }

    pub fn list_episodes(&self, req: ListEpisodesRequest) -> CoreResult<ListEpisodesReply> {
        let kind = req.kind.map(|k| k as i64);
        let keys = self
            .ports
            .episodes
            .list_episode_keys(&req.after_key, req.limit as i64, kind)?;
        let mut objects = Vec::with_capacity(keys.len());
        for k in &keys {
            let url = self.signer.presign_get(k, self.ttl_secs())?;
            objects.push(EpisodeObject {
                object_key: k.clone(),
                download_url: url,
                size: 0,
            });
        }

        let has_more = !keys.is_empty() && keys.len() as i32 == req.limit && req.limit > 0;
        Ok(ListEpisodesReply { objects, has_more })
    }

    pub fn submit_reanalysis(
        &self,
        req: SubmitReanalysisRequest,
    ) -> CoreResult<SubmitReanalysisReply> {
        let reply = |accepted: bool, message: String, queued: u64| SubmitReanalysisReply {
            accepted,
            message,
            queued_positions: queued,
        };
        if !self.reana.enabled() {
            return Ok(reply(
                false,
                "服务端未启用局面重搜（配置 reanalysis.interval_tasks = 0）".into(),
                0,
            ));
        }
        if req.variant != self.cfg.selfplay.variant {
            return Ok(reply(
                false,
                format!(
                    "变体不一致：请求 \"{}\"，服务端 \"{}\"（拒绝跨变体重搜）",
                    req.variant, self.cfg.selfplay.variant
                ),
                0,
            ));
        }
        if req.payload.is_empty() {
            return Ok(reply(false, "重搜载荷为空".into(), 0));
        }
        let item = PendingReanalysis {
            variant: req.variant,
            mcts_sims: req.mcts_sims,
            payload: req.payload,
            positions: req.positions,
            requester: req.requester.clone(),
            created_at: self.clock.now(),
        };
        let (queued, ok) = self.reana.enqueue(item);
        if !ok {
            return Ok(reply(
                false,
                format!(
                    "重搜队列已满（上限 {} 批），本次丢弃：请降低提交频率或提高配置 reanalysis.max_queue",
                    self.cfg.reanalysis.max_queue
                ),
                queued,
            ));
        }
        tracing::info!(
            "[reanalysis] 入队 requester={} positions={} sims={} 队内待重搜位置={}",
            req.requester,
            req.positions,
            req.mcts_sims,
            queued
        );
        Ok(reply(true, String::new(), queued))
    }

    pub fn report_feature_pack(&self, req: FeaturePackMeta) -> CoreResult<FeaturePackAck> {
        let ack = |accepted: bool, message: String| FeaturePackAck {
            accepted,
            message,
            upload_url: String::new(),
            object_key: String::new(),
        };
        let mut version = req.pack_version as i64;
        if version <= 0 {
            version = DEFAULT_FEATURE_PACK_VERSION;
        }
        if req.episode_object_key.is_empty() {
            return Ok(ack(false, "missing_episode_object_key".into()));
        }
        let derived = feature_pack_key(&req.episode_object_key, version);

        let key = match self
            .ports
            .feature_packs
            .get_feature_pack(&req.episode_object_key, version)?
        {
            Some(existing) => existing.object_key,
            None => {
                self.ports
                    .feature_packs
                    .insert_feature_pack(FeaturePackDraft {
                        episode_object_key: req.episode_object_key.clone(),
                        pack_version: version,
                        object_key: derived.clone(),
                        created_at: self.clock.now(),
                    })?;
                derived
            }
        };
        let url = self.signer.presign_put(&key, self.ttl_secs())?;

        self.disp.mark_done(&req.task_id);
        tracing::info!(
            "[feature-pack] worker={} task={} episode={} pack_v{} -> {}",
            req.worker_id,
            req.task_id,
            req.episode_object_key,
            version,
            key
        );
        Ok(FeaturePackAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
    }

    pub fn request_feature_pack(&self, req: FeaturePackRequest) -> CoreResult<FeaturePackReply> {
        let mut version = req.pack_version as i64;
        if version <= 0 {
            version = DEFAULT_FEATURE_PACK_VERSION;
        }
        if req.episode_object_key.is_empty() {
            return Ok(FeaturePackReply {
                accepted: false,
                message: "missing_episode_object_key".into(),
                ..Default::default()
            });
        }
        if let Some(existing) = self
            .ports
            .feature_packs
            .get_feature_pack(&req.episode_object_key, version)?
        {
            let url = self
                .signer
                .presign_get(&existing.object_key, self.ttl_secs())?;
            return Ok(FeaturePackReply {
                accepted: true,
                ready: true,
                object_key: existing.object_key,
                download_url: url,
                ..Default::default()
            });
        }
        let ok = self.derive.enqueue(PendingDerive {
            episode_object_key: req.episode_object_key.clone(),
            pack_version: version,
            requester: req.requester.clone(),
            created_at: self.clock.now(),
        });
        if !ok {
            return Ok(FeaturePackReply {
                accepted: false,
                message: format!(
                    "特征包补派队列已满（上限 {}），稍后重试",
                    crate::consts::DERIVE_QUEUE_MAX
                ),
                ..Default::default()
            });
        }
        tracing::info!(
            "[feature-pack] 补派入队 requester={} episode={} pack_v{} 队列={}",
            req.requester,
            req.episode_object_key,
            version,
            self.derive.pending()
        );
        Ok(FeaturePackReply {
            accepted: true,
            ready: false,
            message: "queued_for_derivation".into(),
            ..Default::default()
        })
    }

    pub fn get_train_config(&self, req: GetTrainConfigRequest) -> CoreResult<GetTrainConfigReply> {
        if req.variant != self.cfg.selfplay.variant {
            return Ok(GetTrainConfigReply {
                accepted: false,
                message: format!(
                    "变体不符：请求 \"{}\"，服务端 \"{}\"",
                    req.variant, self.cfg.selfplay.variant
                ),
                ..Default::default()
            });
        }

        let overrides: HashMap<String, String> =
            self.ctl.train_config_overrides().into_iter().collect();
        Ok(GetTrainConfigReply {
            accepted: true,
            overrides,
            ..Default::default()
        })
    }

    pub fn trainer_heartbeat(
        &self,
        req: TrainerHeartbeatRequest,
    ) -> CoreResult<TrainerHeartbeatReply> {
        let outcome = self
            .balance
            .register_trainer_heartbeat(TrainerHeartbeatInput {
                trainer_id: req.trainer_id,
                variant: req.variant,
                kind: req.kind as i64,
                consumed_key: req.consumed_key,
                samples_consumed: req.samples_consumed,
                backlog: req.backlog as i64,
                buffered: req.buffered as i64,
                health_status: req.health_status,
                last_error: req.last_error,
                progress_json: req.progress_json,
            })?;
        Ok(TrainerHeartbeatReply {
            pause_self_play: self.ctl.effective_paused(),
            auto_paused: self.ctl.auto_paused(),
            auto_pause_reason: self.ctl.auto_pause_reason(),
            message: outcome.message,
        })
    }
}

fn client_version_too_old(client: &str, min: &str) -> bool {
    let parse = |s: &str| -> Option<Vec<Option<u64>>> {
        let mut parts = Vec::new();
        for seg in s.split('.') {
            match seg.parse::<u64>() {
                Ok(n) => parts.push(Some(n)),
                Err(_) => return None,
            }
        }
        Some(parts)
    };
    match (parse(client), parse(min)) {
        (Some(c), Some(m)) => {
            let len = c.len().max(m.len());
            for i in 0..len {
                let cv = c.get(i).copied().flatten().unwrap_or(0);
                let mv = m.get(i).copied().flatten().unwrap_or(0);
                if cv != mv {
                    return cv < mv;
                }
            }
            false
        }

        _ => client < min,
    }
}

fn normalize_format(format: &str) -> Result<String, String> {
    let f = format.trim().to_ascii_lowercase();
    if crate::consts::WEIGHT_FORMATS.contains(&f.as_str()) {
        Ok(f)
    } else {
        Err(format!(
            "unsupported_format=\"{format}\" (want onnx/pt/nnue)"
        ))
    }
}

#[cfg(test)]
mod tests;
