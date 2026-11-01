use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{CollectSearchParams, TaskRequest, TaskResponse};

use crate::{
    assembler::{games_for_threads, SelfplayMode, TaskAssembler},
    control::Control,
    dispatch::TaskDispatcher,
    jobs::{JobManager, JobSpecJson},
    ports::{JobRow, MatchStore, NetworkStore},
    reanalysis::ReanaQueue,
    CoreResult,
};

use super::{new_id, TaskSource};

pub struct JobSource {
    pub jobs: Arc<JobManager>,
    pub matches: Arc<dyn MatchStore>,
    pub nets: Arc<dyn NetworkStore>,
    pub disp: Arc<TaskDispatcher>,
    pub reana: Arc<ReanaQueue>,
    pub asm: Arc<TaskAssembler>,
    pub ctl: Arc<Control>,
    pub cfg: Arc<Config>,
}

impl TaskSource for JobSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        self.jobs.preemption_sweep(false);
        let mut candidates: Vec<JobRow> = Vec::new();
        for kind in [
            bqs_proto::JobKind::JobRating,
            bqs_proto::JobKind::JobCollect,
            bqs_proto::JobKind::JobSelfplay,
        ] {
            candidates.extend(self.jobs.dispatchable(kind)?);
        }
        candidates.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.created_at.cmp(&b.created_at))
        });
        for job in candidates {
            match bqs_proto::JobKind::try_from(job.kind as i32) {
                Ok(bqs_proto::JobKind::JobCollect) => {
                    if let Some(resp) = self.dispatch_collect(&job, req)? {
                        return Ok(Some(resp));
                    }
                }
                Ok(bqs_proto::JobKind::JobRating) => {
                    if let Some(resp) = self.dispatch_rating(&job, req)? {
                        return Ok(Some(resp));
                    }
                }
                Ok(bqs_proto::JobKind::JobSelfplay) => {
                    if let Some(resp) = self.dispatch_selfplay(req)? {
                        return Ok(Some(resp));
                    }
                }
                _ => {}
            }
        }

        Ok(None)
    }
}

impl JobSource {
    pub(super) fn dispatch_collect(
        &self,
        job: &JobRow,
        req: &TaskRequest,
    ) -> CoreResult<Option<TaskResponse>> {
        let Some(JobSpecJson::Collect(spec)) = JobSpecJson::parse(&job.spec_json) else {
            tracing::warn!("[job] collect job {} spec_json 解析失败，跳过", job.job_id);
            return Ok(None);
        };
        let proto_spec = spec.to_proto();

        let a_net = if bq_policy::player_kind::requires_model(&spec.player_a.kind) {
            if spec.player_a.network_sha.is_empty() {
                tracing::warn!(
                    "[job] collect job {} player_a 模型棋手缺 network_sha，跳过",
                    job.job_id
                );
                return Ok(None);
            }
            match self.nets.get_network(&spec.player_a.network_sha)? {
                Some(n) => Some(n),
                None => {
                    tracing::warn!(
                        "[job] collect job {} player_a 网络登记记录缺失 sha={}，跳过",
                        job.job_id,
                        spec.player_a.network_sha
                    );
                    return Ok(None);
                }
            }
        } else {
            None
        };
        let b_net = if bq_policy::player_kind::requires_model(&spec.player_b.kind) {
            if spec.player_b.network_sha.is_empty() {
                tracing::warn!(
                    "[job] collect job {} player_b 模型棋手缺 network_sha，跳过",
                    job.job_id
                );
                return Ok(None);
            }
            match self.nets.get_network(&spec.player_b.network_sha)? {
                Some(n) => Some(n),
                None => {
                    tracing::warn!(
                        "[job] collect job {} player_b 网络登记记录缺失 sha={}，跳过",
                        job.job_id,
                        spec.player_b.network_sha
                    );
                    return Ok(None);
                }
            }
        } else {
            None
        };

        let inflight: Vec<_> = self
            .disp
            .running()
            .into_iter()
            .filter(|t| t.kind == bqs_proto::TaskKind::TaskCollect && t.job_id == job.job_id)
            .collect();

        let max_per_worker = self.cfg.jobs.collect_max_inflight_per_worker;
        if max_per_worker > 0 {
            let worker_inflight = inflight
                .iter()
                .filter(|t| t.worker_id == req.worker_id)
                .count() as i64;
            if worker_inflight >= max_per_worker {
                tracing::debug!(
                    "[job] worker={} 在 job={} 上已有 {} 批在飞（上限 {max_per_worker}），本请求跳过",
                    req.worker_id,
                    job.job_id,
                    worker_inflight
                );
                return Ok(None);
            }
        }

        let base = if self.cfg.jobs.collect_games_per_task > 0 {
            self.cfg.jobs.collect_games_per_task
        } else {
            self.cfg.selfplay.games_per_task as i64
        };
        let mut games = games_for_threads(self.cfg.selfplay.threads_baseline, base, req.threads);
        games += games & 1;

        if spec.total_games > 0 {
            let slack = if self.cfg.jobs.collect_overdispatch_games < 0 {
                0
            } else if self.cfg.jobs.collect_overdispatch_games > 0 {
                self.cfg.jobs.collect_overdispatch_games
            } else {
                base
            };
            let inflight_games: i64 = inflight.iter().map(|t| t.games).sum();
            let remaining = spec.total_games as i64 - job.completed_games - inflight_games + slack;
            if remaining < 2 {
                return Ok(None);
            }
            games = games.min(remaining);
            games -= games & 1;
            if games < 2 {
                return Ok(None);
            }
        }
        let seed = crate::jobs::resolve_seed(&spec.seed_strategy, job.next_seq);
        let deadline_unix =
            (self.jobs.now() + self.cfg.jobs.collect_deadline_seconds).max(0) as u64;
        let task_id = new_id();
        let resp = self.asm.collect_task(
            &task_id,
            req,
            &proto_spec,
            games as u64,
            seed,
            deadline_unix,
            a_net.as_ref(),
            b_net.as_ref(),
        );
        self.jobs.note_dispatch(
            &task_id,
            &job.job_id,
            games,
            &req.worker_id,
            bqs_proto::TaskKind::TaskCollect,
            "",
            "",
            "",
            0,
            Some(self.cfg.jobs.collect_deadline_seconds),
        )?;
        tracing::info!(
            "[task] collect assigned worker={} task={} job={} variant={} games={} seed={} data_kind={}",
            req.worker_id,
            task_id,
            job.job_id,
            proto_spec.variant,
            games,
            seed,
            proto_spec.data_kind
        );
        Ok(Some(resp?))
    }

    pub(super) fn dispatch_rating(
        &self,
        job: &JobRow,
        req: &TaskRequest,
    ) -> CoreResult<Option<TaskResponse>> {
        let Some(JobSpecJson::Rating(spec)) = JobSpecJson::parse(&job.spec_json) else {
            tracing::warn!("[job] rating job {} spec_json 解析失败，跳过", job.job_id);
            return Ok(None);
        };
        if spec.match_id == 0 {
            tracing::warn!("[job] rating job {} 无对发行（提交异常），跳过", job.job_id);
            return Ok(None);
        }
        let Some(m) = self.matches.get_match(spec.match_id)? else {
            tracing::warn!(
                "[job] rating job {} 对发行 {} 缺失，跳过",
                job.job_id,
                spec.match_id
            );
            return Ok(None);
        };
        if !m.running() {
            return Ok(None);
        }

        let (side_a, side_b) = spec.resolved_sides();
        let (a_model, b_model) = spec.sides_are_model();
        let a_net = if a_model {
            match self.nets.get_network(&m.candidate)? {
                Some(n) => Some(n),
                None => {
                    tracing::warn!(
                        "[task] rating side_a 网络登记记录缺失 sha={}，跳过",
                        m.candidate
                    );
                    return Ok(None);
                }
            }
        } else {
            None
        };
        let b_net = if b_model {
            match self.nets.get_network(&m.opponent)? {
                Some(n) => Some(n),
                None => {
                    tracing::warn!(
                        "[task] rating side_b 网络登记记录缺失 sha={}，本批不下发对手网络",
                        m.opponent
                    );
                    None
                }
            }
        } else {
            None
        };

        let variant = if spec.variant.is_empty() {
            self.cfg.selfplay.variant.clone()
        } else {
            spec.variant.clone()
        };
        let mut games = games_for_threads(
            self.cfg.selfplay.threads_baseline,
            self.cfg.selfplay.games_per_task as i64,
            req.threads,
        );
        games += games & 1;
        if spec.total_games > 0 {
            let inflight_games: i64 = self
                .disp
                .running()
                .into_iter()
                .filter(|t| t.kind == bqs_proto::TaskKind::TaskRating && t.job_id == job.job_id)
                .map(|t| t.games)
                .sum();
            let remaining = spec.total_games as i64 - job.completed_games - inflight_games;
            if remaining < 2 {
                return Ok(None);
            }
            games = games.min(remaining);
            games -= games & 1;
            if games < 2 {
                return Ok(None);
            }
        }
        let task_id = new_id();
        let side_a_spec = side_a.to_proto();
        let side_b_spec = side_b.to_proto();
        let resp = self.asm.rating_task(
            &task_id,
            &variant,
            &side_a_spec,
            &side_b_spec,
            a_net.as_ref(),
            b_net.as_ref(),
            games,
            spec.start_fens.clone(),
            req,
        )?;
        self.jobs.note_dispatch(
            &task_id,
            &job.job_id,
            games,
            &req.worker_id,
            bqs_proto::TaskKind::TaskRating,
            &m.candidate,
            &m.opponent,
            "",
            m.id,
            None,
        )?;
        tracing::info!(
            "[task] rating assigned worker={} task={} job={} match={} side_a={}:{} side_b={}:{} games={}",
            req.worker_id,
            task_id,
            job.job_id,
            m.id,
            side_a.kind,
            m.candidate,
            side_b.kind,
            m.opponent,
            games
        );
        Ok(Some(resp))
    }

    fn dispatch_selfplay(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        if self.ctl.manual_paused() {
            return Ok(Some(TaskResponse {
                message: "self_play_paused".into(),
                ..Default::default()
            }));
        }
        if self.ctl.auto_paused() {
            return Ok(Some(TaskResponse {
                message: format!("self_play_auto_paused: {}", self.ctl.auto_pause_reason()),
                ..Default::default()
            }));
        }

        let Some(job) = self
            .jobs
            .dispatchable(bqs_proto::JobKind::JobSelfplay)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let spec = JobSpecJson::parse(&job.spec_json);
        let search: Option<CollectSearchParams> = match &spec {
            Some(JobSpecJson::SelfPlay(s)) => s.search.as_ref().and_then(|s| s.to_proto()),
            _ => None,
        };

        let mut pm = self.matches.pending_match()?;
        if let Some(m) = &pm {
            if self.jobs.is_rating_match(m.id) {
                pm = None;
            }
        }
        let best = self.nets.get_best()?;

        if pm.is_none()
            && best.is_none()
            && self.cfg.selfplay.best == crate::consts::RULE_SENTINEL_SHA
        {
            let task_id = new_id();
            let games = self.asm.games_for(req.threads);
            let resp = self.asm.sentinel_task(&task_id, req);
            self.reana.note_selfplay_assigned();
            self.jobs.note_dispatch(
                &task_id,
                &job.job_id,
                games,
                &req.worker_id,
                bqs_proto::TaskKind::TaskSelfplay,
                crate::consts::RULE_SENTINEL_SHA,
                "",
                "",
                0,
                None,
            )?;
            tracing::info!(
                "[task] selfplay(rule-sentinel) assigned worker={} task={} job={} games={} data_kind=nnue",
                req.worker_id,
                task_id,
                job.job_id,
                games
            );
            return Ok(Some(resp));
        }
        let sel = self
            .asm
            .resolve_selfplay(best, pm, self.ctl.data_kind() as i32)?;
        if sel.mode == SelfplayMode::None {
            return Ok(Some(TaskResponse {
                message: sel.none_message,
                ..Default::default()
            }));
        }
        let producer = sel
            .producer
            .as_ref()
            .ok_or_else(|| crate::CoreError::Internal("selfplay producer missing".into()))?
            .clone();
        let task_id = new_id();
        self.reana.note_selfplay_assigned();

        let resp = match (&sel.mode, &sel.m) {
            (SelfplayMode::Mix, Some(m)) => {
                let opponent = self.nets.get_network(&m.opponent)?;
                let Some(opponent) = opponent else {
                    tracing::warn!(
                        "[task] gatekeeper 对手网络登记记录缺失 sha={}，本批跳过（对手棋手无对象键必被 collector 拒绝）",
                        m.opponent
                    );
                    return Ok(None);
                };
                let games = self.asm.games_for(req.threads);
                let resp = self.asm.gatekeeper_task(
                    &task_id,
                    req,
                    &producer,
                    &m.opponent,
                    Some(&opponent),
                    games,
                )?;
                self.jobs.note_dispatch(
                    &task_id,
                    &job.job_id,
                    games,
                    &req.worker_id,
                    bqs_proto::TaskKind::TaskSelfplay,
                    &producer.sha,
                    &m.opponent,
                    "",
                    m.id,
                    None,
                )?;
                tracing::info!(
                    "[task] gatekeeper assigned worker={} task={} job={} match={} candidate={} opponent={} games={}",
                    req.worker_id,
                    task_id,
                    job.job_id,
                    m.id,
                    producer.sha,
                    m.opponent,
                    games
                );
                resp
            }

            _ => {
                let games = self.asm.games_for(req.threads);
                let resp = self.asm.selfplay_task(
                    &task_id,
                    req,
                    &producer,
                    &self.cfg.selfplay.train_match_opponent,
                    search,
                    games,
                )?;
                self.jobs.note_dispatch(
                    &task_id,
                    &job.job_id,
                    games,
                    &req.worker_id,
                    bqs_proto::TaskKind::TaskSelfplay,
                    &producer.sha,
                    "",
                    "",
                    0,
                    None,
                )?;
                tracing::info!(
                    "[task] selfplay assigned worker={} task={} job={} network={} games={}",
                    req.worker_id,
                    task_id,
                    job.job_id,
                    producer.sha,
                    games
                );
                resp
            }
        };
        Ok(Some(resp))
    }
}
