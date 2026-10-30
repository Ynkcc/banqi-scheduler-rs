use std::sync::{Arc, Mutex, Weak};

use bqs_config::Config;
use bqs_judgment::judgment::{
    RollbackVerdict, consecutive_no_progress, judge_rollback, judge_stop,
};
use bqs_proto::{MatchResult, MatchResultAck, TaskKind};
use serde::Serialize;

use crate::{
    CoreResult,
    assembler::games_for_threads,
    consts::{EVAL_MAX_ATTEMPTS, EVAL_MAX_GAMES_PER_TASK, EVAL_TASK_STALE_AFTER},
    control::Control,
    dispatch::{RunningTask, TaskDispatcher},
    evalopponents::EVAL_OPPONENT_MATERIAL_EXPECTIMAX,
    ports::{Clock, EvalStore, NetworkStore, Ports},
    types::{EvalDelta, EvalResult, network_status},
};

pub const SETTING_EVAL_PROMOTIONS: &str = "eval_promotions";

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EvalOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opponents: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub games: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcts_sims: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_progress_n: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_progress_eps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_eps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor_win_rate: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_win_rate: Option<f64>,
}

pub trait BestMutator: Send + Sync {
    fn promote_best(&self, sha: &str) -> CoreResult<()>;
    fn reject_network(&self, sha: &str) -> CoreResult<()>;
}

pub struct StoreBestMutator {
    nets: Arc<dyn NetworkStore>,
}

impl StoreBestMutator {
    pub fn new(nets: Arc<dyn NetworkStore>) -> Self {
        Self { nets }
    }
}

impl BestMutator for StoreBestMutator {
    fn promote_best(&self, sha: &str) -> CoreResult<()> {
        self.nets.promote_best(sha)
    }

    fn reject_network(&self, sha: &str) -> CoreResult<()> {
        self.nets
            .update_network_status(sha, network_status::REJECTED)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PendingEval {
    pub network_sha: String,
    pub opponent_spec: String,

    pub opponent_sha: String,
    pub target_games: i64,
    pub per_task_games: i64,
    pub attempts: i64,
    pub wins: i64,
    pub draws: i64,
    pub losses: i64,
    pub num_games: i64,
    pub created_at: i64,
}

impl PendingEval {
    fn remaining(&self) -> i64 {
        (self.target_games - self.num_games).max(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedEvalTask {
    pub task_id: String,
    pub network_sha: String,
    pub opponent_spec: String,

    pub games: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvalReportAck {
    pub accepted: bool,
    pub message: String,
    pub concluded: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalRunningJob {
    #[serde(rename = "networkSha")]
    pub network_sha: String,
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,
    #[serde(rename = "numGames")]
    pub num_games: i64,
    #[serde(rename = "targetGames")]
    pub target_games: i64,
    #[serde(rename = "inFlight")]
    pub in_flight: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalProgress {
    #[serde(rename = "opponentSpec")]
    pub opponent_spec: String,

    pub versions: usize,

    pub no_progress: usize,
    #[serde(rename = "latestWinRate")]
    pub latest_win_rate: f64,
    #[serde(rename = "latestSha")]
    pub latest_sha: String,
}

#[derive(Default)]
struct Inner {
    evals: Vec<PendingEval>,
    promotions: i64,
}

pub struct EvalCoordinator {
    cfg: Arc<Config>,
    ctl: Arc<Control>,
    clock: Arc<dyn Clock>,
    eval: Arc<dyn EvalStore>,
    nets: Arc<dyn NetworkStore>,
    disp: Arc<TaskDispatcher>,
    mutator: Box<dyn BestMutator>,
    inner: Mutex<Inner>,

    job_hook: Mutex<Option<Weak<crate::jobs::JobManager>>>,
}

impl EvalCoordinator {
    pub fn new(
        ports: &Ports,
        cfg: Arc<Config>,
        clock: Arc<dyn Clock>,
        ctl: Arc<Control>,
        disp: Arc<TaskDispatcher>,
    ) -> Self {
        let this = Self {
            cfg,
            ctl,
            clock,
            eval: ports.eval.clone(),
            nets: ports.networks.clone(),
            disp,
            mutator: Box::new(StoreBestMutator::new(ports.networks.clone())),
            inner: Mutex::new(Inner::default()),
            job_hook: Mutex::new(None),
        };
        this.restore_queue();
        this
    }

    fn eval_enabled(&self) -> bool {
        match self.ctl.eval_override() {
            Some(o) if o.enabled.is_some() => o.enabled.unwrap(),
            _ => self.cfg.eval.enabled,
        }
    }

    fn eval_opponents(&self) -> Vec<String> {
        match self.ctl.eval_override() {
            Some(o) if o.opponents.is_some() => o.opponents.clone().unwrap(),
            _ => self.cfg.eval.opponents.clone(),
        }
    }

    fn eval_games(&self) -> i64 {
        match self.ctl.eval_override() {
            Some(o) if o.games.is_some() => o.games.unwrap(),
            _ => self.cfg.eval.games,
        }
    }

    fn eval_no_progress_n(&self) -> i64 {
        match self.ctl.eval_override() {
            Some(o) if o.no_progress_n.is_some() => o.no_progress_n.unwrap(),
            _ => self.cfg.eval.no_progress_n,
        }
    }

    fn eval_no_progress_eps(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.no_progress_eps.is_some() => o.no_progress_eps.unwrap(),
            _ => self.cfg.eval.no_progress_eps,
        }
    }

    fn eval_rollback_eps(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.rollback_eps.is_some() => o.rollback_eps.unwrap(),
            _ => self.cfg.eval.rollback_eps,
        }
    }

    fn eval_floor_win_rate(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.floor_win_rate.is_some() => o.floor_win_rate.unwrap(),
            _ => self.cfg.eval.floor_win_rate,
        }
    }

    fn eval_stop_win_rate(&self) -> f64 {
        match self.ctl.eval_override() {
            Some(o) if o.stop_win_rate.is_some() => o.stop_win_rate.unwrap(),
            _ => self.cfg.eval.stop_win_rate,
        }
    }

    pub fn set_job_hook(&self, jobs: &Arc<crate::jobs::JobManager>) {
        *self.job_hook.lock().unwrap() = Some(Arc::downgrade(jobs));
    }

    fn notify_job_created(&self, sha: &str, spec: &str, opponent_sha: &str) {
        let hook = self.job_hook.lock().unwrap().clone();
        if let Some(jobs) = hook.and_then(|w| w.upgrade()) {
            if let Err(e) = jobs.create_eval_job(sha, spec, opponent_sha) {
                tracing::warn!(
                    "[eval] ⚠️ 创建 JOB_EVAL job 失败 network={sha} opponent={spec}: {e}"
                );
            }
        }
    }

    pub fn ensure_pending(
        &self,
        network_sha: &str,
        opponent_spec: &str,
        opponent_sha: &str,
    ) -> CoreResult<bool> {
        let games = self.eval_games();
        let mut per_task = self.cfg.eval.games_per_task;
        if per_task <= 0 {
            per_task = games;
        }
        let mut inner = self.inner.lock().unwrap();
        if inner
            .evals
            .iter()
            .any(|j| j.network_sha == network_sha && j.opponent_spec == opponent_spec)
        {
            return Ok(false);
        }
        let created = self.eval.enqueue_eval_job(
            network_sha,
            opponent_spec,
            opponent_sha,
            games,
            per_task,
        )?;
        if created {
            inner.evals.push(PendingEval {
                network_sha: network_sha.to_string(),
                opponent_spec: opponent_spec.to_string(),
                opponent_sha: opponent_sha.to_string(),
                target_games: games,
                per_task_games: per_task,
                ..Default::default()
            });
        }
        Ok(created)
    }

    fn restore_queue(&self) {
        let entries = match self.eval.load_eval_queue() {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("[eval] ⚠️ 启动恢复 eval 队列失败: {e}");
                Vec::new()
            }
        };
        let counter = match self.eval.get_int_setting(SETTING_EVAL_PROMOTIONS, 0) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    "[eval] ⚠️ {SETTING_EVAL_PROMOTIONS} 计数器格式非法（按 0 计数）: {e}"
                );
                0
            }
        };
        let target = self.eval_games();
        let cfg_per_task = self.cfg.eval.games_per_task;
        let fallback_per_task = if cfg_per_task <= 0 {
            target
        } else {
            cfg_per_task
        };
        let mut inner = self.inner.lock().unwrap();
        inner.evals = entries
            .into_iter()
            .map(|en| PendingEval {
                per_task_games: if en.per_task_games <= 0 {
                    fallback_per_task
                } else {
                    en.per_task_games
                },
                network_sha: en.network_sha,
                opponent_spec: en.opponent_spec,
                opponent_sha: en.opponent_sha,
                target_games: en.target_games,
                attempts: en.attempts,
                wins: en.wins,
                draws: en.draws,
                losses: en.losses,
                num_games: en.num_games,
                created_at: en.created_at,
            })
            .collect();
        inner.promotions = counter;
    }

    fn enqueue_locked(&self, inner: &mut Inner, sha: &str, specs: &[String]) -> i64 {
        let games = self.eval_games();
        let mut per_task = self.cfg.eval.games_per_task;
        if per_task <= 0 {
            per_task = games;
        }
        let mut n = 0;
        for spec in specs {
            let spec = spec.trim();
            if spec.is_empty()
                || inner
                    .evals
                    .iter()
                    .any(|j| j.network_sha == sha && j.opponent_spec == spec)
            {
                continue;
            }
            match self.eval.enqueue_eval_job(sha, spec, "", games, per_task) {
                Ok(true) => {
                    inner.evals.push(PendingEval {
                        network_sha: sha.to_string(),
                        opponent_spec: spec.to_string(),
                        target_games: games,
                        per_task_games: per_task,
                        ..Default::default()
                    });
                    self.notify_job_created(sha, spec, "");
                    n += 1;
                }

                Ok(false) => {}
                Err(e) => {
                    tracing::warn!("[eval] ⚠️ 入队 {sha}/{spec} 持久化失败（跳过该条）: {e}");
                }
            }
        }
        n
    }

    pub fn on_new_best(&self, sha: &str) -> CoreResult<()> {
        if !self.eval_enabled() {
            return Ok(());
        }
        let opponents = self.eval_opponents();
        if opponents.is_empty() || sha.is_empty() {
            return Ok(());
        }
        let mut every = self.cfg.eval.every_n_promotions;
        if every < 1 {
            every = 1;
        }
        let due = {
            let mut inner = self.inner.lock().unwrap();
            inner.promotions += 1;
            inner.promotions % every == 0
        };

        if let Err(e) = self.eval.inc_setting(SETTING_EVAL_PROMOTIONS) {
            tracing::warn!("[eval] ⚠️ 持久化节流计数器失败（忽略）: {e}");
        }
        if !due {
            return Ok(());
        }
        let n = {
            let mut inner = self.inner.lock().unwrap();
            self.enqueue_locked(&mut inner, sha, &opponents)
        };
        if n > 0 {
            tracing::info!(
                "[eval] 入队 network={sha} 对手={opponents:?}（{n} 条；节流 1/{every} 次晋级；模式={}）",
                eval_search_mode_name(self.cfg.eval.mcts_sims)
            );
        }
        Ok(())
    }

    pub fn ensure_best_evaluated(&self) {
        if !self.eval_enabled() {
            return;
        }
        let best = match self.nets.get_best() {
            Ok(Some(b)) => b,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!("[eval] ⚠️ 启动补齐：读取 best 失败: {e}");
                return;
            }
        };
        let mut missing = Vec::new();
        for spec in self.eval_opponents() {
            let spec = spec.trim().to_string();
            if spec.is_empty() {
                continue;
            }
            match self.eval.get_eval_result(&best.sha, &spec) {
                Ok(None) => missing.push(spec),
                Ok(Some(_)) => {}
                Err(e) => {
                    tracing::warn!("[eval] ⚠️ 启动补齐：查询 {}/{spec} 失败: {e}", best.sha)
                }
            }
        }
        if missing.is_empty() {
            return;
        }
        let n = {
            let mut inner = self.inner.lock().unwrap();
            self.enqueue_locked(&mut inner, &best.sha, &missing)
        };
        if n > 0 {
            tracing::info!(
                "[eval] 启动补齐当前 best={} 的缺失评估：{missing:?}",
                best.sha
            );
        }
    }

    pub fn trigger_manual(&self) -> Result<i64, String> {
        if !self.eval_enabled() {
            return Err(
                "评估未启用（eval.enabled=false 或 WebUI 在线禁用）：启用后才会派发评估任务"
                    .to_string(),
            );
        }
        let best = self
            .nets
            .get_best()
            .map_err(|e| format!("读取 best 失败: {e}"))?;
        let Some(best) = best else {
            return Err("当前无 best 网络，无可评估对象".to_string());
        };
        let opponents = self.eval_opponents();
        let n = {
            let mut inner = self.inner.lock().unwrap();
            self.enqueue_locked(&mut inner, &best.sha, &opponents)
        };
        tracing::info!(
            "[eval] 手动触发评估 best={}：新入队 {n} 条（对手={opponents:?}）",
            best.sha
        );
        Ok(n)
    }

    pub fn claim_eval_task(&self, worker_id: &str, threads: i32) -> Option<(PendingEval, String)> {
        if !self.eval_enabled() {
            return None;
        }
        let now = self.clock.now();
        let mut inner = self.inner.lock().unwrap();
        let mut i = 0usize;
        while i < inner.evals.len() {
            let (sha, spec, remaining, attempts, per_task, num_games, target) = {
                let j = &inner.evals[i];
                (
                    j.network_sha.clone(),
                    j.opponent_spec.clone(),
                    j.remaining(),
                    j.attempts,
                    j.per_task_games,
                    j.num_games,
                    j.target_games,
                )
            };
            if remaining == 0 {
                inner.evals.remove(i);
                self.finalize_locked(&mut inner, &sha, &spec);
                continue;
            }

            let in_flight = self.disp.in_flight_games(
                TaskKind::TaskEval,
                &sha,
                &spec,
                now,
                EVAL_TASK_STALE_AFTER,
            );

            if attempts >= EVAL_MAX_ATTEMPTS && in_flight == 0 {
                if let Err(e) = self.eval.delete_eval_job(&sha, &spec) {
                    tracing::warn!(
                        "[eval] ⚠️ 丢弃作业时清理 eval_queue 失败（重启后会再丢一次）: {e}"
                    );
                }
                inner.evals.remove(i);
                tracing::warn!(
                    "[eval] ⚠️ 丢弃评估作业 network={sha} opponent={spec}：连续 {attempts} 次下发均无进度且无在飞\
                     （已累计 {num_games}/{target} 局；collector 版本过旧不认 TASK_EVAL？或 worker 反复掉线）"
                );
                continue;
            }
            if in_flight >= remaining {
                i += 1;
                continue;
            }
            let mut games =
                games_for_threads(self.cfg.selfplay.threads_baseline, per_task, threads);
            if games > EVAL_MAX_GAMES_PER_TASK {
                games = EVAL_MAX_GAMES_PER_TASK;
            }
            let avail = remaining - in_flight;
            if games > avail {
                games = avail;
            }

            let attempts = match self.eval.inc_eval_job_attempts(&sha, &spec) {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!("[eval] ⚠️ 持久化 attempts 失败（按内存递增）: {e}");
                    attempts + 1
                }
            };
            inner.evals[i].attempts = attempts;
            let task_id = crate::sources::new_id();

            self.disp.register_in_flight(
                &task_id,
                RunningTask {
                    kind: TaskKind::TaskEval,
                    network_sha: sha.clone(),
                    opponent_spec: spec.clone(),
                    games,
                    worker_id: worker_id.to_string(),
                    assigned_at: now,
                    deadline: now + EVAL_TASK_STALE_AFTER.as_secs() as i64,
                    ..RunningTask::new(TaskKind::TaskEval)
                },
            );
            return Some((inner.evals[i].clone(), task_id));
        }
        None
    }

    pub fn release_eval_claim(&self, task_id: &str) {
        self.disp.delete(task_id);
    }

    pub fn claim_task(&self, worker_id: &str, threads: i32) -> Option<ClaimedEvalTask> {
        let (job, task_id) = self.claim_eval_task(worker_id, threads)?;
        let games = self
            .disp
            .lookup(&task_id)
            .map(|t| t.games)
            .unwrap_or(job.per_task_games);
        Some(ClaimedEvalTask {
            task_id,
            network_sha: job.network_sha,
            opponent_spec: job.opponent_spec,
            games,
        })
    }

    pub fn release_claim(&self, task_id: &str) {
        self.release_eval_claim(task_id)
    }

    pub fn report_eval_result(
        &self,
        task: &RunningTask,
        req: &MatchResult,
    ) -> CoreResult<MatchResultAck> {
        let spec_hint = req.opponent_spec.trim().to_string();

        let pair_wins = (req.pair_dw + req.pair_ww + req.pair_wl) as i64;
        let pair_losses = (req.pair_ll + req.pair_ld + req.pair_wl) as i64;
        let pair_draws = (req.pair_dd + req.pair_ld + req.pair_dw) as i64;
        let (wins, losses, draws) = if req.wins == 0
            && req.losses == 0
            && req.draws == 0
            && (pair_wins > 0 || pair_losses > 0 || pair_draws > 0)
        {
            (pair_wins, pair_losses, pair_draws)
        } else {
            (req.wins as i64, req.losses as i64, req.draws as i64)
        };
        let delta = EvalDelta {
            wins,
            draws,
            losses,
            games: req.games as i64,
            red_wins: req.red_wins as i64,
            black_wins: req.black_wins as i64,
            draw_no_capture: req.draw_no_capture as i64,
            draw_truncation: req.draw_truncation as i64,
        };
        let ack = self.report_inner(&req.task_id, &task.network_sha, spec_hint, delta);
        Ok(MatchResultAck {
            accepted: ack.accepted,
            message: ack.message,
            match_concluded: ack.concluded,
            ..Default::default()
        })
    }

    pub fn report_result(&self, task_id: &str, req_spec: &str, delta: EvalDelta) -> EvalReportAck {
        let Some(task) = self.disp.lookup(task_id) else {
            return EvalReportAck {
                accepted: false,
                message: "eval_task_not_found".to_string(),
                concluded: false,
            };
        };
        let spec_hint = if req_spec.trim().is_empty() {
            task.opponent_spec.clone()
        } else {
            req_spec.trim().to_string()
        };
        self.report_inner(task_id, &task.network_sha, spec_hint, delta)
    }

    fn report_inner(
        &self,
        task_id: &str,
        sha: &str,
        spec_hint: String,
        delta: EvalDelta,
    ) -> EvalReportAck {
        let spec = if spec_hint.is_empty() {
            self.disp
                .lookup(task_id)
                .map(|t| t.opponent_spec)
                .unwrap_or_default()
        } else {
            spec_hint
        };
        let n = delta.games;
        if n <= 0 {
            self.disp.complete_in_flight(task_id);
            tracing::warn!("[eval] ⚠️ 上报 0 局，忽略 network={sha} opponent={spec}");
            return EvalReportAck {
                accepted: true,
                message: "eval_zero_games_ignored".to_string(),
                concluded: false,
            };
        }
        let entry = match self.eval.accumulate_eval_job(sha, &spec, delta) {
            Ok(e) => e,

            Err(e) => {
                self.disp.complete_in_flight(task_id);
                tracing::info!("[eval] 忽略迟到上报 network={sha} opponent={spec} games={n}: {e}");
                return EvalReportAck {
                    accepted: true,
                    message: "eval_job_missing".to_string(),
                    concluded: false,
                };
            }
        };
        let concluded = entry.reached_target();
        let mut inner = self.inner.lock().unwrap();

        self.disp.complete_in_flight(task_id);
        if let Some(job) = inner
            .evals
            .iter_mut()
            .find(|j| j.network_sha == sha && j.opponent_spec == spec)
        {
            job.wins = entry.wins;
            job.draws = entry.draws;
            job.losses = entry.losses;
            job.num_games = entry.num_games;
            job.attempts = 0;
        }
        if concluded {
            self.finalize_locked(&mut inner, sha, &spec);
        } else {
            let entry_target = entry.target_games;
            let total = entry.num_games;
            tracing::info!(
                "[eval] += network={sha} opponent={spec} {n} 局（累计 {total}/{entry_target}）"
            );
        }
        EvalReportAck {
            accepted: true,
            message: String::new(),
            concluded,
        }
    }

    fn finalize_locked(&self, inner: &mut Inner, sha: &str, spec: &str) {
        let row = match self.eval.get_eval_job(sha, spec) {
            Ok(Some(r)) => r,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!("[eval] ⚠️ 读取作业进度失败 network={sha} opponent={spec}: {e}");
                return;
            }
        };
        inner
            .evals
            .retain(|j| !(j.network_sha == sha && j.opponent_spec == spec));
        if let Err(e) = self.eval.delete_eval_job(sha, spec) {
            tracing::warn!(
                "[eval] ⚠️ 收尾清理 eval_queue 失败（重启后可能重复收尾，UNIQUE 幂等兜底）: {e}"
            );
        }
        if row.num_games <= 0 {
            tracing::warn!("[eval] ⚠️ {sha}/{spec} 收尾但累计 0 局，不落结果");
            return;
        }
        let sum = EvalResult {
            id: 0,
            network_sha: sha.to_string(),
            opponent_spec: spec.to_string(),
            wins: row.wins,
            draws: row.draws,
            losses: row.losses,
            num_games: row.num_games,
            avg_moves: 0.0,
            created_at: self.clock.now(),
            red_wins: row.red_wins,
            black_wins: row.black_wins,
            draw_no_capture: row.draw_no_capture,
            draw_truncation: row.draw_truncation,
        };
        if let Err(e) = self.eval.upsert_eval_result(sum.clone()) {
            tracing::warn!("[eval] ⚠️ 评估结果落库失败 network={sha} opponent={spec}: {e}");
            return;
        }
        tracing::info!(
            "[eval] ✅ network={sha} opponent={spec} {} 局 胜{} 平{} 负{} 胜率{:.1}%",
            sum.num_games,
            sum.wins,
            sum.draws,
            sum.losses,
            100.0 * sum.win_rate()
        );
        self.judge_eval_progress(spec);

        if spec == EVAL_OPPONENT_MATERIAL_EXPECTIMAX {
            self.judge_eval_rollback(sha, sum.win_rate());
            self.judge_eval_target(sum.win_rate());
        }
    }

    fn judge_eval_progress(&self, spec: &str) {
        let n = self.eval_no_progress_n();
        if !self.eval_enabled() || n <= 0 {
            return;
        }
        let trend = match self.eval.eval_trend(spec, n + 2) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("[eval] ⚠️ 读取趋势失败 opponent={spec}: {e}");
                return;
            }
        };
        if trend.len() < 2 {
            return;
        }
        let eps = self.eval_no_progress_eps();
        let rates: Vec<f64> = trend.iter().map(|r| r.win_rate()).collect();
        let stale = consecutive_no_progress(&rates, eps);
        let latest = trend.last().expect("len >= 2");
        tracing::info!(
            "[eval] 趋势 opponent={spec} 已评测版本={} 连续无提升={stale}/{n}（eps={:.1}pt，最新胜率 {:.1}%）",
            trend.len(),
            eps * 100.0,
            100.0 * latest.win_rate()
        );
        if !judge_stop(stale, n) {
            return;
        }
        if self.ctl.should_stop().0 {
            return;
        }
        let reason = format!(
            "eval_no_progress: opponent={spec} 连续 {stale} 次提升 < {:.1}pt（最新胜率 {:.1}%）",
            eps * 100.0,
            100.0 * latest.win_rate()
        );
        if let Err(e) = self.ctl.set_stop(&reason) {
            tracing::warn!("[eval] ⚠️ 置位 should_stop 失败: {e}");
            return;
        }
        tracing::info!("[eval] 🏁 触发停机: {reason}");
    }

    fn judge_eval_rollback(&self, sha: &str, wr: f64) {
        let eps = self.eval_rollback_eps();
        let floor = self.eval_floor_win_rate();
        if eps <= 0.0 && floor <= 0.0 {
            return;
        }
        let net = match self.nets.get_network(sha) {
            Ok(Some(n)) => n,
            _ => return,
        };
        let mut parent_wr = 0.0;
        let mut has_parent = false;
        if let Some(parent) = net.parent_sha.as_deref().filter(|p| !p.is_empty()) {
            match self
                .eval
                .get_eval_result(parent, EVAL_OPPONENT_MATERIAL_EXPECTIMAX)
            {
                Ok(Some(r)) => {
                    parent_wr = r.win_rate();
                    has_parent = true;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("[eval] ⚠️ 回滚判据读取前任评测失败 best={sha}: {e}");
                    return;
                }
            }
        }
        let (verdict, reason) = judge_rollback(wr, parent_wr, has_parent, eps, floor);
        match verdict {
            RollbackVerdict::None => return,
            RollbackVerdict::Observe => {
                tracing::warn!(
                    "[eval] ⛔ material_expectimax 击穿但无前任可回滚 network={sha} wr={:.1}%（floor={:.1}%，仅记录）",
                    100.0 * wr,
                    100.0 * floor
                );
                return;
            }
            RollbackVerdict::Rollback => {}
        }

        let best = match self.nets.get_best() {
            Ok(Some(b)) => b,
            _ => return,
        };
        if best.sha != sha {
            return;
        }
        let Some(parent) = net.parent_sha.as_deref().filter(|p| !p.is_empty()) else {
            return;
        };

        if let Err(e) = self.mutator.promote_best(parent) {
            tracing::warn!("[eval] ⚠️ 回滚晋级失败 best={sha} → {parent}: {e}");
            return;
        }
        if let Err(e) = self.mutator.reject_network(sha) {
            tracing::warn!("[eval] ⚠️ 回滚后标记 rejected 失败 sha={sha}: {e}");
        }
        tracing::info!("[eval] ⏪ 回滚 best={sha} → parent={parent}（{reason}）");
    }

    fn judge_eval_target(&self, wr: f64) {
        let target = self.eval_stop_win_rate();
        if !self.eval_enabled() || target <= 0.0 || wr < target {
            return;
        }
        if self.ctl.should_stop().0 {
            return;
        }
        let reason = format!(
            "eval_target: material_expectimax 胜率 {:.1}% ≥ 目标 {:.1}%",
            100.0 * wr,
            100.0 * target
        );
        if let Err(e) = self.ctl.set_stop(&reason) {
            tracing::warn!("[eval] ⚠️ 置位 should_stop 失败: {e}");
            return;
        }
        tracing::info!("[eval] 🏁 触发停机: {reason}");
    }

    pub fn pending_eval(&self) -> usize {
        self.inner.lock().unwrap().evals.len()
    }

    pub fn running_jobs(&self) -> Vec<EvalRunningJob> {
        let rows = match self.eval.load_eval_queue() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("[eval] ⚠️ 读取进行中作业失败: {e}");
                return Vec::new();
            }
        };
        let now = self.clock.now();
        let _inner = self.inner.lock().unwrap();
        rows.into_iter()
            .map(|r| EvalRunningJob {
                in_flight: self.disp.in_flight_games(
                    TaskKind::TaskEval,
                    &r.network_sha,
                    &r.opponent_spec,
                    now,
                    EVAL_TASK_STALE_AFTER,
                ),
                network_sha: r.network_sha,
                opponent_spec: r.opponent_spec,
                num_games: r.num_games,
                target_games: r.target_games,
            })
            .collect()
    }

    pub fn eval_progress_for(&self, spec: &str, window: i64) -> Option<EvalProgress> {
        let mut window = window;
        if window <= 0 {
            window = (self.eval_no_progress_n() + 2).max(2);
        }
        let trend = match self.eval.eval_trend(spec, window) {
            Ok(t) if !t.is_empty() => t,
            _ => return None,
        };
        let rates: Vec<f64> = trend.iter().map(|r| r.win_rate()).collect();
        Some(EvalProgress {
            opponent_spec: spec.to_string(),
            versions: trend.len(),
            no_progress: consecutive_no_progress(&rates, self.eval_no_progress_eps()),
            latest_win_rate: rates[rates.len() - 1],
            latest_sha: trend[trend.len() - 1].network_sha.clone(),
        })
    }
}

pub fn eval_search_mode_name(mcts_sims: i32) -> String {
    if mcts_sims <= 0 {
        "纯策略 argmax".to_string()
    } else {
        format!("MCTS {mcts_sims} sims")
    }
}

#[cfg(test)]
mod tests {
    use crate::control::Control;
    use crate::dispatch::{TaskDispatcher, TaskState};
    use crate::test_support::Fakes;
    use crate::types::{EvalDelta, EvalResult};

    use super::*;

    struct TestEnv {
        f: Fakes,
        disp: Arc<TaskDispatcher>,
        evalc: EvalCoordinator,
        ctl: Arc<Control>,
    }

    fn env(config_extra: &str) -> TestEnv {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse(config_extra, "test").unwrap());
        let ports = f.ports();
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
        let evalc = EvalCoordinator::new(
            &ports,
            cfg.clone(),
            f.clock.clone(),
            ctl.clone(),
            disp.clone(),
        );
        TestEnv {
            f,
            disp,
            evalc,
            ctl,
        }
    }

    fn enabled_cfg() -> &'static str {
        "[eval]\nenabled = true\ngames = 100\ngames_per_task = 10\nevery_n_promotions = 1\nno_progress_n = 2\nno_progress_eps = 0.01\nrollback_eps = 0.04\n"
    }

    fn delta(games: i64, wins: i64) -> EvalDelta {
        EvalDelta {
            games,
            wins,
            ..Default::default()
        }
    }

    fn claim_spec(env: &TestEnv, spec: &str) -> ClaimedEvalTask {
        loop {
            let claimed = env.evalc.claim_task("w1", 4).expect("应有可认领分片");
            if claimed.opponent_spec == spec {
                return claimed;
            }
            env.evalc.release_claim(&claimed.task_id);
        }
    }

    fn claim_and_report(env: &TestEnv, spec: &str, games: i64, wins: i64) -> EvalReportAck {
        let claimed = claim_spec(env, spec);
        env.evalc
            .report_result(&claimed.task_id, "", delta(games, wins))
    }

    fn eval_row(
        sha: &str,
        spec: &str,
        wins: i64,
        losses: i64,
        num_games: i64,
        created_at: i64,
    ) -> EvalResult {
        EvalResult {
            id: 0,
            network_sha: sha.to_string(),
            opponent_spec: spec.to_string(),
            wins,
            draws: 0,
            losses,
            num_games,
            avg_moves: 0.0,
            created_at,
            red_wins: 0,
            black_wins: 0,
            draw_no_capture: 0,
            draw_truncation: 0,
        }
    }

    #[test]
    fn enqueue_dedup_and_every_n_promotions_throttle() {
        let raw = "[eval]\nenabled = true\ngames = 50\nevery_n_promotions = 2\n";
        let e = env(raw);
        e.f.networks.seed_network("n1", None, "onnx");

        e.evalc.on_new_best("n1").unwrap();
        assert_eq!(e.evalc.pending_eval(), 0);

        e.evalc.on_new_best("n1").unwrap();
        assert_eq!(e.evalc.pending_eval(), 4);

        e.evalc.on_new_best("n1").unwrap();
        e.evalc.on_new_best("n1").unwrap();
        assert_eq!(e.evalc.pending_eval(), 4);

        assert_eq!(e.f.eval.get_int_setting("eval_promotions", 0).unwrap(), 4);
    }

    #[test]
    fn report_eval_result_derives_wld_from_pair_buckets() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();
        let c = e.evalc.claim_task("w1", 4).unwrap();
        let task = e.disp.lookup(&c.task_id).unwrap();
        let req = MatchResult {
            worker_id: "w1".into(),
            task_id: c.task_id.clone(),
            kind: TaskKind::TaskEval as i32,
            games: 8,

            pair_ll: 1,
            pair_ld: 1,
            pair_dd: 1,
            pair_wl: 1,
            pair_dw: 1,
            pair_ww: 1,
            ..Default::default()
        };
        let ack = e.evalc.report_eval_result(&task, &req).unwrap();
        assert!(ack.accepted, "{}", ack.message);

        let row =
            e.f.eval
                .get_eval_job("n", "random")
                .unwrap()
                .expect("应有聚合行");
        assert_eq!(row.num_games, 8);
        assert_eq!((row.wins, row.losses, row.draws), (3, 3, 3));
    }

    #[test]
    fn ensure_best_evaluated_fills_missing_opponents() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("best1", None, "onnx");
        e.f.networks.promote_best("best1").unwrap();

        e.f.eval
            .upsert_eval_result(eval_row("best1", "random", 1, 0, 1, 1))
            .unwrap();
        e.evalc.ensure_best_evaluated();
        assert_eq!(e.evalc.pending_eval(), 3, "已有结果的对手不再补");

        e.evalc.ensure_best_evaluated();
        assert_eq!(e.evalc.pending_eval(), 3);
    }

    #[test]
    fn trigger_manual_error_paths() {
        let e = env(enabled_cfg());

        assert!(e.evalc.trigger_manual().is_err());
        e.f.networks.seed_network("b", None, "onnx");
        e.f.networks.promote_best("b").unwrap();
        let n = e.evalc.trigger_manual().unwrap();
        assert_eq!(n, 4);

        let mut o = EvalOverride::default();
        o.enabled = Some(false);
        e.ctl.set_eval_override(o).unwrap();
        assert!(e.evalc.trigger_manual().is_err());
    }

    #[test]
    fn claim_respects_net_remaining_quota_and_stale_inflight() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();

        let c1 = e.evalc.claim_task("w1", 4).unwrap();
        assert_eq!(c1.opponent_spec, "random");
        assert_eq!(c1.games, 10);
        let t1 = e.disp.lookup(&c1.task_id).unwrap();
        assert_eq!(t1.games, 10, "认领即在 disp 登记在飞");
        assert_eq!(t1.kind, TaskKind::TaskEval);

        let c2 = e.evalc.claim_task("w2", 4).unwrap();
        assert_ne!(c1.task_id, c2.task_id);

        let mut random_shards = 2;
        let mut moved_on = false;
        while let Some(c) = e.evalc.claim_task("w3", 4) {
            if c.opponent_spec == "random" {
                random_shards += 1;
                assert!(random_shards <= 10, "在飞不应超过净剩余额度");
            } else {
                moved_on = true;
                e.evalc.release_claim(&c.task_id);
                break;
            }
        }
        assert_eq!(random_shards, 10);
        assert!(moved_on, "额度覆盖后应跳到下一作业");

        e.f.clock.advance(16 * 60);
        assert!(e.evalc.claim_task("w4", 4).is_some());
    }

    #[test]
    fn report_partial_then_finalize_at_target() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();

        for i in 0..10 {
            let claimed = claim_spec(&e, "random");
            let ack = e.evalc.report_result(&claimed.task_id, "", delta(10, 6));
            if i == 9 {
                assert!(ack.concluded, "第 10 片应触发收尾");
            } else {
                assert!(!ack.concluded);
            }
        }

        let r = e.f.eval.get_eval_result("n", "random").unwrap().unwrap();
        assert_eq!(r.num_games, 100);
        assert_eq!(r.wins, 60);
        assert!(e.f.eval.get_eval_job("n", "random").unwrap().is_none());

        let _ = TaskState::Completed;
    }

    #[test]
    fn zero_games_report_ignored_but_task_completed() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();
        let claimed = claim_spec(&e, "random");
        let ack = e.evalc.report_result(&claimed.task_id, "", delta(0, 0));
        assert_eq!(ack.message, "eval_zero_games_ignored");
        assert_eq!(
            e.disp.lookup(&claimed.task_id).unwrap().state,
            TaskState::Completed,
            "0 局上报也收尾任务本身"
        );

        assert!(
            e.f.eval
                .get_eval_job("n", &claimed.opponent_spec)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn stale_attempts_exhausted_drops_job() {
        let raw = "[eval]\nenabled = true\ngames = 10\ngames_per_task = 0\n";
        let e = env(raw);
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();

        for _ in 0..EVAL_MAX_ATTEMPTS {
            let c = e.evalc.claim_task("w", 4).unwrap();
            e.f.clock
                .advance(crate::consts::EVAL_TASK_STALE_AFTER.as_secs() as i64 + 1);
            assert!(
                e.disp.lookup(&c.task_id).is_some(),
                "超期在飞台账保留（仅额度失效）"
            );
        }
        let next = e.evalc.claim_task("w", 4);
        assert!(
            next.is_none() || next.unwrap().opponent_spec != "random",
            "random 作业 attempts 达上限且在飞为空：应被丢弃"
        );
        assert!(e.f.eval.get_eval_job("n", "random").unwrap().is_none());
    }

    #[test]
    fn in_flight_present_blocks_drop() {
        let raw = "[eval]\nenabled = true\ngames = 10\ngames_per_task = 0\n";
        let e = env(raw);
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();

        for _ in 0..(EVAL_MAX_ATTEMPTS - 1) {
            let c = e.evalc.claim_task("w", 4).unwrap();
            e.f.clock
                .advance(crate::consts::EVAL_TASK_STALE_AFTER.as_secs() as i64 + 1);
            e.evalc.release_claim(&c.task_id);
        }
        let _alive = e.evalc.claim_task("w", 4).unwrap();

        let c2 = e.evalc.claim_task("w2", 4).unwrap();
        assert_ne!(c2.opponent_spec, "random");
        e.evalc.release_claim(&c2.task_id);
        assert!(e.f.eval.get_eval_job("n", "random").unwrap().is_some());
    }

    #[test]
    fn stop_criterion_after_consecutive_no_progress() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n1", None, "onnx");

        for created_at in 1..=2i64 {
            e.f.eval
                .upsert_eval_result(eval_row(
                    &format!("old{created_at}"),
                    "random",
                    50,
                    50,
                    100,
                    created_at,
                ))
                .unwrap();
        }
        e.f.networks.promote_best("n1").unwrap();
        e.evalc.on_new_best("n1").unwrap();

        let _ = claim_and_report(&e, "random", 100, 50);

        let (stopped, reason) = e.ctl.should_stop();
        assert!(stopped, "应触发停机");
        assert!(reason.contains("eval_no_progress"), "{reason}");
        assert!(reason.contains("random"), "{reason}");
    }

    #[test]
    fn no_progress_does_not_fire_when_improving_or_disabled() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n1", None, "onnx");
        for created_at in 1..=2i64 {
            e.f.eval
                .upsert_eval_result(eval_row(
                    &format!("old{created_at}"),
                    "random",
                    40,
                    60,
                    100,
                    created_at,
                ))
                .unwrap();
        }
        e.f.networks.promote_best("n1").unwrap();
        e.evalc.on_new_best("n1").unwrap();
        let _ = claim_and_report(&e, "random", 100, 70);
        assert!(!e.ctl.should_stop().0);

        let raw = "[eval]\nenabled = true\ngames = 10\nno_progress_n = 0\n";
        let e2 = env(raw);
        e2.f.networks.seed_network("n", None, "onnx");
        e2.evalc.on_new_best("n").unwrap();
        let _ = claim_and_report(&e2, "random", 10, 0);
        assert!(!e2.ctl.should_stop().0);
    }

    #[test]
    fn rollback_to_parent_when_regressed_on_strongest_gauge() {
        let e = env(enabled_cfg());

        e.f.networks.seed_network("parent", None, "nnue");
        e.f.networks.seed_network("child", Some("parent"), "nnue");
        e.f.eval
            .upsert_eval_result(eval_row(
                "parent",
                "rule:material_expectimax",
                60,
                40,
                100,
                1,
            ))
            .unwrap();
        e.f.networks.promote_best("child").unwrap();
        e.evalc.on_new_best("child").unwrap();
        let _ = claim_and_report(&e, "rule:material_expectimax", 100, 30);

        assert_eq!(e.f.networks.best_sha().as_deref(), Some("parent"));
        assert_eq!(e.f.networks.status_of("child").as_deref(), Some("rejected"));
    }

    #[test]
    fn rollback_aborts_if_best_replaced_during_eval() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("parent", None, "nnue");
        e.f.networks.seed_network("child", Some("parent"), "nnue");
        e.f.networks.seed_network("newer", Some("child"), "nnue");
        e.f.eval
            .upsert_eval_result(eval_row(
                "parent",
                "rule:material_expectimax",
                60,
                40,
                100,
                1,
            ))
            .unwrap();
        e.f.networks.promote_best("child").unwrap();
        e.evalc.on_new_best("child").unwrap();

        let claimed = claim_spec(&e, "rule:material_expectimax");
        e.f.networks.promote_best("newer").unwrap();
        e.evalc.report_result(&claimed.task_id, "", delta(100, 30));
        assert_eq!(e.f.networks.best_sha().as_deref(), Some("newer"));

        assert_eq!(e.f.networks.status_of("child").as_deref(), Some("archived"));
    }

    #[test]
    fn target_stop_when_strongest_gauge_reaches_goal() {
        let raw = "[eval]\nenabled = true\ngames = 10\nstop_win_rate = 0.8\n";
        let e = env(raw);
        e.f.networks.seed_network("n1", None, "onnx");
        e.f.networks.promote_best("n1").unwrap();
        e.evalc.on_new_best("n1").unwrap();
        let _ = claim_and_report(&e, "rule:material_expectimax", 10, 9);
        let (stopped, reason) = e.ctl.should_stop();
        assert!(stopped);
        assert!(reason.contains("eval_target"), "{reason}");
    }

    #[test]
    fn partial_result_does_not_degrade_full_result() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();
        let _ = claim_and_report(&e, "random", 100, 60);

        e.f.eval
            .upsert_eval_result(eval_row("n", "random", 10, 40, 50, 99))
            .unwrap();
        let r = e.f.eval.get_eval_result("n", "random").unwrap().unwrap();
        assert_eq!(r.num_games, 100);
    }

    #[test]
    fn queue_restore_after_restart() {
        let e = env(enabled_cfg());
        e.f.networks.seed_network("n", None, "onnx");
        e.evalc.on_new_best("n").unwrap();
        let claimed = claim_spec(&e, "random");
        e.evalc.report_result(&claimed.task_id, "", delta(10, 6));

        let evalc2 = EvalCoordinator::new(
            &e.f.ports(),
            Arc::new(Config::parse(enabled_cfg(), "test").unwrap()),
            e.f.clock.clone(),
            e.ctl.clone(),
            e.disp.clone(),
        );
        assert_eq!(evalc2.pending_eval(), 4);

        let job = evalc2
            .running_jobs()
            .into_iter()
            .find(|j| j.opponent_spec == "random")
            .expect("random 作业应恢复");
        assert_eq!(job.num_games, 10);
        assert_eq!(job.target_games, 100);
    }
}
