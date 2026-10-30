use bqs_proto::TaskKind;

use super::{EvalCoordinator, types::{ClaimedEvalTask, PendingEval}};
use crate::{
    assembler::games_for_threads,
    consts::{EVAL_MAX_ATTEMPTS, EVAL_MAX_GAMES_PER_TASK, EVAL_TASK_STALE_AFTER},
    dispatch::RunningTask,
    sources::new_id,
};

impl EvalCoordinator {
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
            let task_id = new_id();

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
}
