use bqs_proto::{MatchResult, MatchResultAck};

use super::{EvalCoordinator, types::EvalReportAck};
use crate::{
    CoreResult,
    dispatch::RunningTask,
    types::{EvalDelta, EvalResult},
};

impl EvalCoordinator {
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

    pub(super) fn finalize_locked(&self, inner: &mut super::Inner, sha: &str, spec: &str) {
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

        if spec == crate::evalopponents::EVAL_OPPONENT_MATERIAL_EXPECTIMAX {
            self.judge_eval_rollback(sha, sum.win_rate());
            self.judge_eval_target(sum.win_rate());
        }
    }
}
