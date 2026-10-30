use bqs_judgment::judgment::consecutive_no_progress;
use bqs_proto::TaskKind;

use super::{EvalCoordinator, types::{EvalProgress, EvalRunningJob}};
use crate::consts::EVAL_TASK_STALE_AFTER;

impl EvalCoordinator {
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
