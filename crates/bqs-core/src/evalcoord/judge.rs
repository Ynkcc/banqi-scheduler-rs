use bqs_judgment::judgment::{
    RollbackVerdict, consecutive_no_progress, judge_rollback, judge_stop,
};

use super::EvalCoordinator;
use crate::evalopponents::EVAL_OPPONENT_MATERIAL_EXPECTIMAX;

impl EvalCoordinator {
    pub(super) fn judge_eval_progress(&self, spec: &str) {
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

    pub(super) fn judge_eval_rollback(&self, sha: &str, wr: f64) {
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

    pub(super) fn judge_eval_target(&self, wr: f64) {
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
}
