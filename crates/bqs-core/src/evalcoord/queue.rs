use super::{
    SETTING_EVAL_PROMOTIONS, EvalCoordinator, Inner, eval_search_mode_name,
    types::PendingEval,
};
use crate::CoreResult;

impl EvalCoordinator {
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

    pub(super) fn restore_queue(&self) {
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

    pub(super) fn enqueue_locked(&self, inner: &mut Inner, sha: &str, specs: &[String]) -> i64 {
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
}
