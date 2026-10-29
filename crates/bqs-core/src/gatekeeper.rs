//! 【所有权：core-judgment 成员】
//!
//! Gatekeeper：相对门禁 GSPRT 聚合与晋级/拒绝。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/gatekeeper.go`。要点：
//! - matches 表是**纯聚合器**（多 worker 并发贡献计数，SQL 原子累加）；
//! - 判定用 `bqs_judgment::judgment::judge_match`（打满目标局数强制 reject，
//!   **不得优化掉**——候选未能在预算内给出显著优势证据即不具备晋级资格）；
//! - 条件结转（`conclude_match` CAS）防双晋级 → 晋级则 promote_best + 触发
//!   evalc.on_new_best；拒绝则标记 rejected；
//! - 混入模式下本子系统不派发任务（独立 rating 派发已移除，仅兼容显式
//!   TASK_RATING 上报）。
//!
//! 公共 API（供 pipeline 的 reports.rs gate_sink 调用，对齐 Go
//! `Gatekeeper.Aggregate` 语义）：[`Gatekeeper::accept_report`]——
//! 输入 (match_id, 六桶增量, 局数)，输出 [`GateDecision`]（可直接映射
//! `MatchResultAck` 的 accepted/message/match_concluded/promoted/best_sha）。

use std::sync::Arc;

use bqs_config::Config;
use bqs_judgment::judgment::judge_match;
use bqs_judgment::{Hexanomial, Verdict};
use bqs_proto::{MatchResult, MatchResultAck};

use crate::{
    dispatch::RunningTask,
    evalcoord::EvalCoordinator,
    ports::{MatchStore, NetworkStore, Ports},
    types::network_status,
    CoreError, CoreResult,
};

/// 聚合判定回执：字段与 proto `MatchResultAck` 的 gate 分支一一对应。
#[derive(Debug, Clone, PartialEq)]
pub struct GateDecision {
    /// false = match 不存在或已结转（迟到上报，业务拒绝）
    pub accepted: bool,
    pub message: String,
    pub concluded: bool,
    pub promoted: bool,
    pub best_sha: String,
}

impl GateDecision {
    fn not_running(match_id: i64) -> Self {
        Self {
            accepted: false,
            message: format!("match_{match_id}_not_running"),
            concluded: false,
            promoted: false,
            best_sha: String::new(),
        }
    }
}

pub struct Gatekeeper {
    cfg: Arc<Config>,
    matches: Arc<dyn MatchStore>,
    nets: Arc<dyn NetworkStore>,
    evalc: Arc<EvalCoordinator>,
}

impl Gatekeeper {
    pub fn new(ports: &Ports, cfg: Arc<Config>, evalc: Arc<EvalCoordinator>) -> Self {
        Self {
            cfg,
            matches: ports.matches.clone(),
            nets: ports.networks.clone(),
            evalc,
        }
    }

    /// 上报聚合入口（pipeline 的 gate_sink 调用，对齐 Go `Gatekeeper.Aggregate`
    /// 语义）：从 MatchResult 取六桶增量与局数，按 task.match_id 聚合，
    /// 结果直接映射 `MatchResultAck`。
    pub fn aggregate_gating_result(
        &self,
        task: &RunningTask,
        req: &MatchResult,
    ) -> CoreResult<MatchResultAck> {
        let delta = Hexanomial([
            req.pair_ll as i64,
            req.pair_ld as i64,
            req.pair_dd as i64,
            req.pair_wl as i64,
            req.pair_dw as i64,
            req.pair_ww as i64,
        ]);
        let d = self.accept_report(task.match_id, delta, req.games as i64)?;
        Ok(MatchResultAck {
            accepted: d.accepted,
            message: d.message,
            match_concluded: d.concluded,
            promoted: d.promoted,
            best_sha: d.best_sha,
        })
    }

    /// 门禁聚合：SQL 原子累加 → GSPRT 判停 → 条件结转与晋级/拒绝。
    ///
    /// - match 不存在 / 已结转（`add_match_games` 返回 not-running）→ 业务拒绝
    ///   （accepted=false），不走 Err；
    /// - 判定 Continue → 计数已累加，对打继续；
    /// - AcceptH1：conclude CAS 赢家执行 promote_best 并触发 eval 入队；
    /// - RejectH0：conclude CAS 赢家把候选标记 rejected。
    pub fn accept_report(&self, match_id: i64, delta: Hexanomial, games: i64) -> CoreResult<GateDecision> {
        let Some(m) = self.matches.get_match(match_id)? else {
            return Ok(GateDecision::not_running(match_id));
        };
        // SQL 原子累加（WHERE status='running'）：并发同 match 上报不再丢更新
        let (agg, running) = self.matches.add_match_games(m.id, delta, games)?;
        if !running {
            return Ok(GateDecision::not_running(match_id));
        }

        let judgment = judge_match(
            agg.pairs,
            m.target_games,
            agg.num_games,
            self.cfg.sprt.elo0,
            self.cfg.sprt.elo1,
            self.cfg.sprt.alpha,
            self.cfg.sprt.beta,
        )
        .map_err(|e| CoreError::Internal(format!("sprt judge match={} : {e}", m.id)))?;
        if judgment.forced {
            // 打满目标局数仍无结论：无显著优势证据 → 拒绝晋级（对打收敛，不无限对打）
            tracing::info!(
                "[gatekeeper] match={} exhausted target_games={} llr={:.3} forced verdict=reject（未达显著优势）",
                m.id,
                m.target_games,
                judgment.llr
            );
        }
        if judgment.verdict == Verdict::Continue {
            return Ok(GateDecision {
                accepted: true,
                message: String::new(),
                concluded: false,
                promoted: false,
                best_sha: self.current_best_sha()?,
            });
        }

        // 当前 best（晋级回包口径；未晋级时回传原值）
        let mut best_sha = self.current_best_sha()?;
        // 条件结转：并发双判停时只有一个调用方完成 status 迁移（防双晋级）
        let concluded = self.matches.conclude_match(m.id)?;
        let mut promoted = false;
        if concluded {
            match judgment.verdict {
                Verdict::AcceptH1 => {
                    self.nets.promote_best(&m.candidate)?;
                    promoted = true;
                    best_sha = m.candidate.clone();
                    // 新 best 入队绝对强度评估（按 EvalEveryNPromotions 节流）。
                    // 入队失败只告警不回滚晋级（best 指针已切；ensure_best_evaluated
                    // 会在后续 GetTask 路径上补齐缺失的对手）。
                    if let Err(e) = self.evalc.on_new_best(&m.candidate) {
                        tracing::warn!(sha = %m.candidate, error = %e, "晋级后评估入队失败");
                    }
                }
                Verdict::RejectH0 => {
                    self.nets.update_network_status(&m.candidate, network_status::REJECTED)?;
                }
                Verdict::Continue => unreachable!("Continue 已提前返回"),
            }
        }
        tracing::info!(
            "[gatekeeper] match={} concluded verdict={} llr={:.3} pairs={} promoted={promoted}",
            m.id,
            judgment.verdict.as_str(),
            judgment.llr,
            agg.pairs.labeled()
        );
        Ok(GateDecision {
            accepted: true,
            message: String::new(),
            concluded: true,
            promoted,
            best_sha,
        })
    }

    fn current_best_sha(&self) -> CoreResult<String> {
        Ok(self
            .nets
            .get_best()?
            .map(|b| b.sha)
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use crate::dispatch::TaskDispatcher;
    use crate::test_support::Fakes;
    use bqs_judgment::BUCKET_LL;

    use super::*;

    fn env(config_extra: &str) -> (Fakes, Gatekeeper) {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse(config_extra, "test").unwrap());
        let ports = f.ports();
        let ctl = Arc::new(crate::control::Control::new(&ports, cfg.clone()));
        let disp = Arc::new(TaskDispatcher::new(&ports, cfg.clone(), f.clock.clone()));
        let evalc = Arc::new(EvalCoordinator::new(
            &ports,
            cfg.clone(),
            f.clock.clone(),
            ctl,
            disp,
        ));
        let gate = Gatekeeper::new(&ports, cfg, evalc);
        (f, gate)
    }

    /// 预置谱系：old_best 在位，cand 已登记（gatekeeper 对打的双方）。
    fn seeded(f: &Fakes) {
        f.networks.seed_network("old_best", None, "onnx");
        f.networks.seed_network("cand", Some("old_best"), "onnx");
        f.networks.promote_best("old_best");
    }

    const CFG: &str = "[eval]\nenabled = true\n[sprt]\nelo0 = 0.0\nelo1 = 15.0\nalpha = 0.05\nbeta = 0.05\n";

    #[test]
    fn unknown_match_rejected_as_business() {
        let (_f, gate) = env(CFG);
        let d = gate.accept_report(42, Hexanomial::ZERO, 0).unwrap();
        assert!(!d.accepted);
        assert_eq!(d.message, "match_42_not_running");
    }

    #[test]
    fn continue_while_no_verdict() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 100);
        // 少量 DD（≈50% 得分）：LLR 接近 0 → 继续
        let d = gate.accept_report(id, Hexanomial([0, 0, 5, 0, 0, 0]), 10).unwrap();
        assert!(d.accepted && !d.concluded && !d.promoted);
        let m = f.matches.get_match(id).unwrap().unwrap();
        assert_eq!(m.num_games, 10);
        assert!(m.running());
    }

    #[test]
    fn accept_promotes_and_triggers_eval() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 100);
        // 100 对全 WW → LLR 大幅上穿接受界
        let d = gate.accept_report(id, Hexanomial([0, 0, 0, 0, 0, 100]), 200).unwrap();
        assert!(d.accepted && d.concluded && d.promoted);
        assert_eq!(d.best_sha, "cand");
        assert_eq!(f.networks.best_sha().as_deref(), Some("cand"));
        // 晋级触发 eval 入队（4 个默认对手）
        assert_eq!(f.eval.load_eval_queue().unwrap().len(), 4);
    }

    #[test]
    fn reject_marks_candidate_rejected() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 100);
        // 100 对全 LL → 强制拒绝路径之外的显著劣势
        let d = gate
            .accept_report(id, Hexanomial([100, 0, 0, 0, 0, 0]), 200)
            .unwrap();
        assert!(d.accepted && d.concluded && !d.promoted);
        assert_eq!(f.networks.best_sha().as_deref(), Some("old_best"));
        assert_eq!(f.networks.status_of("cand").as_deref(), Some("rejected"));
    }

    #[test]
    fn forced_reject_on_exhaustion() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 10);
        // 打满 20 局（target=10 对）仍全 DD ≈ 判定带内 → 强制 reject
        let d = gate.accept_report(id, Hexanomial([0, 0, 10, 0, 0, 0]), 20).unwrap();
        assert!(d.concluded && !d.promoted);
        assert_eq!(f.networks.status_of("cand").as_deref(), Some("rejected"));
    }

    #[test]
    fn conclude_cas_prevents_double_promotion() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 10);
        f.matches.force_progress(id, 20);
        // 直接把 match 置为已结转（模拟并发赢家已结转）
        f.matches.force_conclude(id);
        let d = gate
            .accept_report(id, Hexanomial([1, 0, 0, 0, 0, 0]), 2)
            .unwrap();
        assert!(!d.accepted);
        assert_eq!(d.message, format!("match_{id}_not_running"));
        assert_eq!(f.networks.best_sha().as_deref(), Some("old_best"));
        // 计数未被污染（LL 桶未累加：get_match 后 add 前置校验 running）
        let m = f.matches.get_match(id).unwrap().unwrap();
        assert_eq!(m.pairs.0[BUCKET_LL], 0);
    }
}
