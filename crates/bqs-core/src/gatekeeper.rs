use std::sync::Arc;

use bqs_config::Config;
use bqs_judgment::judgment::judge_match;
use bqs_judgment::{Hexanomial, Verdict};
use bqs_proto::{MatchResult, MatchResultAck};

use crate::{
    CoreError, CoreResult,
    dispatch::RunningTask,
    ports::{MatchStore, NetworkStore, Ports},
    types::network_status,
};

#[derive(Debug, Clone, PartialEq)]
pub struct GateDecision {
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
}

impl Gatekeeper {
    pub fn new(ports: &Ports, cfg: Arc<Config>) -> Self {
        Self {
            cfg,
            matches: ports.matches.clone(),
            nets: ports.networks.clone(),
        }
    }

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

    pub fn accept_report(
        &self,
        match_id: i64,
        delta: Hexanomial,
        games: i64,
    ) -> CoreResult<GateDecision> {
        let Some(m) = self.matches.get_match(match_id)? else {
            return Ok(GateDecision::not_running(match_id));
        };

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

        let mut best_sha = self.current_best_sha()?;

        let concluded = self.matches.conclude_match(m.id)?;
        let mut promoted = false;
        if concluded {
            match judgment.verdict {
                Verdict::AcceptH1 => {
                    self.nets.promote_best(&m.candidate)?;
                    promoted = true;
                    best_sha = m.candidate.clone();
                }
                Verdict::RejectH0 => {
                    self.nets
                        .update_network_status(&m.candidate, network_status::REJECTED)?;
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
        Ok(self.nets.get_best()?.map(|b| b.sha).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::Fakes;
    use bqs_judgment::BUCKET_LL;

    use super::*;

    fn env(config_extra: &str) -> (Fakes, Gatekeeper) {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse(config_extra, "test").unwrap());
        let ports = f.ports();
        let _ctl = Arc::new(crate::control::Control::new(&ports, cfg.clone()));
        let gate = Gatekeeper::new(&ports, cfg);
        (f, gate)
    }

    fn seeded(f: &Fakes) {
        f.networks.seed_network("old_best", None, "onnx");
        f.networks.seed_network("cand", Some("old_best"), "onnx");
        f.networks.promote_best("old_best").unwrap();
    }

    const CFG: &str = "[sprt]\nelo0 = 0.0\nelo1 = 15.0\nalpha = 0.05\nbeta = 0.05\n";

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

        let d = gate
            .accept_report(id, Hexanomial([0, 0, 5, 0, 0, 0]), 10)
            .unwrap();
        assert!(d.accepted && !d.concluded && !d.promoted);
        let m = f.matches.get_match(id).unwrap().unwrap();
        assert_eq!(m.num_games, 10);
        assert!(m.running());
    }

    #[test]
    fn accept_promotes_candidate() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 100);

        let d = gate
            .accept_report(id, Hexanomial([0, 0, 0, 0, 0, 100]), 200)
            .unwrap();
        assert!(d.accepted && d.concluded && d.promoted);
        assert_eq!(d.best_sha, "cand");
        assert_eq!(f.networks.best_sha().as_deref(), Some("cand"));
    }

    #[test]
    fn reject_marks_candidate_rejected() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 100);

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

        let d = gate
            .accept_report(id, Hexanomial([0, 0, 10, 0, 0, 0]), 20)
            .unwrap();
        assert!(d.concluded && !d.promoted);
        assert_eq!(f.networks.status_of("cand").as_deref(), Some("rejected"));
    }

    #[test]
    fn conclude_cas_prevents_double_promotion() {
        let (f, gate) = env(CFG);
        seeded(&f);
        let id = f.matches.seed_match("cand", "old_best", 10);
        f.matches.force_progress(id, 20);

        f.matches.force_conclude(id);
        let d = gate
            .accept_report(id, Hexanomial([1, 0, 0, 0, 0, 0]), 2)
            .unwrap();
        assert!(!d.accepted);
        assert_eq!(d.message, format!("match_{id}_not_running"));
        assert_eq!(f.networks.best_sha().as_deref(), Some("old_best"));

        let m = f.matches.get_match(id).unwrap().unwrap();
        assert_eq!(m.pairs.0[BUCKET_LL], 0);
    }
}
