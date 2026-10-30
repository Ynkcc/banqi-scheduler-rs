use crate::sprt::{judge, new_bounds, Hexanomial, Verdict};

pub fn consecutive_no_progress(rates: &[f64], eps: f64) -> usize {
    let mut stale = 0;
    for i in 1..rates.len() {
        if rates[i] - rates[i - 1] < eps {
            stale += 1;
        } else {
            stale = 0;
        }
    }
    stale
}

pub fn judge_stop(stale: usize, n: i64) -> bool {
    n > 0 && stale as i64 >= n
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackVerdict {
    None,

    Observe,

    Rollback,
}

impl RollbackVerdict {
    pub fn should_rollback(self) -> bool {
        matches!(self, RollbackVerdict::Rollback)
    }
}

pub fn judge_rollback(
    wr: f64,
    parent_wr: f64,
    has_parent: bool,
    rollback_eps: f64,
    floor_win_rate: f64,
) -> (RollbackVerdict, String) {
    let regressed = has_parent && rollback_eps > 0.0 && wr < parent_wr - rollback_eps;
    let below_floor = floor_win_rate > 0.0 && wr < floor_win_rate;
    match (regressed, below_floor, has_parent) {
        (true, _, _) => (
            RollbackVerdict::Rollback,
            format!(
                "对最强标尺胜率 {:.1}% 比前任 {:.1}% 回退超 {:.1}pt",
                100.0 * wr,
                100.0 * parent_wr,
                100.0 * rollback_eps
            ),
        ),
        (false, true, true) => (
            RollbackVerdict::Rollback,
            format!(
                "对最强标尺胜率 {:.1}% 跌破地板 {:.1}%",
                100.0 * wr,
                100.0 * floor_win_rate
            ),
        ),
        (false, true, false) => (
            RollbackVerdict::Observe,
            format!(
                "对最强标尺胜率 {:.1}% 跌破地板 {:.1}%，但无前任可回滚（仅记录）",
                100.0 * wr,
                100.0 * floor_win_rate
            ),
        ),
        (false, false, _) => (RollbackVerdict::None, String::new()),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MatchJudgeError {
    #[error("{0}")]
    Sprt(#[from] crate::sprt::LlrError),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MatchJudgment {
    pub verdict: Verdict,
    pub llr: f64,

    pub forced: bool,
}

pub fn judge_match(
    p: Hexanomial,
    target_games: i64,
    num_games: i64,
    elo0: f64,
    elo1: f64,
    alpha: f64,
    beta: f64,
) -> Result<MatchJudgment, MatchJudgeError> {
    if num_games < target_games * 2 && p.total() < target_games {
        return Ok(MatchJudgment {
            verdict: Verdict::Continue,
            llr: 0.0,
            forced: false,
        });
    }
    let (mut verdict, llr) = judge(p, elo0, elo1, new_bounds(alpha, beta))?;
    let mut forced = false;
    if verdict == Verdict::Continue && num_games >= target_games * 2 {
        verdict = Verdict::RejectH0;
        forced = true;
    }
    Ok(MatchJudgment {
        verdict,
        llr,
        forced,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_progress_counts_trailing_run() {
        assert_eq!(consecutive_no_progress(&[], 0.01), 0);
        assert_eq!(consecutive_no_progress(&[0.5], 0.01), 0);
        assert_eq!(consecutive_no_progress(&[0.5, 0.51, 0.52], 0.01), 0);
        assert_eq!(consecutive_no_progress(&[0.5, 0.52, 0.521, 0.522], 0.01), 2);

        assert_eq!(
            consecutive_no_progress(&[0.5, 0.51, 0.53, 0.531, 0.532], 0.01),
            2
        );
    }

    #[test]
    fn stop_gating() {
        assert!(!judge_stop(3, 0));
        assert!(!judge_stop(2, 3));
        assert!(judge_stop(3, 3));
        assert!(judge_stop(5, 3));
    }

    #[test]
    fn rollback_paths() {
        let (v, _) = judge_rollback(0.40, 0.50, true, 0.04, 0.0);
        assert_eq!(v, RollbackVerdict::Rollback);

        let (v, _) = judge_rollback(0.30, 0.31, true, 0.04, 0.5);
        assert_eq!(v, RollbackVerdict::Rollback);

        let (v, _) = judge_rollback(0.30, 0.0, false, 0.04, 0.5);
        assert_eq!(v, RollbackVerdict::Observe);

        let (v, _) = judge_rollback(0.10, 0.90, true, 0.0, 0.0);
        assert_eq!(v, RollbackVerdict::None);

        let (v, _) = judge_rollback(0.47, 0.50, true, 0.04, 0.0);
        assert_eq!(v, RollbackVerdict::None);
    }

    #[test]
    fn match_under_budget_continues() {
        let j = judge_match(Hexanomial::ZERO, 400, 0, 0.0, 15.0, 0.05, 0.05).unwrap();
        assert_eq!(j.verdict, Verdict::Continue);
        assert!(!j.forced);
    }

    #[test]
    fn match_forced_reject_on_exhaustion() {
        let p = Hexanomial([0, 0, 100, 100, 0, 0]);
        let j = judge_match(p, 400, 800, 0.0, 15.0, 0.05, 0.05).unwrap();
        assert_eq!(j.verdict, Verdict::RejectH0);
        assert!(j.forced);
    }

    #[test]
    fn match_accept_before_exhaustion() {
        let p = Hexanomial([0, 0, 0, 0, 0, 400]);
        let j = judge_match(p, 400, 800, 0.0, 15.0, 0.05, 0.05).unwrap();
        assert_eq!(j.verdict, Verdict::AcceptH1);
        assert!(!j.forced);
    }
}
