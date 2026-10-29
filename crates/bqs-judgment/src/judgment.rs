//! 组合判据：停机 / 回滚 / 对打裁决（纯函数，副作用留在 `bqs-core` 调用方）。
//!
//! 输入是已聚合的统计量（胜率序列 / 六桶成对计数 / 配置阈值），输出是结论；
//! 副作用（读库、置位、晋级）一律由调用方执行，使每条判据可用表驱动测试独立锁死。

use crate::sprt::{judge, new_bounds, Hexanomial, Verdict};

/// 统计序列末尾「连续提升不足 eps」的次数。
///
/// 唯一定义处：停机判定与 WebUI 趋势展示共用，避免界面显示的
/// 「无提升次数」与真正用于停机的数字各算一套。
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

/// 「连续 N 次无提升」停机判据：n <= 0 表示判停关闭（只观测不置位）。
pub fn judge_stop(stale: usize, n: i64) -> bool {
    n > 0 && stale as i64 >= n
}

/// 「地板 + 回滚」判据的结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackVerdict {
    /// 未触发：胜率既未比前任回退超 eps，也未跌破地板。
    None,
    /// 击穿地板/回退阈值，但无前任可回退（首代 / 刚开启阶梯）：只记录不清谱系。
    Observe,
    /// 触发回滚：best 指针应退回前任，本网络标 rejected。
    Rollback,
}

impl RollbackVerdict {
    /// true = 谱系写权应执行（best 退回前任 + 标记 rejected）。
    pub fn should_rollback(self) -> bool {
        matches!(self, RollbackVerdict::Rollback)
    }
}

/// 绝对强度「地板 + 回滚」判据。
///
/// 背景：GSPRT 只比较相邻两代，追平期每代小幅提升都可能 accept，但换来的可能是
/// 对规则基线标尺的回退。本判据在 best 的最强标尺（material_expectimax）评测落库
/// 后触发：
/// - 回归回滚：`wr` 比前任 best 低超过 `rollback_eps` → 回滚（`rollback_eps <= 0` 关闭）；
/// - 绝对地板：`floor_win_rate > 0` 且 `wr` 跌破地板 → 同样回滚（0 = 关闭）；
/// - 无前任（`has_parent = false`）但击穿地板 → [`RollbackVerdict::Observe`]。
///
/// 返回（结论，人类可读原因；未触发时为空串）。
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
            format!("对最强标尺胜率 {:.1}% 跌破地板 {:.1}%", 100.0 * wr, 100.0 * floor_win_rate),
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

/// 相对门禁 GSPRT 判定结论（含打满目标局数强制拒绝标记）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MatchJudgment {
    pub verdict: Verdict,
    pub llr: f64,
    /// true = 打满目标局数仍未越界，按预算耗尽强制拒绝（供调用方输出耗尽告警）。
    pub forced: bool,
}

/// 相对门禁 GSPRT 判定（含打满目标局数强制拒绝策略）。
///
/// 策略：累计对局远不足目标时 Continue；打满（`num_games >= 2 × target`，
/// 成对对打故翻倍）仍未越过 SPRT 界时强制 reject——候选未能在预算内给出显著
/// 优势证据即不具备晋级资格，**不按 LLR 符号掷硬币**。
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
        return Ok(MatchJudgment { verdict: Verdict::Continue, llr: 0.0, forced: false });
    }
    let (mut verdict, llr) = judge(p, elo0, elo1, new_bounds(alpha, beta))?;
    let mut forced = false;
    if verdict == Verdict::Continue && num_games >= target_games * 2 {
        verdict = Verdict::RejectH0;
        forced = true;
    }
    Ok(MatchJudgment { verdict, llr, forced })
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
        // 中间的改善会重置计数
        assert_eq!(consecutive_no_progress(&[0.5, 0.51, 0.53, 0.531, 0.532], 0.01), 2);
    }

    #[test]
    fn stop_gating() {
        assert!(!judge_stop(3, 0)); // n=0 关闭
        assert!(!judge_stop(2, 3));
        assert!(judge_stop(3, 3));
        assert!(judge_stop(5, 3));
    }

    #[test]
    fn rollback_paths() {
        // 回归：比前任低超过 eps
        let (v, _) = judge_rollback(0.40, 0.50, true, 0.04, 0.0);
        assert_eq!(v, RollbackVerdict::Rollback);
        // 地板 + 有前任
        let (v, _) = judge_rollback(0.30, 0.31, true, 0.04, 0.5);
        assert_eq!(v, RollbackVerdict::Rollback);
        // 地板 + 无前任 → 只观测
        let (v, _) = judge_rollback(0.30, 0.0, false, 0.04, 0.5);
        assert_eq!(v, RollbackVerdict::Observe);
        // 关闭（eps=0 且 floor=0）
        let (v, _) = judge_rollback(0.10, 0.90, true, 0.0, 0.0);
        assert_eq!(v, RollbackVerdict::None);
        // 小幅回退在容差内
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
        // 打满 800 局（target=400）仍在判定带内 → 强制拒绝
        // 得分约 0.5 的混合计数
        let p = Hexanomial([0, 0, 100, 100, 0, 0]);
        let j = judge_match(p, 400, 800, 0.0, 15.0, 0.05, 0.05).unwrap();
        assert_eq!(j.verdict, Verdict::RejectH0);
        assert!(j.forced);
    }

    #[test]
    fn match_accept_before_exhaustion() {
        // 400 对全胜（800 局，达目标局数）：预算内 LLR 大幅上穿接受界 → 晋级，非强制
        let p = Hexanomial([0, 0, 0, 0, 0, 400]);
        let j = judge_match(p, 400, 800, 0.0, 15.0, 0.05, 0.05).unwrap();
        assert_eq!(j.verdict, Verdict::AcceptH1);
        assert!(!j.forced);
    }
}
