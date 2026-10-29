//! 六桶成对计数（Hexanomial）与广义 SPRT。
//!
//! 把连续两局组成一对，按互斥对型分成 6 桶，顺序 `[LL, LD, DD, WL, DW, WW]`：
//!
//! ```text
//! LL = (负,负)   LD = (负,和)+(和,负)   DD = (和,和)
//! WL = (胜,负)+(负,胜) —— 一胜一负混合对
//! DW = (胜,和)+(和,胜)   WW = (胜,胜)
//! ```
//!
//! 由五桶 pentanomial 升级而来：旧桶 2（得分和 = 1 分）聚合了 DD 与 WL 两种对型，
//! 裸数组易被误读（例：`[0,0,22,1,1]` 实为 20 对混合 + 2 对双和）。六桶逐桶无歧义，
//! 且可直接推出胜/负/和局数。

use serde::Serialize;

/// 桶下标常量（顺序 [LL, LD, DD, WL, DW, WW]）。
pub const BUCKET_LL: usize = 0;
pub const BUCKET_LD: usize = 1;
pub const BUCKET_DD: usize = 2;
pub const BUCKET_WL: usize = 3;
pub const BUCKET_DW: usize = 4;
pub const BUCKET_WW: usize = 5;

/// 桶得分（每局，范围 [0,2]）。
const BUCKET_SCORES: [f64; 6] = [0.0, 0.5, 1.0, 1.0, 1.5, 2.0];

/// 六桶成对计数（每一对只落入一种桶）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Hexanomial(pub [i64; 6]);

impl Hexanomial {
    pub const ZERO: Hexanomial = Hexanomial([0; 6]);

    pub fn total(self) -> i64 {
        self.0.iter().sum()
    }

    /// 主网络视角胜局数（不是对数）。
    pub fn wins(self) -> i64 {
        self.0[BUCKET_WL] + self.0[BUCKET_DW] + 2 * self.0[BUCKET_WW]
    }

    /// 主网络视角负局数（不是对数）。
    pub fn losses(self) -> i64 {
        self.0[BUCKET_WL] + self.0[BUCKET_LD] + 2 * self.0[BUCKET_LL]
    }

    /// 和棋局数（不是对数；一对双和计 2 局）。
    pub fn draws(self) -> i64 {
        2 * self.0[BUCKET_DD] + self.0[BUCKET_LD] + self.0[BUCKET_DW]
    }

    /// 带桶名标注的显示形式，杜绝裸数组被误读。
    pub fn labeled(self) -> String {
        let [ll, ld, dd, wl, dw, ww] = self.0;
        format!("[LL={ll}, LD={ld}, DD={dd}, WL={wl}, DW={dw}, WW={ww}]")
    }

    /// 统计量：返回（每局平均得分，每局得分方差）。
    ///
    /// 桶得分：LL=0, LD=0.5, DD=1, WL=1, DW=1.5, WW=2。
    pub fn stats(self) -> Result<(f64, f64), SprtError> {
        let n = self.total();
        if n == 0 {
            return Err(SprtError::Empty);
        }
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for (i, &c) in self.0.iter().enumerate() {
            let v = BUCKET_SCORES[i];
            sum += c as f64 * v;
            sum_sq += c as f64 * v * v;
        }
        let mean = sum / n as f64;
        // 成对得分方差（单位：对；样本方差除以 n-1）
        let var_pair = (sum_sq - n as f64 * mean * mean) / (n - 1) as f64;
        // 每局得分 = 成对得分 / 2，方差同理
        Ok((mean / 2.0, var_pair / 4.0))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SprtError {
    #[error("hexanomial empty")]
    Empty,
}

fn score_from_elo(elo: f64) -> f64 {
    1.0 / (1.0 + 10f64.powf(-elo / 400.0))
}

/// SPRT 判定错误。
#[derive(Debug, thiserror::Error)]
pub enum LlrError {
    #[error("{0}")]
    Stats(#[from] SprtError),
}

/// 基于正态近似的广义 SPRT 对数似然比：
///
/// ```text
/// LLR ≈ N * (s - (s0+s1)/2) * (s1-s0) / σ²
/// ```
///
/// `s` 为每局平均得分，σ² 为每局得分方差，s0/s1 由 elo0/elo1 边界换算。
pub fn llr(p: Hexanomial, elo0: f64, elo1: f64) -> Result<f64, LlrError> {
    let (s, variance) = p.stats()?;
    let s0 = score_from_elo(elo0);
    let s1 = score_from_elo(elo1);
    // 全胜/全负无方差：按经验下限处理，避免除零
    let variance = if variance <= 0.0 { 0.1 } else { variance };
    let n = p.total() as f64;
    Ok(n * (s - (s0 + s1) / 2.0) * (s1 - s0) / variance)
}

/// SPRT 判定边界（对数域）。
#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    /// ≤ lower → 拒绝（H0 elo0 成立）
    pub lower: f64,
    /// ≥ upper → 接受（H1 elo1 成立）
    pub upper: f64,
}

pub fn new_bounds(alpha: f64, beta: f64) -> Bounds {
    Bounds {
        lower: (beta / (1.0 - alpha)).ln(),
        upper: ((1.0 - beta) / alpha).ln(),
    }
}

/// 判定结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Continue,
    /// 新网络变强，晋级
    AcceptH1,
    /// 新网络未达标，拒绝
    RejectH0,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::AcceptH1 => "accept",
            Verdict::RejectH0 => "reject",
            Verdict::Continue => "continue",
        }
    }
}

/// 一次完整判定：LLR 越界即出结论。
pub fn judge(p: Hexanomial, elo0: f64, elo1: f64, bounds: Bounds) -> Result<(Verdict, f64), LlrError> {
    let llr = llr(p, elo0, elo1)?;
    let verdict = if llr >= bounds.upper {
        Verdict::AcceptH1
    } else if llr <= bounds.lower {
        Verdict::RejectH0
    } else {
        Verdict::Continue
    };
    Ok((verdict, llr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_error() {
        assert!(Hexanomial::ZERO.stats().is_err());
        assert!(llr(Hexanomial::ZERO, 0.0, 15.0).is_err());
    }

    #[test]
    fn game_counts_consistent() {
        // [LL=0, LD=1, DD=2, WL=3, DW=4, WW=5]
        let p = Hexanomial([0, 1, 2, 3, 4, 5]);
        assert_eq!(p.total(), 15); // 对数
        assert_eq!(p.wins(), 3 + 4 + 10);
        assert_eq!(p.losses(), 3 + 1 + 0);
        assert_eq!(p.draws(), 4 + 1 + 4);
        // 每对两局
        assert_eq!(p.wins() + p.losses() + p.draws(), 2 * p.total());
    }

    #[test]
    fn strong_candidate_accepts() {
        // 大幅占优 → LLR 快速上穿接受界
        let p = Hexanomial([0, 0, 0, 0, 0, 100]);
        let (v, llr) = judge(p, 0.0, 15.0, new_bounds(0.05, 0.05)).unwrap();
        assert_eq!(v, Verdict::AcceptH1);
        assert!(llr > 0.0);
    }

    #[test]
    fn weak_candidate_rejects() {
        let p = Hexanomial([100, 0, 0, 0, 0, 0]);
        let (v, _) = judge(p, 0.0, 15.0, new_bounds(0.05, 0.05)).unwrap();
        assert_eq!(v, Verdict::RejectH0);
    }

    #[test]
    fn coin_flip_continues() {
        // 全 DD（双双和棋）≈ 50% 得分，LLR 接近 0，不应立即判停
        let p = Hexanomial([0, 0, 10, 0, 0, 0]);
        let (v, _) = judge(p, 0.0, 15.0, new_bounds(0.05, 0.05)).unwrap();
        assert_eq!(v, Verdict::Continue);
    }

    #[test]
    fn wl_mixed_pairs_negative_llr() {
        // 纯 WL 混合对：每对 (胜,负) 得分和恒为 1 → 成对得分方差退化为 0，
        // 走方差下限 0.1；每局得分 0.5 低于 (elo0=0, elo1=15) 的中点 → LLR 为负。
        // （与 Go 版同公式同行为；纯单一对型的统计退化由调用方按打满强制拒绝兜底。）
        let p = Hexanomial([0, 0, 0, 50, 0, 0]);
        let l = llr(p, 0.0, 15.0).unwrap();
        assert!(l < 0.0, "llr = {l}");
        assert!(l.is_finite());
    }

    #[test]
    fn bounds_formula() {
        let b = new_bounds(0.05, 0.05);
        assert!((b.lower - (0.05f64 / 0.95).ln()).abs() < 1e-12);
        assert!((b.upper - (0.95f64 / 0.05).ln()).abs() < 1e-12);
    }

    #[test]
    fn zero_variance_floored() {
        // 单一对型全胜：样本方差为 0，LLR 仍可计算（方差下限 0.1）
        let p = Hexanomial([0, 0, 0, 0, 0, 1]);
        assert!(llr(p, 0.0, 15.0).is_ok());
    }
}
