use serde::Serialize;

pub const BUCKET_LL: usize = 0;
pub const BUCKET_LD: usize = 1;
pub const BUCKET_DD: usize = 2;
pub const BUCKET_WL: usize = 3;
pub const BUCKET_DW: usize = 4;
pub const BUCKET_WW: usize = 5;

const BUCKET_SCORES: [f64; 6] = [0.0, 0.5, 1.0, 1.0, 1.5, 2.0];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Hexanomial(pub [i64; 6]);

impl Hexanomial {
    pub const ZERO: Hexanomial = Hexanomial([0; 6]);

    pub fn total(self) -> i64 {
        self.0.iter().sum()
    }

    pub fn wins(self) -> i64 {
        self.0[BUCKET_WL] + self.0[BUCKET_DW] + 2 * self.0[BUCKET_WW]
    }

    pub fn losses(self) -> i64 {
        self.0[BUCKET_WL] + self.0[BUCKET_LD] + 2 * self.0[BUCKET_LL]
    }

    pub fn draws(self) -> i64 {
        2 * self.0[BUCKET_DD] + self.0[BUCKET_LD] + self.0[BUCKET_DW]
    }

    pub fn labeled(self) -> String {
        let [ll, ld, dd, wl, dw, ww] = self.0;
        format!("[LL={ll}, LD={ld}, DD={dd}, WL={wl}, DW={dw}, WW={ww}]")
    }

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

        let var_pair = (sum_sq - n as f64 * mean * mean) / (n - 1) as f64;

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

#[derive(Debug, thiserror::Error)]
pub enum LlrError {
    #[error("{0}")]
    Stats(#[from] SprtError),
}

pub fn llr(p: Hexanomial, elo0: f64, elo1: f64) -> Result<f64, LlrError> {
    let (s, variance) = p.stats()?;
    let s0 = score_from_elo(elo0);
    let s1 = score_from_elo(elo1);

    let variance = if !(variance > 0.0) { 0.1 } else { variance };
    let n = p.total() as f64;
    Ok(n * (s - (s0 + s1) / 2.0) * (s1 - s0) / variance)
}

#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    pub lower: f64,

    pub upper: f64,
}

pub fn new_bounds(alpha: f64, beta: f64) -> Bounds {
    Bounds {
        lower: (beta / (1.0 - alpha)).ln(),
        upper: ((1.0 - beta) / alpha).ln(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Continue,

    AcceptH1,

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

pub fn judge(
    p: Hexanomial,
    elo0: f64,
    elo1: f64,
    bounds: Bounds,
) -> Result<(Verdict, f64), LlrError> {
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
        let p = Hexanomial([0, 1, 2, 3, 4, 5]);
        assert_eq!(p.total(), 15);
        assert_eq!(p.wins(), 3 + 4 + 10);
        assert_eq!(p.losses(), 3 + 1 + 0);
        assert_eq!(p.draws(), 4 + 1 + 4);

        assert_eq!(p.wins() + p.losses() + p.draws(), 2 * p.total());
    }

    #[test]
    fn strong_candidate_accepts() {
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
        let p = Hexanomial([0, 0, 10, 0, 0, 0]);
        let (v, _) = judge(p, 0.0, 15.0, new_bounds(0.05, 0.05)).unwrap();
        assert_eq!(v, Verdict::Continue);
    }

    #[test]
    fn wl_mixed_pairs_negative_llr() {
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
        let p = Hexanomial([0, 0, 0, 0, 0, 1]);
        assert!(llr(p, 0.0, 15.0).is_ok());
    }

    #[test]
    fn single_pair_nan_variance_floored() {
        let p = Hexanomial([0, 0, 0, 1, 0, 0]);
        let l = llr(p, 0.0, 15.0).unwrap();
        assert!(l.is_finite(), "llr = {l}");
    }
}
