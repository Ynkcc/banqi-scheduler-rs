pub mod judgment;
pub mod sprt;

pub use sprt::{
    BUCKET_DD, BUCKET_DW, BUCKET_LD, BUCKET_LL, BUCKET_WL, BUCKET_WW, Bounds, Hexanomial, Verdict,
    judge, new_bounds,
};
