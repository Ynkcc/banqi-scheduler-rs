pub mod judgment;
pub mod sprt;

pub use sprt::{
    judge, new_bounds, Bounds, Hexanomial, Verdict, BUCKET_DD, BUCKET_DW, BUCKET_LD, BUCKET_LL,
    BUCKET_WL, BUCKET_WW,
};
