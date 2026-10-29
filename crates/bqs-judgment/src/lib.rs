//! 判定引擎：**判据单一真源**。全部为纯函数（无 I/O、无锁、无时钟依赖），
//! 副作用（读库 / 置位 / 晋级）留在 `bqs-core` 调用方；判据语义用表驱动测试锁死。
//!
//! 模块划分：
//! - [`sprt`]：六桶成对计数 + 广义 SPRT（正态近似 LLR）——相对门禁的数学核心；
//! - [`judgment`]：组合判据（停机 / 回滚 / 对打判定），包装 sprt 与阈值口径。

pub mod judgment;
pub mod sprt;

pub use sprt::{judge, new_bounds, Bounds, Hexanomial, Verdict, BUCKET_DD, BUCKET_DW, BUCKET_LL,
               BUCKET_LD, BUCKET_WL, BUCKET_WW};
