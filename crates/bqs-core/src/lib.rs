//! banqi-scheduler 领域内核（Rust 重构版）。
//!
//! 架构：**Ports & Adapters（六边形）**。
//! - `ports`：内核对外部世界（元数据存储 / 对象签名 / 时钟）的抽象端口，
//!   全部为同步 trait（负载特征：SQLite 短事务 + 字符串签名运算，阻塞开销可忽略；
//!   传输层负责把调用放入阻塞线程池）；
//! - 各子系统独立持锁（`std::sync::Mutex`），无后台任务——超时回收、平衡评估等
//!   都是请求路径上的惰性计算（带节流），与 Go 版语义一致；
//! - **跨子系统锁序约定：`evalc.mu → disp.mu`**（evalc 是唯一允许在持有期间
//!   获取其他子系统锁的对象；其余子系统一律是叶子，禁止反向持锁）；
//! - `server`：gRPC 契约编排门面（组装根），自身无业务状态与锁。
//!
//! 契约兼容性承诺（对 collector / trainer / WebUI 三端零改动）：
//! - gRPC 消息与字段 = `proto/scheduler.proto`（唯一真源）；
//! - SQLite 表结构 = Go 版迁移链终态（bqs-store 迁移链 v1..v8 可直接打开旧库）；
//! - WebUI JSON 字段名与 Go 版 `/api/*` 完全一致（bqs-api 侧锁定）。
//!
//! 业务错误约定：**面向客户端的拒绝走正常回包（accepted=false + message），
//! 不走 Err**——旧版客户端把传输层错误当作可无限重试的故障刷屏（Go 版教训）。

pub mod consts;
pub mod ports;
pub mod types;
pub mod variants;

// —— 子系统（文件头注释标注所有权）——
pub mod assembler;
pub mod balance;
pub mod control;
pub mod dispatch;
pub mod evalcoord;
pub mod evalopponents;
pub mod featurepack;
pub mod gatekeeper;
pub mod jobs;
pub mod reanalysis;
pub mod reports;
pub mod sources;
pub mod trainconfig;

// —— 组装根与 RPC 门面 ——
pub mod server;

/// 共享内存 fake ports（仅测试编译；团队约定：bqs-core 单测统一用这套，
/// 追加实现请改本文件，勿另建）。
#[cfg(test)]
pub mod test_support;

/// 内核错误：仅表示「真故障」（存储失败 / 签名失败 / 内部不变量破坏），
/// 会映射为 gRPC `Status::internal`；业务拒绝不产生此类型。
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("store: {0}")]
    Store(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type CoreResult<T> = Result<T, CoreError>;
