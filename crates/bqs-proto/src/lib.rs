//! gRPC 契约层：`proto/scheduler.proto` 是全仓唯一真源（与 banqi-collector /
//! banqi-training 共享同一份文件，禁止再复制）。
//!
//! 字段号只增不改；破坏性变更必须提升 schema_version。
//! 本 crate 只做代码生成与少量类型助手，不含任何业务逻辑。

pub mod pb {
    tonic::include_proto!("scheduler");
}

pub use pb::*;
