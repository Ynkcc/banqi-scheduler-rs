//! 变体对局元信息（单一真源：`banqi-core::Variant`）。
//!
//! `resources/variants.json` 由 banqi-core 生成并随二进制内嵌，经
//! `GetInfo.variant_meta` 下发给训练端（训练端不再本地声明对局元数据）。
//! **新增变体后必须重新生成**：
//!
//! ```text
//! cd ../banqi-core && cargo run -p banqi-core --bin variants_meta
//!   > crates/bqs-core/resources/variants.json
//! ```

use std::sync::LazyLock;

use serde::Deserialize;

/// 变体对局元信息（与 proto `VariantMeta` 字段一一对应；此处用自有 struct
/// 承担 JSON 反序列化，由调用方转换为 proto 消息）。
#[derive(Debug, Clone, Deserialize)]
pub struct VariantMeta {
    /// 稳定标识（Variant::as_str()）
    pub id: String,
    /// 显示名
    pub label: String,
    pub rows: u32,
    pub cols: u32,
    /// 7 元，按 PieceType 索引（未激活型为 0）
    pub piece_counts: Vec<u32>,
    /// 7 元，按 PieceType 索引
    pub piece_values: Vec<i32>,
    pub initial_health: i32,
    pub max_consecutive_moves_for_draw: u32,
    /// 合法空间对称集（含 identity，由棋盘形状推导）
    pub symmetries: Vec<String>,
}

pub static VARIANTS: LazyLock<Vec<VariantMeta>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../resources/variants.json"))
        .expect("内嵌 variants.json 格式非法（应由 banqi-core variants_meta 生成）")
});

/// 按变体 id 查找元信息。
pub fn lookup(variant: &str) -> Option<&'static VariantMeta> {
    VARIANTS.iter().find(|v| v.id == variant)
}

/// 变体词表（与 banqi-core::Variant::as_str() 一致）。
pub fn all_ids() -> Vec<&'static str> {
    VARIANTS.iter().map(|v| v.id.as_str()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_variants_present() {
        for id in ["4x8", "4x8-fast", "4x4", "4x2"] {
            assert!(lookup(id).is_some(), "variants.json 缺少 {id}");
        }
    }

    #[test]
    fn piece_arrays_are_seven_element() {
        for v in VARIANTS.iter() {
            assert_eq!(v.piece_counts.len(), 7, "{}", v.id);
            assert_eq!(v.piece_values.len(), 7, "{}", v.id);
        }
    }
}
