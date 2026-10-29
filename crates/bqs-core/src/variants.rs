//! 变体对局元信息（唯一真源：`banqi-core/crates/bq-env` 的 `Variant`）。
//!
//! 经跨仓库 path 依赖 `bq-env` 在编译期直接取数（`list_variant_metas()`），
//! 不再有手工生成/拷贝的 `variants.json`——banqi-core 新增变体后，本 crate
//! 重新编译即自动跟进，无需任何再生成步骤。
//! 元信息经 `GetInfo.variant_meta` 下发给训练端（训练端不再本地声明对局元数据）。

use std::sync::LazyLock;

/// 变体对局元信息（与 proto `VariantMeta` 字段一一对应；由调用方转换为
/// proto 消息。字段类型收窄为 proto 侧的 u32/i32）。
#[derive(Debug, Clone)]
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

impl From<bq_env::env::config::VariantMeta> for VariantMeta {
    fn from(m: bq_env::env::config::VariantMeta) -> Self {
        VariantMeta {
            id: m.id.to_string(),
            label: m.label.to_string(),
            rows: m.rows as u32,
            cols: m.cols as u32,
            piece_counts: m.piece_counts.iter().map(|&c| c as u32).collect(),
            piece_values: m.piece_values,
            initial_health: m.initial_health,
            max_consecutive_moves_for_draw: m.max_consecutive_moves_for_draw as u32,
            symmetries: m.symmetries.iter().map(|s| s.to_string()).collect(),
        }
    }
}

pub static VARIANTS: LazyLock<Vec<VariantMeta>> = LazyLock::new(|| {
    bq_env::env::config::list_variant_metas()
        .into_iter()
        .map(VariantMeta::from)
        .collect()
});

/// 按变体 id 查找元信息。
pub fn lookup(variant: &str) -> Option<&'static VariantMeta> {
    VARIANTS.iter().find(|v| v.id == variant)
}

/// 变体词表（与 banqi-core::Variant::as_str() 一致）。
pub fn all_ids() -> Vec<&'static str> {
    bq_env::env::config::Variant::ALL
        .iter()
        .map(|v| v.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_variants_present() {
        for id in ["4x8", "4x8-fast", "4x4", "4x2", "4x8-rook-fast"] {
            assert!(lookup(id).is_some(), "bq-env 缺少变体 {id}");
        }
    }

    #[test]
    fn ids_match_bq_env_table() {
        assert_eq!(all_ids(), VARIANTS.iter().map(|v| v.id.as_str()).collect::<Vec<_>>());
    }

    #[test]
    fn piece_arrays_are_seven_element() {
        for v in VARIANTS.iter() {
            assert_eq!(v.piece_counts.len(), 7, "{}", v.id);
            assert_eq!(v.piece_values.len(), 7, "{}", v.id);
        }
    }

    #[test]
    fn config_values_survive_conversion() {
        let v = lookup("4x8").expect("4x8 存在");
        let cfg = bq_env::env::config::Variant::DarkChess4x8.config();
        assert_eq!(v.rows, cfg.rows as u32);
        assert_eq!(v.cols, cfg.cols as u32);
        assert_eq!(v.initial_health, cfg.initial_health);
        assert_eq!(v.max_consecutive_moves_for_draw, cfg.max_consecutive_moves_for_draw as u32);
        assert!(!v.symmetries.is_empty());
    }
}
