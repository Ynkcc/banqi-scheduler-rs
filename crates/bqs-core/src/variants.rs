use std::sync::LazyLock;

#[derive(Debug, Clone)]
pub struct VariantMeta {
    pub id: String,

    pub label: String,
    pub rows: u32,
    pub cols: u32,

    pub piece_counts: Vec<u32>,

    pub piece_values: Vec<i32>,
    pub initial_health: i32,
    pub max_consecutive_moves_for_draw: u32,

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

pub fn lookup(variant: &str) -> Option<&'static VariantMeta> {
    VARIANTS.iter().find(|v| v.id == variant)
}

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
        assert_eq!(
            all_ids(),
            VARIANTS.iter().map(|v| v.id.as_str()).collect::<Vec<_>>()
        );
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
        assert_eq!(
            v.max_consecutive_moves_for_draw,
            cfg.max_consecutive_moves_for_draw as u32
        );
        assert!(!v.symmetries.is_empty());
    }
}
