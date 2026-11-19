use std::collections::BTreeMap;

use serde::Serialize;

pub const SETTING_TRAIN_CONFIG: &str = "train_config";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrainConfigKind {
    Float,
    Int,
    Bool,
    Enum,
}

impl TrainConfigKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TrainConfigKind::Float => "float",
            TrainConfigKind::Int => "int",
            TrainConfigKind::Bool => "bool",
            TrainConfigKind::Enum => "enum",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TrainConfigSpec {
    pub kind: TrainConfigKind,

    pub min: f64,

    pub max: f64,
    pub has_max: bool,

    pub gt_zero: bool,

    pub enum_values: &'static [&'static str],
}

const fn f(min: f64, max: f64) -> TrainConfigSpec {
    TrainConfigSpec {
        kind: TrainConfigKind::Float,
        min,
        max,
        has_max: true,
        gt_zero: false,
        enum_values: &[],
    }
}

const fn i(min: f64) -> TrainConfigSpec {
    TrainConfigSpec {
        kind: TrainConfigKind::Int,
        min,
        max: 0.0,
        has_max: false,
        gt_zero: false,
        enum_values: &[],
    }
}

const fn b() -> TrainConfigSpec {
    TrainConfigSpec {
        kind: TrainConfigKind::Bool,
        min: 0.0,
        max: 0.0,
        has_max: false,
        gt_zero: false,
        enum_values: &[],
    }
}

static SPECS: &[(&str, TrainConfigSpec)] = &[
    ("LEARNING_RATE", f(0.0, 1.0)),
    ("MIN_LR", f(0.0, 1.0)),
    ("LR_DECAY_STEPS", i(1.0)),
    ("LR_DECAY_ROUNDS", i(0.0)),
    ("TRAIN_BATCH", i(1.0)),
    ("TRAIN_EPOCHS_PER_ROUND", i(1.0)),
    ("MIN_NEW_SAMPLES_TO_TRAIN", i(0.0)),
    ("WEIGHT_DECAY", f(0.0, 1.0)),
    ("EMA_DECAY", f(0.0, 1.0)),
    ("RECENT_SAMPLE_ENABLED", b()),
    ("FAST_SAMPLE_LOSS_WEIGHT", f(0.0, 1.0)),
    (
        "VALUE_TARGET_MODE",
        TrainConfigSpec {
            kind: TrainConfigKind::Enum,
            min: 0.0,
            max: 0.0,
            has_max: false,
            gt_zero: false,
            enum_values: &["mcts", "game", "completed_q", "mixed", "anneal", "game_hp"],
        },
    ),
    ("VALUE_TARGET_ANNEAL_ROUNDS", i(0.0)),
    ("VALUE_MIX_GAME_WEIGHT", f(0.0, 1.0)),
    (
        "POLICY_TARGET_TEMPERATURE",
        TrainConfigSpec {
            kind: TrainConfigKind::Float,
            min: 0.0,
            max: 0.0,
            has_max: false,
            gt_zero: true,
            enum_values: &[],
        },
    ),
    ("POLICY_TARGET_ACTION_MIX", f(0.0, 1.0)),
    (
        "VALUE_GAUSS_SIGMA",
        TrainConfigSpec {
            kind: TrainConfigKind::Float,
            min: 0.0,
            max: 1.0,
            has_max: true,
            gt_zero: true,
            enum_values: &[],
        },
    ),
    ("DATA_AUGMENT_ENABLED", b()),
    ("DATA_AUGMENT_K", i(1.0)),
    ("DATA_AUGMENT_KEEP_ORIGINAL", b()),
    ("REANALYSIS_BATCH_POSITIONS", i(1.0)),
    ("REANALYSIS_SUBMIT_EVERY_N_ROUNDS", i(1.0)),
    ("REANALYSIS_MCTS_SIMS", i(0.0)),
    ("MAX_RUNTIME_SECONDS", i(0.0)),
    ("SHOULD_STOP_POLL_SECONDS", i(0.0)),
    ("CKPT_EXPORT_EVERY", i(1.0)),
    ("NNUE_EXPORT_EVERY_ROUNDS", i(1.0)),
    ("NNUE_EXPORT_GATE", b()),
];

#[derive(Debug, Clone, Serialize)]
pub struct TrainConfigField {
    pub name: String,
    pub kind: String,
    pub min: f64,
    pub max: f64,
    #[serde(rename = "hasMax")]
    pub has_max: bool,
    #[serde(rename = "gtZero")]
    pub gt_zero: bool,
    #[serde(rename = "enum", skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<String>,
}

pub struct TrainConfig;

impl TrainConfig {
    pub fn fields() -> Vec<TrainConfigField> {
        Self::field_names()
            .into_iter()
            .map(|name| {
                let spec = Self::spec_for(&name).expect("spec 存在");
                TrainConfigField {
                    name,
                    kind: spec.kind.as_str().to_string(),
                    min: spec.min,
                    max: spec.max,
                    has_max: spec.has_max,
                    gt_zero: spec.gt_zero,
                    enum_values: spec.enum_values.iter().map(|s| s.to_string()).collect(),
                }
            })
            .collect()
    }

    pub fn field_names() -> Vec<String> {
        let mut names: Vec<String> = SPECS.iter().map(|(n, _)| n.to_string()).collect();
        names.sort();
        names
    }

    pub fn normalize(input: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>, String> {
        let mut out = BTreeMap::new();
        for (name, raw) in input {
            let Some(spec) = Self::spec_for(name) else {
                return Err(format!(
                    "字段 {name:?} 不可远程调节；可调字段: {}",
                    Self::field_names().join(" ")
                ));
            };
            let value = Self::normalize_value(spec, name, raw)?;
            out.insert(name.clone(), value);
        }
        Ok(out)
    }

    pub fn load_from_setting(raw: &str) -> Result<BTreeMap<String, String>, String> {
        let stored: BTreeMap<String, String> = serde_json::from_str(raw)
            .map_err(|e| format!("setting {SETTING_TRAIN_CONFIG}={raw:?} 不是 JSON 对象: {e}"))?;
        Self::normalize(&stored).map_err(|e| format!("setting {SETTING_TRAIN_CONFIG}: {e}"))
    }

    fn spec_for(name: &str) -> Option<&'static TrainConfigSpec> {
        SPECS.iter().find(|(n, _)| *n == name).map(|(_, s)| s)
    }

    fn normalize_value(spec: &TrainConfigSpec, name: &str, raw: &str) -> Result<String, String> {
        let value = raw.trim();
        match spec.kind {
            TrainConfigKind::Bool => {
                let parsed = Self::parse_bool(value).map_err(|e| format!("{name}={raw:?}: {e}"))?;
                Ok(if parsed { "true" } else { "false" }.to_string())
            }
            TrainConfigKind::Int => {
                let parsed: i64 = value
                    .parse()
                    .map_err(|_| format!("{name}={raw:?} 不是整数"))?;
                Self::check_range(spec, name, parsed as f64)?;
                Ok(parsed.to_string())
            }
            TrainConfigKind::Float => {
                let parsed: f64 = value
                    .parse()
                    .map_err(|_| format!("{name}={raw:?} 不是数值"))?;

                if !parsed.is_finite() {
                    return Err(format!("{name}={raw:?} 不是有限数值"));
                }
                Self::check_range(spec, name, parsed)?;
                Ok(format!("{parsed}"))
            }
            TrainConfigKind::Enum => {
                let lowered = value.to_lowercase();
                for candidate in spec.enum_values {
                    if lowered == *candidate {
                        return Ok(candidate.to_string());
                    }
                }
                Err(format!(
                    "{name}={raw:?} 非法取值，可选: {}",
                    spec.enum_values.join(" / ")
                ))
            }
        }
    }

    fn check_range(spec: &TrainConfigSpec, name: &str, value: f64) -> Result<(), String> {
        if spec.gt_zero && value <= 0.0 {
            return Err(format!("{name} 必须 > 0，得到 {value}"));
        }
        if value < spec.min {
            return Err(format!("{name} 必须 >= {}, 得到 {value}", spec.min));
        }
        if spec.has_max && value > spec.max {
            return Err(format!("{name} 必须 <= {}, 得到 {value}", spec.max));
        }
        Ok(())
    }

    fn parse_bool(value: &str) -> Result<bool, String> {
        match value.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err("需为布尔值（true/false、1/0、yes/no、on/off）".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn whitelist_complete() {
        let names = TrainConfig::field_names();
        assert_eq!(names.len(), 28);

        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);

        let fields = TrainConfig::fields();
        let lr = fields.iter().find(|f| f.name == "LEARNING_RATE").unwrap();
        assert_eq!(lr.kind, "float");
        assert!(lr.has_max);
        assert_eq!(lr.min, 0.0);
        assert_eq!(lr.max, 1.0);
        let vtm = fields
            .iter()
            .find(|f| f.name == "VALUE_TARGET_MODE")
            .unwrap();
        assert_eq!(vtm.kind, "enum");
        assert_eq!(vtm.enum_values.len(), 6);
        let ptt = fields
            .iter()
            .find(|f| f.name == "POLICY_TARGET_TEMPERATURE")
            .unwrap();
        assert!(ptt.gt_zero);
        assert!(!ptt.has_max);
    }

    #[test]
    fn normalize_ok_and_canonical() {
        let out = TrainConfig::normalize(&map(&[
            ("LEARNING_RATE", " 0.10 "),
            ("TRAIN_BATCH", "256"),
            ("RECENT_SAMPLE_ENABLED", "ON"),
            ("DATA_AUGMENT_ENABLED", "no"),
            ("VALUE_TARGET_MODE", "MCTS"),
        ]))
        .unwrap();
        assert_eq!(out.get("LEARNING_RATE").unwrap(), "0.1");
        assert_eq!(out.get("TRAIN_BATCH").unwrap(), "256");
        assert_eq!(out.get("RECENT_SAMPLE_ENABLED").unwrap(), "true");
        assert_eq!(out.get("DATA_AUGMENT_ENABLED").unwrap(), "false");
        assert_eq!(out.get("VALUE_TARGET_MODE").unwrap(), "mcts");
    }

    #[test]
    fn bool_strict_forms_only() {
        for ok in [
            "1", "true", "yes", "on", "0", "false", "no", "off", "TRUE", "Off",
        ] {
            TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", ok)])).unwrap();
        }

        assert!(TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", "enabled")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", "2")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", "")])).is_err());
    }

    #[test]
    fn unknown_field_rejects_whole_batch() {
        let input = map(&[("LEARNING_RATE", "0.1"), ("BOGUS_FIELD", "true")]);
        let err = TrainConfig::normalize(&input).unwrap_err();
        assert!(err.contains("BOGUS_FIELD"), "{err}");
        assert!(err.contains("不可远程调节"), "{err}");
    }

    #[test]
    fn range_and_enum_rejections() {
        assert!(TrainConfig::normalize(&map(&[("LEARNING_RATE", "1.5")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("TRAIN_BATCH", "0")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("LR_DECAY_STEPS", "-3")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("POLICY_TARGET_TEMPERATURE", "0")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("TRAIN_BATCH", "abc")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("VALUE_TARGET_MODE", "bogus")])).is_err());

        TrainConfig::normalize(&map(&[("VALUE_GAUSS_SIGMA", "1.0")])).unwrap();
    }

    #[test]
    fn setting_roundtrip() {
        let normalized = TrainConfig::normalize(&map(&[("EMA_DECAY", "0.999")])).unwrap();
        let raw = serde_json::to_string(&normalized).unwrap();
        let back = TrainConfig::load_from_setting(&raw).unwrap();
        assert_eq!(back.get("EMA_DECAY").unwrap(), "0.999");

        assert!(TrainConfig::load_from_setting("not json").is_err());
        assert!(TrainConfig::load_from_setting(r#"{"NOPE":"1"}"#).is_err());
    }
}
