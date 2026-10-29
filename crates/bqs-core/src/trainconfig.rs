//! 【所有权：core-judgment 成员】
//!
//! 可远程调整的训练超参：白名单 + 归一化/范围校验（调度器是权威裁定方）。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/trainconfig.go`（逐字段移植
//! `trainConfigSpecs` 白名单）。要点：
//! - 白名单约 30 字段（float/int/bool/enum + 范围），字段名与
//!   `banqi_training.config.Config` 一一对应，**必须与
//!   `banqi_training/config.py` 的 `TRAIN_CONFIG_OVERRIDABLE` 同步增删**；
//! - 非白名单字段 / 越界 / 非法枚举 → 整批拒绝（不静默丢弃；bool 只接受
//!   true/false、1/0、yes/no、on/off，**不复用 config.py 的「未知即真」容错**）；
//! - **全量替换语义**：PUT 的 overrides 即当前全部覆盖，未出现 = 删除该覆盖；
//! - 落库 [`SETTING_TRAIN_CONFIG`]（JSON 对象，字段名 → 值字符串），跨重启保留，
//!   加载时重新校验（坏数据拒绝启动）；
//! - [`TrainConfig::fields`] 即白名单投影（GET /api/train-config 的字段清单，
//!   前端动态渲染表单，JSON 字段名与 Go 版一致）。

use std::collections::BTreeMap;

use serde::Serialize;

/// 训练配置覆盖项的落库键：JSON 对象（字段名 → 值字符串）。
pub const SETTING_TRAIN_CONFIG: &str = "train_config";

/// 覆盖项取值类型：决定规范化方式与 WebUI 表单控件。
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

/// 单个可远程调节字段的规格（取值合法性的唯一权威）。
#[derive(Debug, Clone, Copy)]
pub struct TrainConfigSpec {
    pub kind: TrainConfigKind,
    /// 闭下界（float / int）
    pub min: f64,
    /// 闭上界，仅 `has_max` 时校验
    pub max: f64,
    pub has_max: bool,
    /// 必须严格 > 0
    pub gt_zero: bool,
    /// kind = Enum 时的合法取值（小写）
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

/// 白名单：`trainConfigSpecs` 的逐字段移植（对照 Go 版 trainconfig.go）。
///
/// 判据：不改模型结构、不依赖本地路径/设备、且能在训练循环中热更。结构开关
/// （HEALTH_VALUE_HEAD_ENABLED / VALUE_DIST_* / POLICY_TRUNK_INDEPENDENT）、
/// 路径与设备、一次性资源容量（MAX_SAMPLE_BUFFER_SIZE / REANALYSIS_POOL_SIZE）
/// 一律不在表中。
static SPECS: &[(&str, TrainConfigSpec)] = &[
    // ---- LR 计划（改动后 trainer 重建余弦调度器并保留进度）----
    ("LEARNING_RATE", f(0.0, 1.0)),
    ("MIN_LR", f(0.0, 1.0)),
    ("LR_DECAY_STEPS", i(1.0)),
    ("LR_DECAY_ROUNDS", i(0.0)),
    // ---- 训练量 ----
    ("TRAIN_BATCH", i(1.0)),
    ("TRAIN_EPOCHS_PER_ROUND", i(1.0)),
    ("MIN_NEW_SAMPLES_TO_TRAIN", i(0.0)),
    ("WEIGHT_DECAY", f(0.0, 1.0)),
    // ---- EMA 与采样 ----
    ("EMA_DECAY", f(0.0, 1.0)),
    ("RECENT_SAMPLE_ENABLED", b()),
    ("FAST_SAMPLE_LOSS_WEIGHT", f(0.0, 1.0)),
    // ---- 目标函数（取值域对齐 config.py 与 buffer.py 的构造期校验）----
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
    ("HEALTH_LOSS_WEIGHT", f(0.0, 10.0)),
    (
        "HEALTH_GAUSS_SIGMA",
        TrainConfigSpec {
            kind: TrainConfigKind::Float,
            min: 0.0,
            max: 1.0,
            has_max: true,
            gt_zero: true,
            enum_values: &[],
        },
    ),
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
    // ---- 数据增强 ----
    ("DATA_AUGMENT_ENABLED", b()),
    ("DATA_AUGMENT_K", i(1.0)),
    ("DATA_AUGMENT_KEEP_ORIGINAL", b()),
    // ---- 局面重搜节流 ----
    ("REANALYSIS_BATCH_POSITIONS", i(1.0)),
    ("REANALYSIS_SUBMIT_EVERY_N_ROUNDS", i(1.0)),
    ("REANALYSIS_MCTS_SIMS", i(0.0)),
    // ---- 运行控制 ----
    ("MAX_RUNTIME_SECONDS", i(0.0)),
    ("SHOULD_STOP_POLL_SECONDS", i(0.0)),
    // ---- 模型导出 ----
    // TorchScript/ONNX 导出间隔轮次（save_checkpoint 每轮动态读取，热更即生效）
    ("CKPT_EXPORT_EVERY", i(1.0)),
    // NNUE 导出节奏与门槛（nnue trainer 每次导出时动态读取，热更即生效）
    ("NNUE_EXPORT_EVERY_ROUNDS", i(1.0)),
    ("NNUE_EXPORT_GATE", b()),
];

/// 白名单字段的只读描述：WebUI 据此渲染表单与校验提示（JSON 字段名与 Go 版
/// `TrainConfigField` 完全一致：name/kind/min/max/hasMax/gtZero/enum）。
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

/// 训练超参白名单与校验的公共入口。
///
/// 全部为关联函数/静态数据（无状态、无 I/O）；`Control` 的
/// `train_config_overrides` / `set_train_config_overrides` 与
/// `GET /api/train-config` 均消费本类型。
pub struct TrainConfig;

impl TrainConfig {
    /// 白名单字段清单（按字段名升序；错误提示与表单排序都依赖稳定顺序）。
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

    /// 升序字段名清单。
    pub fn field_names() -> Vec<String> {
        let mut names: Vec<String> = SPECS.iter().map(|(n, _)| n.to_string()).collect();
        names.sort();
        names
    }

    /// 归一化即校验：非白名单字段直接报错（不静默丢弃——远程配置写错字段名必须
    /// 立刻可见），取值按 spec 归一后返回。整批拒绝：任一字段非法即整体失败。
    pub fn normalize(
        input: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, String> {
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

    /// 从 settings JSON 读回并重新校验（启动路径；坏数据由调用方拒绝启动）。
    pub fn load_from_setting(raw: &str) -> Result<BTreeMap<String, String>, String> {
        let stored: BTreeMap<String, String> = serde_json::from_str(raw)
            .map_err(|e| format!("setting {SETTING_TRAIN_CONFIG}={raw:?} 不是 JSON 对象: {e}"))?;
        Self::normalize(&stored)
            .map_err(|e| format!("setting {SETTING_TRAIN_CONFIG}: {e}"))
    }

    fn spec_for(name: &str) -> Option<&'static TrainConfigSpec> {
        SPECS.iter().find(|(n, _)| *n == name).map(|(_, s)| s)
    }

    /// 把单个字段的原始字符串解析为规范写法，并校验取值域。
    fn normalize_value(spec: &TrainConfigSpec, name: &str, raw: &str) -> Result<String, String> {
        let value = raw.trim();
        match spec.kind {
            TrainConfigKind::Bool => {
                let parsed = Self::parse_bool(value)
                    .map_err(|e| format!("{name}={raw:?}: {e}"))?;
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
                // 与 Go 版刻意差异：拒绝 inf/NaN（Go 的 ParseFloat 放行，但训练侧
                // 无法消费，这里直接报错更安全）。
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

    /// 数值域校验；错误信息带具体字段与边界，便于 WebUI 直接回显。
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

    /// 只接受明确的布尔写法。刻意不用「未知值即真」容错：远程下发写错必须报错。
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
        assert_eq!(names.len(), 30);
        // 升序稳定
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        // 抽查关键字段与边界
        let fields = TrainConfig::fields();
        let lr = fields.iter().find(|f| f.name == "LEARNING_RATE").unwrap();
        assert_eq!(lr.kind, "float");
        assert!(lr.has_max);
        assert_eq!(lr.min, 0.0);
        assert_eq!(lr.max, 1.0);
        let vtm = fields.iter().find(|f| f.name == "VALUE_TARGET_MODE").unwrap();
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
        for ok in ["1", "true", "yes", "on", "0", "false", "no", "off", "TRUE", "Off"] {
            TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", ok)])).unwrap();
        }
        // 「未知即真」容错被刻意禁用
        assert!(TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", "enabled")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", "2")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("RECENT_SAMPLE_ENABLED", "")])).is_err());
    }

    #[test]
    fn unknown_field_rejects_whole_batch() {
        let input = map(&[
            ("LEARNING_RATE", "0.1"),
            ("HEALTH_VALUE_HEAD_ENABLED", "true"), // 结构开关不在白名单
        ]);
        let err = TrainConfig::normalize(&input).unwrap_err();
        assert!(err.contains("HEALTH_VALUE_HEAD_ENABLED"), "{err}");
        assert!(err.contains("不可远程调节"), "{err}");
    }

    #[test]
    fn range_and_enum_rejections() {
        assert!(TrainConfig::normalize(&map(&[("LEARNING_RATE", "1.5")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("TRAIN_BATCH", "0")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("LR_DECAY_STEPS", "-3")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("POLICY_TARGET_TEMPERATURE", "0")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("HEALTH_GAUSS_SIGMA", "0")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("TRAIN_BATCH", "abc")])).is_err());
        assert!(TrainConfig::normalize(&map(&[("VALUE_TARGET_MODE", "bogus")])).is_err());
        // gt_zero + has_max 组合：上限合法
        TrainConfig::normalize(&map(&[("HEALTH_GAUSS_SIGMA", "1.0")])).unwrap();
    }

    #[test]
    fn setting_roundtrip() {
        let normalized = TrainConfig::normalize(&map(&[("EMA_DECAY", "0.999")])).unwrap();
        let raw = serde_json::to_string(&normalized).unwrap();
        let back = TrainConfig::load_from_setting(&raw).unwrap();
        assert_eq!(back.get("EMA_DECAY").unwrap(), "0.999");
        // 坏数据：非 JSON / 非白名单字段，读回即报错
        assert!(TrainConfig::load_from_setting("not json").is_err());
        assert!(TrainConfig::load_from_setting(r#"{"NOPE":"1"}"#).is_err());
    }
}
