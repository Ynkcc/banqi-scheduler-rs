//! 配置层（单一真源）。
//!
//! 语义约定（与 Go 版 banqi-scheduler 对齐，section/键名保持兼容）：
//! - 配置仅来源于配置文件（TOML 风格）与命令行覆盖项；不读环境变量；
//! - **未知键 / 未知 section 直接拒绝启动**（`deny_unknown_fields`，防拼写错误静默走默认值）；
//! - 缺失的键回落到 `Default`；
//! - `SelfplaySection::initial_revealed` / `data_kind` / `eval` 覆盖等是「库中无记录时的
//!   初始值」，运行时以 Control（settings 表落库值）为准；
//! - `[storage]` 段仅在首次启动（库中无存储 profile）时引导 default profile，
//!   之后以 settings 表为准。
//!
//! 与 Go 版的刻意差异：TOML 语法由 `toml` crate 权威解析（不支持 `;` 行注释）；
//! 类型解析错误即时报告（首个错误即失败），不再聚合全部错误。

use serde::Deserialize;

pub type Result<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("读取配置文件 {file}: {source}")]
    Io {
        file: String,
        #[source]
        source: std::io::Error,
    },
    #[error("解析配置文件 {0}")]
    Parse(String),
    #[error("配置校验失败: {0}")]
    Invalid(String),
}

/// 数据类别（训练侧在训架构）：映射到 proto `DataKind`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DataKind {
    /// Gumbel MCTS 栈（ResNet 稠密特征，配 .onnx 权重）
    #[default]
    Resnet,
    /// Expectimax 栈（NNUE 稀疏特征，配 .nnue 权重）
    Nnue,
}

impl DataKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DataKind::Resnet => "resnet",
            DataKind::Nnue => "nnue",
        }
    }
}

/// 停滞动作模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BalanceMode {
    /// 只记日志与 WebUI 展示
    #[default]
    Warn,
    /// 自动暂停 selfplay 派发（含混入对打）
    Pause,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerSection,
    pub db: DbSection,
    /// 首次启动引导 default 存储 profile；库中已有 profile 时忽略
    pub storage: Option<StorageSection>,
    pub selfplay: SelfplaySection,
    pub gatekeeper: GatekeeperSection,
    pub sprt: SprtSection,
    pub reanalysis: ReanalysisSection,
    pub balance: BalanceSection,
    pub eval: EvalSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerSection {
    /// gRPC 监听地址（与采集端/训练端一致）
    pub listen: String,
    /// WebUI 监听地址（无鉴权，勿暴露公网）；启动失败只记日志
    pub http_addr: String,
    /// worker 在线判定窗口（秒）
    pub worker_online_seconds: i64,
    /// client_version 低于该值的 worker 不派单（空 = 不限制）
    pub min_client_version: String,
    /// WebUI 静态资源目录（SPA 构建产物；空 = 不托管前端）
    pub webui_dist: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen: ":50051".into(),
            http_addr: "127.0.0.1:9536".into(),
            worker_online_seconds: 60,
            min_client_version: String::new(),
            webui_dist: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct DbSection {
    /// SQLite 数据库路径（WAL 模式）
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct StorageSection {
    pub bucket: String,
    /// R2 用 "auto"
    pub region: String,
    /// S3 API 端点（不带桶名路径；本地 MinIO/RustFS 填 http://host:port）
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    /// MinIO/RustFS 等无法 virtual-host 寻址的实现必须 true；R2 保持 false
    pub path_style: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SelfplaySection {
    /// 下发给 worker/trainer 的变体 id（须在 variants.json 中登记）
    pub variant: String,
    /// selfplay 每次下发的局数
    pub games_per_task: i32,
    /// gatekeeper 对打目标局数（换色成对数）
    pub gatekeeper_games: i32,
    /// 混入比例（0~1）：存在待判定候选时与上代 champion 对打的局数占比
    pub mix_ratio: f64,
    /// best 来源："rule" = 规则评估哨兵（冷启动蒸馏教师）；空 = 按登记 best
    pub best: String,
    /// 对打训练模式（实验）：非空时纯自对弈改为 vs 该规则对手的录制对打
    pub train_match_opponent: String,
    /// 资源分配基准线程数（0 = 不按资源缩放）
    pub threads_baseline: i32,
    /// 课程学习初始翻子数（仅库中无记录时生效；运行时以 Control 为准）
    pub initial_revealed: i32,
    /// 训练侧在训架构产哪类数据（仅库中无记录时生效）
    pub data_kind: DataKind,
}

impl Default for SelfplaySection {
    fn default() -> Self {
        Self {
            variant: "4x8".into(),
            games_per_task: 16,
            gatekeeper_games: 400,
            mix_ratio: 0.25,
            best: String::new(),
            train_match_opponent: String::new(),
            threads_baseline: 4,
            initial_revealed: 0,
            data_kind: DataKind::Resnet,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct GatekeeperSection {
    /// 对打目标局数（换色成对数；实际局数 = 值 × 2）——仅库中无 match 时作为初值口径
    pub pairs: i32,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct SprtSection {
    /// H0：新网络 <= elo0（拒绝晋级）
    pub elo0: f64,
    /// H1：新网络 >= elo1（晋级 best）
    pub elo1: f64,
    pub alpha: f64,
    pub beta: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ReanalysisSection {
    /// 每 N 个 selfplay 任务最多下发 1 个重搜任务（0 = 关闭）
    pub interval_tasks: i64,
    /// 待下发重搜队列上限（按载荷条数，<=0 不限）
    pub max_queue: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BalanceSection {
    pub enabled: bool,
    pub mode: BalanceMode,
    /// 心跳离线判定窗口（秒）
    pub trainer_online_seconds: i64,
    /// 在线停摆判定窗口（秒）
    pub stalled_seconds: i64,
    /// 积压触发阈值（0 = 关闭）
    pub backlog_pause_batches: i64,
    /// 自动暂停时是否一并暂停重搜派发
    pub pause_reanalysis: bool,
}

impl Default for BalanceSection {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: BalanceMode::Warn,
            trainer_online_seconds: 120,
            stalled_seconds: 600,
            backlog_pause_batches: 0,
            pause_reanalysis: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EvalSection {
    pub enabled: bool,
    /// 对手阶梯（random / rule:reveal_first / rule:capture_first / rule:material_expectimax）
    pub opponents: Vec<String>,
    /// 每档评估的目标局数
    pub games: i64,
    /// 单任务局数额度（>0 流式并行；<=0 单任务整包）
    pub games_per_task: i64,
    /// 评估搜索深度：0 = 纯策略 argmax；>0 = MCTS 模拟数
    pub mcts_sims: i32,
    /// 每 N 次 best 晋级评测一次
    pub every_n_promotions: i64,
    /// 连续 N 次提升 < no_progress_eps 即停机（0 = 只观测不判停）
    pub no_progress_n: i64,
    pub no_progress_eps: f64,
    /// 回滚判据：新 best 胜率比前任低超过该值 → 回滚 + 标记 rejected
    pub rollback_eps: f64,
    /// 绝对地板胜率（0 = 关闭）
    pub floor_win_rate: f64,
    /// 达标停机：material_expectimax 胜率 >= 该值即停机（0 = 关闭）
    pub stop_win_rate: f64,
}

impl Default for EvalSection {
    fn default() -> Self {
        Self {
            enabled: false,
            opponents: vec![
                "random".into(),
                "rule:reveal_first".into(),
                "rule:capture_first".into(),
                "rule:material_expectimax".into(),
            ],
            games: 1000,
            games_per_task: 0,
            mcts_sims: 0,
            every_n_promotions: 1,
            no_progress_n: 3,
            no_progress_eps: 0.035,
            rollback_eps: 0.04,
            floor_win_rate: 0.0,
            stop_win_rate: 0.0,
        }
    }
}

impl Config {
    /// 解析配置文件。
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Io { file: path.display().to_string(), source })?;
        Self::parse(&raw, &path.display().to_string())
    }

    /// 从字符串解析（测试友好）。
    pub fn parse(raw: &str, path: &str) -> Result<Self> {
        toml::from_str(raw).map_err(|e| ConfigError::Parse(format!("{path}: {e}")))
    }

    /// 启动校验（超出范围的值拒绝启动，防静默走默认）。
    pub fn validate(&self) -> Result<()> {
        let s = &self.selfplay;
        if s.variant.is_empty() {
            return Err(ConfigError::Invalid("selfplay.variant 不能为空".into()));
        }
        if !(0.0..=1.0).contains(&s.mix_ratio) {
            return Err(ConfigError::Invalid(format!(
                "selfplay.mix_ratio 必须在 [0,1]，当前 {}",
                s.mix_ratio
            )));
        }
        if s.games_per_task <= 0 {
            return Err(ConfigError::Invalid("selfplay.games_per_task 必须 > 0".into()));
        }
        let sp = &self.sprt;
        for (name, v) in [("alpha", sp.alpha), ("beta", sp.beta)] {
            if !(0.0 < v && v < 1.0) {
                return Err(ConfigError::Invalid(format!("sprt.{name} 必须在 (0,1)，当前 {v}")));
            }
        }
        if sp.elo1 <= sp.elo0 {
            return Err(ConfigError::Invalid(format!(
                "sprt.elo1 ({}) 必须大于 sprt.elo0 ({})",
                sp.elo1, sp.elo0
            )));
        }
        if self.eval.games <= 0 {
            return Err(ConfigError::Invalid("eval.games 必须 > 0".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse_empty() {
        let cfg = Config::parse("", "test").unwrap();
        assert_eq!(cfg.server.listen, ":50051");
        assert_eq!(cfg.selfplay.variant, "4x8");
        assert!(cfg.storage.is_none());
    }

    #[test]
    fn unknown_key_rejected() {
        let err = Config::parse("[server]\nlisten = \":1\"\ntypo_key = 1\n", "test").unwrap_err();
        assert!(err.to_string().contains("typo_key"), "{err}");
    }

    #[test]
    fn unknown_section_rejected() {
        let err = Config::parse("[nope]\nx = 1\n", "test").unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn full_example_parses() {
        let raw = include_str!("../../../configs/config.example.conf");
        let cfg = Config::parse(raw, "example").unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.selfplay.mix_ratio, 0.25);
        assert_eq!(cfg.eval.opponents.len(), 4);
        assert_eq!(cfg.balance.mode, BalanceMode::Warn);
    }

    #[test]
    fn mix_ratio_out_of_range_rejected() {
        let cfg = Config::parse("[selfplay]\nmix_ratio = 1.5\n", "test").unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn data_kind_parses() {
        let cfg = Config::parse("[selfplay]\ndata_kind = \"nnue\"\n", "test").unwrap();
        assert_eq!(cfg.selfplay.data_kind, DataKind::Nnue);
    }
}
