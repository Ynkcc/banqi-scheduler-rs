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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DataKind {
    #[default]
    Resnet,

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BalanceMode {
    #[default]
    Warn,

    Pause,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerSection,
    pub db: DbSection,

    pub storage: Option<StorageSection>,
    pub selfplay: SelfplaySection,
    pub gatekeeper: GatekeeperSection,
    pub sprt: SprtSection,
    pub reanalysis: ReanalysisSection,
    pub balance: BalanceSection,
    pub eval: EvalSection,

    pub jobs: JobsSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerSection {
    pub listen: String,

    pub http_addr: String,

    pub worker_online_seconds: i64,

    pub min_client_version: String,

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
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct StorageSection {
    pub bucket: String,

    pub region: String,

    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,

    pub path_style: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SelfplaySection {
    pub variant: String,

    pub games_per_task: i32,

    pub gatekeeper_games: i32,

    pub mix_ratio: f64,

    pub best: String,

    pub train_match_opponent: String,

    pub threads_baseline: i32,

    pub initial_revealed: i32,

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
    pub pairs: i32,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct SprtSection {
    pub elo0: f64,

    pub elo1: f64,
    pub alpha: f64,
    pub beta: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ReanalysisSection {
    pub interval_tasks: i64,

    pub max_queue: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BalanceSection {
    pub enabled: bool,
    pub mode: BalanceMode,

    pub trainer_online_seconds: i64,

    pub stalled_seconds: i64,

    pub backlog_pause_batches: i64,

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

    pub opponents: Vec<String>,

    pub games: i64,

    pub games_per_task: i64,

    pub mcts_sims: i32,

    pub every_n_promotions: i64,

    pub no_progress_n: i64,
    pub no_progress_eps: f64,

    pub rollback_eps: f64,

    pub floor_win_rate: f64,

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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JobsSection {
    pub collect_games_per_task: i64,

    pub collect_deadline_seconds: i64,

    pub priority_selfplay: i64,
    pub priority_collect: i64,
    pub priority_eval: i64,

    pub priority_rating: i64,
}

impl Default for JobsSection {
    fn default() -> Self {
        Self {
            collect_games_per_task: 0,
            collect_deadline_seconds: 1800,
            priority_selfplay: 0,
            priority_collect: 50,
            priority_eval: 100,
            priority_rating: 80,
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            file: path.display().to_string(),
            source,
        })?;
        Self::parse(&raw, &path.display().to_string())
    }

    pub fn parse(raw: &str, path: &str) -> Result<Self> {
        toml::from_str(raw).map_err(|e| ConfigError::Parse(format!("{path}: {e}")))
    }

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
            return Err(ConfigError::Invalid(
                "selfplay.games_per_task 必须 > 0".into(),
            ));
        }
        let sp = &self.sprt;
        for (name, v) in [("alpha", sp.alpha), ("beta", sp.beta)] {
            if !(0.0 < v && v < 1.0) {
                return Err(ConfigError::Invalid(format!(
                    "sprt.{name} 必须在 (0,1)，当前 {v}"
                )));
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
