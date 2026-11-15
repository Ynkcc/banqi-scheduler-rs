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

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerSection,
    pub db: DbSection,

    pub storage: Option<StorageSection>,
    pub selfplay: SelfplaySection,

    pub jobs: JobsSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerSection {
    pub listen: String,

    pub http_addr: String,

    pub worker_online_seconds: i64,

    pub webui_dist: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen: ":50051".into(),
            http_addr: "127.0.0.1:9536".into(),
            worker_online_seconds: 60,
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

    pub threads_baseline: i32,

    pub initial_revealed: i32,

    pub data_kind: DataKind,

    pub search: SearchDefaults,
}

impl Default for SelfplaySection {
    fn default() -> Self {
        Self {
            variant: "4x8".into(),
            games_per_task: 16,
            threads_baseline: 4,
            initial_revealed: 0,
            data_kind: DataKind::Resnet,
            search: SearchDefaults::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SearchDefaults {
    pub mcts: MctsSearchDefaults,
    pub expectimax: ExpectimaxSearchDefaults,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MctsSearchDefaults {
    pub sims: u32,

    pub max_considered_actions: u32,

    pub c_scale: f64,

    pub gumbel_scale: f64,

    pub playout_cap_random: bool,

    pub fast_sims: u32,

    pub full_search_prob: f64,

    pub eta: u32,

    pub ucb_c: f64,

    pub crn_enabled: bool,

    pub health: HealthSearchDefaults,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HealthSearchDefaults {
    pub enabled: bool,
    pub weight: f64,
    pub confidence_exp: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ExpectimaxSearchDefaults {
    pub deterministic_depth: u32,

    pub chance_outcome_depth: i32,

    pub root_fullwidth: bool,

    pub expl: ExplSearchDefaults,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ExplSearchDefaults {
    pub enabled: bool,

    pub mode: String,
    pub topk: u32,
    pub temp_start: f64,
    pub temp_end: f64,
    pub temp_decay_steps: u32,
}

impl Default for SearchDefaults {
    fn default() -> Self {
        Self {
            mcts: MctsSearchDefaults::default(),
            expectimax: ExpectimaxSearchDefaults::default(),
        }
    }
}

impl Default for MctsSearchDefaults {
    fn default() -> Self {
        Self {
            sims: 64,
            max_considered_actions: 16,
            c_scale: 1.0,
            gumbel_scale: 1.0,
            playout_cap_random: false,
            fast_sims: 0,
            full_search_prob: 0.25,
            eta: 2,
            ucb_c: 0.0,
            crn_enabled: false,
            health: HealthSearchDefaults::default(),
        }
    }
}

impl Default for HealthSearchDefaults {
    fn default() -> Self {
        Self {
            enabled: false,
            weight: 0.0,
            confidence_exp: 0.0,
        }
    }
}

impl Default for ExpectimaxSearchDefaults {
    fn default() -> Self {
        Self {
            deterministic_depth: 8,
            chance_outcome_depth: 0,
            root_fullwidth: true,
            expl: ExplSearchDefaults::default(),
        }
    }
}

impl Default for ExplSearchDefaults {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "gumbel".into(),
            topk: 8,
            temp_start: 1.0,
            temp_end: 0.15,
            temp_decay_steps: 48,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JobsSection {
    pub collect_games_per_task: i64,

    pub collect_deadline_seconds: i64,

    pub collect_max_inflight_per_worker: i64,

    pub collect_overdispatch_games: i64,

    pub priority_collect: i64,

    pub priority_rating: i64,
}

impl Default for JobsSection {
    fn default() -> Self {
        Self {
            collect_games_per_task: 0,
            collect_deadline_seconds: 1800,
            collect_max_inflight_per_worker: 1,
            collect_overdispatch_games: 0,
            priority_collect: 50,
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
        if s.games_per_task <= 0 {
            return Err(ConfigError::Invalid(
                "selfplay.games_per_task 必须 > 0".into(),
            ));
        }
        let d = &s.search;
        if d.mcts.sims == 0 {
            return Err(ConfigError::Invalid(
                "selfplay.search.mcts.sims 必须 >= 1（任务下发的探索参数必填校验）".into(),
            ));
        }
        if d.mcts.max_considered_actions == 0 {
            return Err(ConfigError::Invalid(
                "selfplay.search.mcts.max_considered_actions 必须 >= 1".into(),
            ));
        }
        if d.mcts.eta < 2 {
            return Err(ConfigError::Invalid(
                "selfplay.search.mcts.eta 必须 >= 2".into(),
            ));
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
    }

    #[test]
    fn data_kind_parses() {
        let cfg = Config::parse("[selfplay]\ndata_kind = \"nnue\"\n", "test").unwrap();
        assert_eq!(cfg.selfplay.data_kind, DataKind::Nnue);
    }
}
