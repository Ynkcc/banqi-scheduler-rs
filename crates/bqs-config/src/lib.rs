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
    pub mcts_sims: u32,
    pub mcts_max_considered_actions: u32,
    pub mcts_c_scale: f64,
    pub mcts_gumbel_scale: f64,
    pub playout_cap_random: bool,
    pub fast_mcts_sims: u32,
    pub full_search_prob: f64,
    pub mcts_eta: u32,
    pub mcts_ucb_c: f64,
    pub mcts_crn_enabled: bool,
    pub health_enabled: bool,
    pub health_weight: f64,
    pub health_confidence_exp: f64,
    pub expectimax_node_budget: u64,
    pub expectimax_max_depth: u32,
    pub expl_enabled: bool,
    pub expl_topk: u32,
    pub expl_mode: String,
    pub expl_temp_start: f64,
    pub expl_temp_end: f64,
    pub expl_temp_decay_steps: u32,
    pub expectimax_root_fullwidth: bool,

    pub expectimax_chance_outcome_depth: i32,

    pub expectimax_chance_budget_ratio: f64,
}

impl Default for SearchDefaults {
    fn default() -> Self {
        Self {
            mcts_sims: 64,
            mcts_max_considered_actions: 16,
            mcts_c_scale: 1.0,
            mcts_gumbel_scale: 1.0,
            playout_cap_random: false,
            fast_mcts_sims: 0,
            full_search_prob: 0.25,
            mcts_eta: 2,
            mcts_ucb_c: 0.0,
            mcts_crn_enabled: false,
            health_enabled: false,
            health_weight: 0.0,
            health_confidence_exp: 0.0,
            expectimax_node_budget: 500_000,
            expectimax_max_depth: 8,
            expl_enabled: false,
            expl_topk: 8,
            expl_mode: "gumbel".into(),
            expl_temp_start: 1.0,
            expl_temp_end: 0.15,
            expl_temp_decay_steps: 48,
            expectimax_root_fullwidth: true,

            expectimax_chance_outcome_depth: 0,
            expectimax_chance_budget_ratio: 0.25,
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
        if d.mcts_sims == 0 {
            return Err(ConfigError::Invalid(
                "selfplay.search.mcts_sims 必须 >= 1（任务下发的探索参数必填校验）".into(),
            ));
        }
        if d.mcts_max_considered_actions == 0 {
            return Err(ConfigError::Invalid(
                "selfplay.search.mcts_max_considered_actions 必须 >= 1".into(),
            ));
        }
        if d.mcts_eta < 2 {
            return Err(ConfigError::Invalid(
                "selfplay.search.mcts_eta 必须 >= 2".into(),
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
