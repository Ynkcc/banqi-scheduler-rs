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

    pub search: SearchDefaults,
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
    pub labeler_mode: String,
    pub labeler_budget_mult: u32,
    pub expl_enabled: bool,
    pub expl_topk: u32,
    pub expl_temp_start: f64,
    pub expl_temp_end: f64,
    pub expl_temp_decay_steps: u32,
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
            labeler_mode: String::new(),
            labeler_budget_mult: 2,
            expl_enabled: false,
            expl_topk: 8,
            expl_temp_start: 1.0,
            expl_temp_end: 0.15,
            expl_temp_decay_steps: 48,
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
pub struct JobsSection {
    pub collect_games_per_task: i64,

    pub collect_deadline_seconds: i64,

    pub collect_max_inflight_per_worker: i64,

    pub collect_overdispatch_games: i64,

    pub priority_selfplay: i64,
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
            priority_selfplay: 0,
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
