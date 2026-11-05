use bqs_proto::{
    CollectPlayerSpec, CollectSearchParams, CollectionJobSpec, OpeningScenario, RatingJobSpec,
    SeedStrategy, SeedStrategyKind,
};

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SearchSpecJson {
    #[serde(alias = "mcts_sims")]
    pub mcts_sims: u32,
    #[serde(alias = "mcts_max_considered_actions")]
    pub mcts_max_considered_actions: u32,
    #[serde(alias = "mcts_c_scale")]
    pub mcts_c_scale: f64,
    #[serde(alias = "mcts_gumbel_scale")]
    pub mcts_gumbel_scale: f64,
    pub temperature: f32,
    #[serde(alias = "playout_cap_random")]
    pub playout_cap_random: bool,
    #[serde(alias = "expectimax_node_budget")]
    pub expectimax_node_budget: u64,
    #[serde(alias = "expectimax_max_depth")]
    pub expectimax_max_depth: u32,
    #[serde(alias = "labeler_mode")]
    pub labeler_mode: String,
    #[serde(alias = "labeler_budget_mult")]
    pub labeler_budget_mult: u32,
    #[serde(alias = "expl_enabled")]
    pub expl_enabled: bool,
    #[serde(alias = "expl_topk")]
    pub expl_topk: u32,
    #[serde(alias = "expl_temp_start")]
    pub expl_temp_start: f64,
    #[serde(alias = "expl_temp_end")]
    pub expl_temp_end: f64,
    #[serde(alias = "expl_temp_decay_steps")]
    pub expl_temp_decay_steps: u32,
    #[serde(alias = "fast_mcts_sims")]
    pub fast_mcts_sims: u32,
    #[serde(alias = "full_search_prob")]
    pub full_search_prob: f64,
    #[serde(alias = "mcts_eta")]
    pub mcts_eta: u32,
    #[serde(alias = "mcts_ucb_c")]
    pub mcts_ucb_c: f64,
    #[serde(alias = "mcts_crn_enabled")]
    pub mcts_crn_enabled: bool,
    #[serde(alias = "health_enabled")]
    pub health_enabled: bool,
    #[serde(alias = "health_weight")]
    pub health_weight: f64,
    #[serde(alias = "health_confidence_exp")]
    pub health_confidence_exp: f64,
    #[serde(alias = "expl_mode")]
    pub expl_mode: String,
    #[serde(alias = "expectimax_root_fullwidth")]
    pub expectimax_root_fullwidth: bool,
}

impl SearchSpecJson {
    pub fn from_proto(s: Option<CollectSearchParams>) -> Self {
        match s {
            Some(s) => Self {
                mcts_sims: s.mcts_sims,
                mcts_max_considered_actions: s.mcts_max_considered_actions,
                mcts_c_scale: s.mcts_c_scale,
                mcts_gumbel_scale: s.mcts_gumbel_scale,
                temperature: s.temperature,
                playout_cap_random: s.playout_cap_random,
                expectimax_node_budget: s.expectimax_node_budget,
                expectimax_max_depth: s.expectimax_max_depth,
                labeler_mode: s.labeler_mode,
                labeler_budget_mult: s.labeler_budget_mult,
                expl_enabled: s.expl_enabled,
                expl_topk: s.expl_topk,
                expl_temp_start: s.expl_temp_start,
                expl_temp_end: s.expl_temp_end,
                expl_temp_decay_steps: s.expl_temp_decay_steps,
                fast_mcts_sims: s.fast_mcts_sims,
                full_search_prob: s.full_search_prob,
                mcts_eta: s.mcts_eta,
                mcts_ucb_c: s.mcts_ucb_c,
                mcts_crn_enabled: s.mcts_crn_enabled,
                health_enabled: s.health_enabled,
                health_weight: s.health_weight,
                health_confidence_exp: s.health_confidence_exp,
                expl_mode: s.expl_mode,
                expectimax_root_fullwidth: s.expectimax_root_fullwidth,
            },
            None => Self::default(),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.mcts_sims == 0
            && self.mcts_max_considered_actions == 0
            && self.mcts_c_scale == 0.0
            && self.mcts_gumbel_scale == 0.0
            && self.temperature == 0.0
            && !self.playout_cap_random
            && self.expectimax_node_budget == 0
            && self.expectimax_max_depth == 0
            && self.labeler_mode.is_empty()
            && self.labeler_budget_mult == 0
            && !self.expl_enabled
            && self.expl_topk == 0
            && self.expl_temp_start == 0.0
            && self.expl_temp_end == 0.0
            && self.expl_temp_decay_steps == 0
            && self.fast_mcts_sims == 0
            && self.full_search_prob == 0.0
            && self.mcts_eta == 0
            && self.mcts_ucb_c == 0.0
            && !self.mcts_crn_enabled
            && !self.health_enabled
            && self.health_weight == 0.0
            && self.health_confidence_exp == 0.0
            && self.expl_mode.is_empty()
            && !self.expectimax_root_fullwidth
    }

    pub fn to_proto(&self) -> Option<CollectSearchParams> {
        let s = &self;
        if s.is_zero() {
            return None;
        }
        Some(CollectSearchParams {
            mcts_sims: s.mcts_sims,
            mcts_max_considered_actions: s.mcts_max_considered_actions,
            mcts_c_scale: s.mcts_c_scale,
            mcts_gumbel_scale: s.mcts_gumbel_scale,
            temperature: s.temperature,
            playout_cap_random: s.playout_cap_random,
            expectimax_node_budget: s.expectimax_node_budget,
            expectimax_max_depth: s.expectimax_max_depth,
            labeler_mode: s.labeler_mode.clone(),
            labeler_budget_mult: s.labeler_budget_mult,
            expl_enabled: s.expl_enabled,
            expl_topk: s.expl_topk,
            expl_temp_start: s.expl_temp_start,
            expl_temp_end: s.expl_temp_end,
            expl_temp_decay_steps: s.expl_temp_decay_steps,
            fast_mcts_sims: s.fast_mcts_sims,
            full_search_prob: s.full_search_prob,
            mcts_eta: s.mcts_eta,
            mcts_ucb_c: s.mcts_ucb_c,
            mcts_crn_enabled: s.mcts_crn_enabled,
            health_enabled: s.health_enabled,
            health_weight: s.health_weight,
            health_confidence_exp: s.health_confidence_exp,
            expl_mode: s.expl_mode.clone(),
            expectimax_root_fullwidth: s.expectimax_root_fullwidth,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PlayerSpecJson {
    pub kind: String,
    #[serde(alias = "network_sha")]
    pub network_sha: String,
    pub search: Option<SearchSpecJson>,
}

impl PlayerSpecJson {
    pub fn from_proto(p: Option<CollectPlayerSpec>) -> Self {
        match p {
            Some(p) => Self {
                kind: p.kind,
                network_sha: p.network_sha,
                search: p.search.map(|s| SearchSpecJson::from_proto(Some(s))),
            },
            None => Self::default(),
        }
    }

    pub fn to_proto(&self) -> CollectPlayerSpec {
        CollectPlayerSpec {
            kind: self.kind.clone(),
            network_sha: self.network_sha.clone(),
            search: self.search.as_ref().and_then(|s| s.to_proto()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SeedStrategyJson {
    pub kind: i32,
    #[serde(alias = "fixed_seed")]
    pub fixed_seed: u64,
    #[serde(alias = "base_seed")]
    pub base_seed: u64,
    #[serde(alias = "min_seed")]
    pub min_seed: u64,
    #[serde(alias = "max_seed")]
    pub max_seed: u64,
}

impl SeedStrategyJson {
    fn from_proto(s: Option<SeedStrategy>) -> Self {
        match s {
            Some(s) => Self {
                kind: s.kind,
                fixed_seed: s.fixed_seed,
                base_seed: s.base_seed,
                min_seed: s.min_seed,
                max_seed: s.max_seed,
            },
            None => Self::default(),
        }
    }

    fn to_proto(&self) -> SeedStrategy {
        SeedStrategy {
            kind: self.kind,
            fixed_seed: self.fixed_seed,
            base_seed: self.base_seed,
            min_seed: self.min_seed,
            max_seed: self.max_seed,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CollectSpecJson {
    pub variant: String,
    pub scenario: i32,
    #[serde(alias = "seed_strategy")]
    pub seed_strategy: SeedStrategyJson,
    #[serde(alias = "player_a")]
    pub player_a: PlayerSpecJson,
    #[serde(alias = "player_b")]
    pub player_b: PlayerSpecJson,
    #[serde(alias = "data_kind")]
    pub data_kind: i32,
    #[serde(alias = "total_games")]
    pub total_games: u64,

    pub pairs_parallel: Option<bool>,

    pub pair_seed_refresh: Option<bool>,

    #[serde(alias = "start_fens")]
    pub start_fens: Vec<String>,
}

impl CollectSpecJson {
    pub(super) fn from_proto(s: CollectionJobSpec) -> Self {
        Self {
            variant: s.variant,
            scenario: s.scenario,
            seed_strategy: SeedStrategyJson::from_proto(s.seed_strategy),
            player_a: PlayerSpecJson::from_proto(s.player_a),
            player_b: PlayerSpecJson::from_proto(s.player_b),
            data_kind: s.data_kind,
            total_games: s.total_games,
            pairs_parallel: s.pairs_parallel,
            pair_seed_refresh: s.pair_seed_refresh,
            start_fens: s.start_fens,
        }
    }

    pub fn to_proto(&self) -> CollectionJobSpec {
        CollectionJobSpec {
            variant: self.variant.clone(),
            scenario: self.scenario,
            seed_strategy: Some(self.seed_strategy.to_proto()),
            player_a: Some(self.player_a.to_proto()),
            player_b: Some(self.player_b.to_proto()),
            data_kind: self.data_kind,
            total_games: self.total_games,
            pairs_parallel: self.pairs_parallel,
            pair_seed_refresh: self.pair_seed_refresh,
            start_fens: self.start_fens.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RatingSpecJson {
    pub variant: String,
    #[serde(alias = "candidate_sha")]
    pub candidate_sha: String,
    #[serde(alias = "opponent_sha")]
    pub opponent_sha: String,
    #[serde(alias = "total_games")]
    pub total_games: u64,
    #[serde(alias = "sprt_elo0")]
    pub sprt_elo0: f64,
    #[serde(alias = "sprt_elo1")]
    pub sprt_elo1: f64,
    #[serde(alias = "sprt_alpha")]
    pub sprt_alpha: f64,
    #[serde(alias = "sprt_beta")]
    pub sprt_beta: f64,
    #[serde(alias = "side_a")]
    pub side_a: PlayerSpecJson,
    #[serde(alias = "side_b")]
    pub side_b: PlayerSpecJson,
    #[serde(alias = "match_id")]
    pub match_id: i64,
    #[serde(alias = "start_fens")]
    pub start_fens: Vec<String>,
    pub scenario: i32,
}

impl RatingSpecJson {
    pub(super) fn from_proto(s: RatingJobSpec) -> Self {
        Self {
            variant: s.variant,
            candidate_sha: s.candidate_sha,
            opponent_sha: s.opponent_sha,
            total_games: s.total_games,
            sprt_elo0: s.sprt_elo0,
            sprt_elo1: s.sprt_elo1,
            sprt_alpha: s.sprt_alpha,
            sprt_beta: s.sprt_beta,
            side_a: PlayerSpecJson::default(),
            side_b: PlayerSpecJson::default(),
            match_id: 0,
            start_fens: s.start_fens,
            scenario: s.scenario,
        }
    }

    pub fn resolved_sides(&self) -> (PlayerSpecJson, PlayerSpecJson) {
        if self.side_a.kind.is_empty() && self.side_b.kind.is_empty() {
            self.legacy_fallback_sides()
        } else {
            (self.side_a.clone(), self.side_b.clone())
        }
    }

    pub fn legacy_fallback_sides(&self) -> (PlayerSpecJson, PlayerSpecJson) {
        (
            PlayerSpecJson {
                kind: "expectimax_nnue".into(),
                network_sha: self.candidate_sha.clone(),
                search: None,
            },
            PlayerSpecJson {
                kind: "expectimax_nnue".into(),
                network_sha: self.opponent_sha.clone(),
                search: None,
            },
        )
    }

    pub fn sides_are_model(&self) -> (bool, bool) {
        let (a, b) = self.resolved_sides();
        (
            bq_policy::player_kind::requires_model(&a.kind),
            bq_policy::player_kind::requires_model(&b.kind),
        )
    }

    pub fn to_proto(&self) -> RatingJobSpec {
        let (side_a, side_b) = self.resolved_sides();
        RatingJobSpec {
            variant: self.variant.clone(),
            candidate_sha: self.candidate_sha.clone(),
            opponent_sha: self.opponent_sha.clone(),
            total_games: self.total_games,
            sprt_elo0: self.sprt_elo0,
            sprt_elo1: self.sprt_elo1,
            sprt_alpha: self.sprt_alpha,
            sprt_beta: self.sprt_beta,
            side_a: Some(side_a.to_proto()),
            side_b: Some(side_b.to_proto()),
            start_fens: self.start_fens.clone(),
            scenario: self.scenario,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobSpecJson {
    Collect(CollectSpecJson),
    Rating(RatingSpecJson),
}

impl JobSpecJson {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn parse(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}

pub fn resolve_seed(strategy: &SeedStrategyJson, seq: i64) -> u64 {
    let seq = seq.max(0) as u64;
    match SeedStrategyKind::try_from(strategy.kind) {
        Ok(SeedStrategyKind::SeedFixed) => strategy.fixed_seed,
        Ok(SeedStrategyKind::SeedRandom) => splitmix64(strategy.base_seed ^ splitmix64(seq)),
        Ok(SeedStrategyKind::SeedIncremental) => strategy.base_seed.wrapping_add(seq),
        Ok(SeedStrategyKind::SeedRange) => {
            if strategy.max_seed <= strategy.min_seed {
                strategy.min_seed
            } else {
                let span = strategy.max_seed - strategy.min_seed;
                strategy.min_seed + splitmix64(strategy.base_seed ^ splitmix64(seq)) % (span + 1)
            }
        }
        _ => strategy.fixed_seed,
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

pub(super) fn spec_total(spec: &JobSpecJson) -> u64 {
    match spec {
        JobSpecJson::Collect(c) => c.total_games,
        JobSpecJson::Rating(r) => r.total_games,
    }
}

pub fn valid_scenario(scenario: i32) -> bool {
    OpeningScenario::try_from(scenario).is_ok()
}
