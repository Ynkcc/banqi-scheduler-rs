use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bqs_config::Config;
use bqs_proto::{
    CollectPlayerSpec, CollectSearchParams, CollectionJobSpec, EvalJobSpec, JobKind,
    OpeningScenario, RatingJobSpec, SeedStrategy, SeedStrategyKind, SelfPlayJobSpec,
    SubmitJobRequest,
};

use crate::{
    CoreResult, consts,
    control::Control,
    dispatch::TaskDispatcher,
    ports::{Clock, JobRow, JobStore, MatchStore, NetworkStore},
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
}

impl CollectSpecJson {
    fn from_proto(s: CollectionJobSpec) -> Self {
        Self {
            variant: s.variant,
            scenario: s.scenario,
            seed_strategy: SeedStrategyJson::from_proto(s.seed_strategy),
            player_a: PlayerSpecJson::from_proto(s.player_a),
            player_b: PlayerSpecJson::from_proto(s.player_b),
            data_kind: s.data_kind,
            total_games: s.total_games,
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
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SelfPlaySpecJson {
    pub variant: String,
    #[serde(alias = "data_kind")]
    pub data_kind: i32,
    #[serde(alias = "network_sha")]
    pub network_sha: String,
    #[serde(alias = "opponent_sha")]
    pub opponent_sha: String,
    #[serde(alias = "mix_ratio")]
    pub mix_ratio: f64,
    pub search: Option<SearchSpecJson>,
    #[serde(alias = "total_games")]
    pub total_games: u64,
}

impl SelfPlaySpecJson {
    fn from_proto(s: SelfPlayJobSpec) -> Self {
        Self {
            variant: s.variant,
            data_kind: s.data_kind,
            network_sha: s.network_sha,
            opponent_sha: s.opponent_sha,
            mix_ratio: s.mix_ratio,
            search: s.search.map(|x| SearchSpecJson::from_proto(Some(x))),
            total_games: s.total_games,
        }
    }

    pub fn to_proto(&self) -> SelfPlayJobSpec {
        SelfPlayJobSpec {
            variant: self.variant.clone(),
            data_kind: self.data_kind,
            network_sha: self.network_sha.clone(),
            opponent_sha: self.opponent_sha.clone(),
            mix_ratio: self.mix_ratio,
            search: self.search.as_ref().and_then(|s| s.to_proto()),
            total_games: self.total_games,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EvalSpecJson {
    pub variant: String,
    #[serde(alias = "network_sha")]
    pub network_sha: String,
    #[serde(alias = "opponent_kind")]
    pub opponent_kind: String,
    #[serde(alias = "opponent_network_sha")]
    pub opponent_network_sha: String,
    pub search: Option<SearchSpecJson>,
    #[serde(alias = "total_games")]
    pub total_games: u64,
}

impl EvalSpecJson {
    fn from_proto(s: EvalJobSpec) -> Self {
        Self {
            variant: s.variant,
            network_sha: s.network_sha,
            opponent_kind: s.opponent_kind,
            opponent_network_sha: s.opponent_network_sha,
            search: s.search.map(|x| SearchSpecJson::from_proto(Some(x))),
            total_games: s.total_games,
        }
    }

    pub fn to_proto(&self) -> EvalJobSpec {
        EvalJobSpec {
            variant: self.variant.clone(),
            network_sha: self.network_sha.clone(),
            opponent_kind: self.opponent_kind.clone(),
            opponent_network_sha: self.opponent_network_sha.clone(),
            search: self.search.as_ref().and_then(|s| s.to_proto()),
            total_games: self.total_games,
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
}

impl RatingSpecJson {
    fn from_proto(s: RatingJobSpec) -> Self {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobSpecJson {
    Collect(CollectSpecJson),
    SelfPlay(SelfPlaySpecJson),
    Eval(EvalSpecJson),
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

fn require_registered_sha(nets: &dyn NetworkStore, sha: &str, note: &str) -> Result<(), String> {
    if sha.is_empty() {
        return Ok(());
    }
    let registered = nets
        .get_network(sha)
        .map_err(|e| format!("查询网络登记失败: {e}"))?;
    if registered.is_none() {
        return Err(format!("network_not_registered sha={sha}{note}"));
    }
    Ok(())
}

fn check_model_sha(sha: &str, msg: String) -> Result<(), String> {
    if sha.is_empty() { Ok(()) } else { Err(msg) }
}

fn validate_player(p: &PlayerSpecJson, nets: &dyn NetworkStore) -> Result<(), String> {
    let Some(kind) = bq_policy::player_kind::canonical(&p.kind) else {
        return Err(format!(
            "invalid_player_kind=\"{}\"（可选 {}）",
            p.kind,
            bq_policy::player_kind::PLAYER_KINDS.join("/")
        ));
    };
    if bq_policy::player_kind::requires_model(kind) {
        if p.network_sha.is_empty() {
            return Err(format!("player_kind={kind} 需要 network_sha"));
        }
        require_registered_sha(nets, &p.network_sha, "（模型类棋手的网络必须已登记）")
    } else {
        check_model_sha(
            &p.network_sha,
            format!("player_kind={kind} 的 network_sha 必须为空"),
        )
    }
}

fn validate_variant(v: &str) -> Result<(), String> {
    if consts::JOB_VARIANTS.contains(&v) {
        Ok(())
    } else {
        Err(format!(
            "invalid_variant=\"{v}\"（可选 {}）",
            consts::JOB_VARIANTS.join("/")
        ))
    }
}

#[derive(Default)]
struct Inner {
    cancelled: HashMap<String, String>,

    last_preempt: Option<i64>,
}

pub struct JobManager {
    store: Arc<dyn JobStore>,
    disp: Arc<TaskDispatcher>,
    nets: Arc<dyn NetworkStore>,
    matches: Arc<dyn MatchStore>,
    ctl: Arc<Control>,
    cfg: Arc<Config>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
}

impl JobManager {
    pub fn new(
        store: Arc<dyn JobStore>,
        disp: Arc<TaskDispatcher>,
        nets: Arc<dyn NetworkStore>,
        matches: Arc<dyn MatchStore>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            disp,
            nets,
            matches,
            ctl,
            cfg,
            clock,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn now(&self) -> i64 {
        self.clock.now()
    }

    fn default_priority(&self, kind: JobKind) -> i64 {
        match kind {
            JobKind::JobSelfplay => self.cfg.jobs.priority_selfplay,
            JobKind::JobCollect => self.cfg.jobs.priority_collect,
            JobKind::JobEval => self.cfg.jobs.priority_eval,
            JobKind::JobRating => self.cfg.jobs.priority_rating,
        }
    }

    fn insert_new_job(
        &self,
        kind: JobKind,
        priority: i64,
        total: i64,
        spec: &JobSpecJson,
    ) -> CoreResult<String> {
        let job_id = crate::sources::new_id();
        let now = self.now();
        self.store.insert_job(JobRow {
            job_id: job_id.clone(),
            kind: kind as i64,
            priority,
            state: "running".into(),
            total_games: total,
            completed_games: 0,
            dispatched_games: 0,
            spec_json: spec.to_json(),
            created_at: now,
            updated_at: now,
            next_seq: 0,
        })?;
        Ok(job_id)
    }

    pub fn submit(&self, req: &SubmitJobRequest) -> CoreResult<(bool, String, String)> {
        let Ok(kind) = JobKind::try_from(req.kind) else {
            return Ok((
                false,
                format!("invalid_job_kind={}", req.kind),
                String::new(),
            ));
        };
        let priority = if req.priority == 0 {
            self.default_priority(kind)
        } else {
            req.priority as i64
        };
        let spec: JobSpecJson = match kind {
            JobKind::JobCollect => {
                let Some(c) = &req.collect else {
                    return Ok((
                        false,
                        "collect spec required for JOB_COLLECT".into(),
                        String::new(),
                    ));
                };
                match self.validate_collect(c) {
                    Ok(()) => JobSpecJson::Collect(CollectSpecJson::from_proto(c.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobSelfplay => {
                let Some(s) = &req.selfplay else {
                    return Ok((
                        false,
                        "selfplay spec required for JOB_SELFPLAY".into(),
                        String::new(),
                    ));
                };
                match self.validate_selfplay(s) {
                    Ok(()) => JobSpecJson::SelfPlay(SelfPlaySpecJson::from_proto(s.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobEval => {
                let Some(e) = &req.eval else {
                    return Ok((
                        false,
                        "eval spec required for JOB_EVAL".into(),
                        String::new(),
                    ));
                };
                match self.validate_eval(e) {
                    Ok(()) => JobSpecJson::Eval(EvalSpecJson::from_proto(e.clone())),
                    Err(m) => return Ok((false, m, String::new())),
                }
            }
            JobKind::JobRating => {
                let Some(r) = &req.rating else {
                    return Ok((
                        false,
                        "rating spec required for JOB_RATING".into(),
                        String::new(),
                    ));
                };

                let (side_a, side_b) = match self.validate_and_resolve_rating(r) {
                    Ok(v) => v,
                    Err(m) => return Ok((false, m, String::new())),
                };
                let mut spec = RatingSpecJson::from_proto(r.clone());
                spec.side_a = side_a.clone();
                spec.side_b = side_b.clone();

                let match_id = self.matches.create_match(
                    &side_a.network_sha,
                    &side_b.network_sha,
                    (spec.total_games / 2) as i64,
                )?;
                spec.match_id = match_id;
                tracing::info!(
                    "[job] rating match created match={} side_a={}:{} side_b={}:{} target_pairs={}",
                    match_id,
                    side_a.kind,
                    side_a.network_sha,
                    side_b.kind,
                    side_b.network_sha,
                    spec.total_games / 2
                );
                JobSpecJson::Rating(spec)
            }
        };
        let total = spec_total(&spec) as i64;
        let job_id = self.insert_new_job(kind, priority, total, &spec)?;
        tracing::info!(
            "[job] submitted requester={} id={} kind={} priority={} total={}",
            req.requester,
            job_id,
            kind.as_str_name(),
            priority,
            total
        );

        self.preemption_sweep(true);
        Ok((true, String::new(), job_id))
    }

    fn validate_collect(&self, c: &CollectionJobSpec) -> Result<(), String> {
        validate_variant(&c.variant)?;
        if c.total_games < 2 || c.total_games % 2 != 0 {
            return Err(format!(
                "collect.total_games 必须 ≥2 且为偶数，当前 {}",
                c.total_games
            ));
        }
        let nets = self.nets.as_ref();
        let pa = c
            .player_a
            .as_ref()
            .map(|p| PlayerSpecJson::from_proto(Some(p.clone())))
            .ok_or("collect.player_a required")?;
        let pb = c
            .player_b
            .as_ref()
            .map(|p| PlayerSpecJson::from_proto(Some(p.clone())))
            .ok_or("collect.player_b required")?;
        validate_player(&pa, nets)?;
        validate_player(&pb, nets)?;
        if c.data_kind != bqs_proto::DataKind::DataResnet as i32
            && c.data_kind != bqs_proto::DataKind::DataNnue as i32
        {
            return Err("collect.data_kind 必须为 DATA_RESNET / DATA_NNUE".into());
        }
        Ok(())
    }

    fn validate_selfplay(&self, s: &SelfPlayJobSpec) -> Result<(), String> {
        validate_variant(&s.variant)?;
        if s.data_kind != bqs_proto::DataKind::DataResnet as i32
            && s.data_kind != bqs_proto::DataKind::DataNnue as i32
        {
            return Err("selfplay.data_kind 必须为 DATA_RESNET / DATA_NNUE".into());
        }

        for sha in [&s.network_sha, &s.opponent_sha] {
            require_registered_sha(self.nets.as_ref(), sha, "")?;
        }
        Ok(())
    }

    fn validate_eval(&self, e: &EvalJobSpec) -> Result<(), String> {
        validate_variant(&e.variant)?;
        if e.total_games < 2 || e.total_games % 2 != 0 {
            return Err(format!(
                "eval.total_games 必须 ≥2 且为偶数，当前 {}",
                e.total_games
            ));
        }
        let Some(kind) = bq_policy::player_kind::canonical(&e.opponent_kind) else {
            return Err(format!(
                "invalid_opponent_kind=\"{}\"（可选 {}）",
                e.opponent_kind,
                bq_policy::player_kind::PLAYER_KINDS.join("/")
            ));
        };
        let is_model = bq_policy::player_kind::requires_model(kind);
        if is_model {
            if e.opponent_network_sha.is_empty() {
                return Err("eval.opponent_network_sha required for model opponent".into());
            }
            require_registered_sha(
                self.nets.as_ref(),
                &e.opponent_network_sha,
                "（eval 对手网络必须已登记）",
            )?;
        } else {
            check_model_sha(
                &e.opponent_network_sha,
                "eval.opponent_network_sha 必须为空（非模型类对手）".into(),
            )?;
        }

        require_registered_sha(self.nets.as_ref(), &e.network_sha, "")
    }

    fn validate_and_resolve_rating(
        &self,
        r: &RatingJobSpec,
    ) -> Result<(PlayerSpecJson, PlayerSpecJson), String> {
        if !r.variant.is_empty() {
            validate_variant(&r.variant)?;
        }

        if r.total_games != 0 && (r.total_games < 2 || r.total_games % 2 != 0) {
            return Err(format!(
                "rating.total_games 必须为 0（仅 SPRT 判停）或 ≥2 且为偶数，当前 {}",
                r.total_games
            ));
        }

        for (name, v) in [("alpha", r.sprt_alpha), ("beta", r.sprt_beta)] {
            if v != 0.0 && !(0.0 < v && v < 1.0) {
                return Err(format!("rating.sprt_{name} 必须在 (0,1)，当前 {v}"));
            }
        }
        if r.sprt_elo0 != 0.0 && r.sprt_elo1 != 0.0 && r.sprt_elo1 <= r.sprt_elo0 {
            return Err(format!(
                "rating.sprt_elo1 ({}) 必须大于 rating.sprt_elo0 ({})",
                r.sprt_elo1, r.sprt_elo0
            ));
        }
        if r.side_a.is_some() || r.side_b.is_some() {
            let Some(sa) = r.side_a.as_ref() else {
                return Err("rating.side_a/side_b 必须成对填写（sides 形态）".into());
            };
            let Some(sb) = r.side_b.as_ref() else {
                return Err("rating.side_a/side_b 必须成对填写（sides 形态）".into());
            };
            let a = self.normalize_rating_side("side_a", sa)?;
            let b = self.normalize_rating_side("side_b", sb)?;
            return Ok((a, b));
        }

        if r.candidate_sha.is_empty() {
            return Err("rating.candidate_sha required（被测网络必填）".into());
        }
        require_registered_sha(
            self.nets.as_ref(),
            &r.candidate_sha,
            "（rating 候选网络必须已登记）",
        )?;

        let opponent_sha = if r.opponent_sha.is_empty() {
            match self
                .nets
                .get_best()
                .map_err(|e| format!("查询 best 网络失败: {e}"))?
            {
                Some(b) => b.sha,
                None => {
                    return Err(
                        "no_best_network_registered（对手空 = 当前 best，但尚无 best 网络）".into(),
                    );
                }
            }
        } else {
            require_registered_sha(
                self.nets.as_ref(),
                &r.opponent_sha,
                "（rating 对手网络必须已登记）",
            )?;
            r.opponent_sha.clone()
        };
        let mk = |sha: &str| PlayerSpecJson {
            kind: "expectimax_nnue".into(),
            network_sha: sha.to_string(),
            search: None,
        };
        Ok((mk(&r.candidate_sha), mk(&opponent_sha)))
    }

    fn normalize_rating_side(
        &self,
        label: &str,
        p: &CollectPlayerSpec,
    ) -> Result<PlayerSpecJson, String> {
        let Some(kind) = bq_policy::player_kind::canonical(&p.kind) else {
            return Err(format!(
                "rating.{label} invalid_player_kind=\"{}\"（可选 {}）",
                p.kind,
                bq_policy::player_kind::PLAYER_KINDS.join("/")
            ));
        };
        let mut out = PlayerSpecJson::from_proto(Some(p.clone()));
        out.kind = kind.to_string();
        if bq_policy::player_kind::requires_model(kind) {
            if out.network_sha.is_empty() {
                match self
                    .nets
                    .get_best()
                    .map_err(|e| format!("查询 best 网络失败: {e}"))?
                {
                    Some(b) => out.network_sha = b.sha,
                    None => {
                        return Err(format!(
                            "no_best_network_registered（rating.{label} 模型类 sha 空 = 当前 best，但尚无 best 网络）"
                        ));
                    }
                }
            } else {
                require_registered_sha(
                    self.nets.as_ref(),
                    &out.network_sha,
                    &format!("（rating.{label} 模型类网络必须已登记）"),
                )?;
            }
        } else {
            check_model_sha(
                &out.network_sha,
                format!("rating.{label} player_kind={kind} 的 network_sha 必须为空"),
            )?;
        }
        Ok(out)
    }

    pub fn list(&self, state: Option<&str>, kind: Option<i64>) -> CoreResult<Vec<JobRow>> {
        self.store.list_jobs(state, kind)
    }

    pub fn get(&self, job_id: &str) -> CoreResult<Option<JobRow>> {
        self.store.get_job(job_id)
    }

    pub fn control(&self, job_id: &str, action: &str) -> CoreResult<(bool, String, String)> {
        let Some(job) = self.store.get_job(job_id)? else {
            return Ok((false, format!("job_not_found:{job_id}"), String::new()));
        };
        let now = self.now();
        match action {
            "pause" => {
                if job.state != "running" {
                    return Ok((
                        false,
                        format!("pause_requires_running state={}", job.state),
                        job.state,
                    ));
                }
                self.store.update_job_state(job_id, "paused", now)?;
                tracing::info!("[job] paused id={job_id}");
                Ok((true, String::new(), "paused".into()))
            }
            "resume" => {
                if job.state != "paused" {
                    return Ok((
                        false,
                        format!("resume_requires_paused state={}", job.state),
                        job.state,
                    ));
                }
                self.store.update_job_state(job_id, "running", now)?;
                tracing::info!("[job] resumed id={job_id}");

                self.preemption_sweep(true);
                Ok((true, String::new(), "running".into()))
            }
            "cancel" => {
                if job.state == "completed" || job.state == "cancelled" {
                    return Ok((
                        false,
                        format!("job_is_terminal state={}", job.state),
                        job.state,
                    ));
                }
                self.store.update_job_state(job_id, "cancelled", now)?;

                self.cancel_in_flight_for_job(job_id);

                if job.kind == JobKind::JobRating as i64 {
                    self.cancel_rating_match(&job.spec_json);
                }
                tracing::info!("[job] cancelled id={job_id}（在飞任务已加入取消集合）");
                Ok((true, String::new(), "cancelled".into()))
            }
            other => Ok((
                false,
                format!("invalid_control=\"{other}\"（可选 pause/resume/cancel）"),
                job.state,
            )),
        }
    }

    pub fn note_dispatch(
        &self,
        task_id: &str,
        job_id: &str,
        games: i64,
        worker_id: &str,
        kind: bqs_proto::TaskKind,
        network_sha: &str,
        opponent_sha: &str,
        opponent_spec: &str,
        match_id: i64,
        deadline_secs_override: Option<i64>,
    ) -> CoreResult<i64> {
        let row = self.store.incr_job_dispatched(job_id, games, self.now())?;
        self.store.insert_job_task(task_id, job_id, games)?;
        let mut t = crate::dispatch::RunningTask::new(kind);
        t.worker_id = worker_id.to_string();
        t.network_sha = network_sha.to_string();
        t.opponent_sha = opponent_sha.to_string();
        t.opponent_spec = opponent_spec.to_string();
        t.games = games;
        t.match_id = match_id;
        t.job_id = job_id.to_string();
        self.disp.register(task_id, t);
        if let Some(extra) = deadline_secs_override {
            if extra > 0 {
                self.disp.override_deadline(task_id, self.now() + extra);
            }
        }
        Ok(row.next_seq - 1)
    }

    pub fn note_eval_claim(&self, task_id: &str, job_id: &str) -> CoreResult<()> {
        let games = self.disp.lookup(task_id).map(|t| t.games).unwrap_or(0);
        let row = self.store.incr_job_dispatched(job_id, games, self.now())?;
        let _ = row;
        self.store.insert_job_task(task_id, job_id, games)
    }

    pub fn record_report(&self, task_id: &str, games: i64) -> CoreResult<()> {
        let Some(link) = self.store.get_job_task(task_id)? else {
            return Ok(());
        };
        if games <= 0 {
            return Ok(());
        }
        let row = self
            .store
            .incr_job_completed(&link.job_id, games, self.now())?;
        self.store.delete_job_task(task_id)?;
        let total = row.total_games;
        if total > 0 && row.completed_games >= total && row.state == "running" {
            self.store
                .update_job_state(&link.job_id, "completed", self.now())?;
            self.cancel_in_flight_for_job(&link.job_id);
            tracing::info!(
                "[job] ✅ completed id={} completed={}/{}（在飞任务已加入取消集合）",
                link.job_id,
                row.completed_games,
                total
            );
        } else if total > 0 {
            tracing::info!(
                "[job] += id={} task={} games={} completed={}/{}",
                link.job_id,
                task_id,
                games,
                row.completed_games,
                total
            );
        } else {
            tracing::info!(
                "[job] += id={} task={} games={} completed={}（持续型）",
                link.job_id,
                task_id,
                games,
                row.completed_games
            );
        }
        Ok(())
    }

    pub fn cancel_in_flight_for_job(&self, job_id: &str) {
        let in_flight: Vec<(String, String)> = self
            .disp
            .running()
            .into_iter()
            .filter(|t| t.job_id == job_id)
            .map(|t| (t.task_id, t.worker_id))
            .collect();
        let mut inner = self.inner.lock().unwrap();
        for (task_id, worker_id) in in_flight {
            inner.cancelled.entry(task_id).or_insert(worker_id);
        }
    }

    pub fn preemption_sweep(&self, force: bool) {
        let now = self.now();
        {
            let mut inner = self.inner.lock().unwrap();
            if !force {
                if let Some(last) = inner.last_preempt {
                    if now - last < consts::JOB_PREEMPT_INTERVAL.as_secs() as i64 {
                        return;
                    }
                }
            }
            inner.last_preempt = Some(now);
        }
        let jobs = match self.store.list_jobs(None, None) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!("[job] ⚠️ 抢占评估读取 job 列表失败: {e}");
                return;
            }
        };
        let max_dispatchable = jobs
            .iter()
            .filter(|j| {
                j.state == "running" && (j.total_games == 0 || j.completed_games < j.total_games)
            })
            .map(|j| j.priority)
            .max();
        let Some(max_p) = max_dispatchable else {
            return;
        };
        for j in jobs.iter().filter(|j| {
            j.state == "running"
                && j.priority < max_p
                && (j.total_games == 0 || j.completed_games < j.total_games)
        }) {
            self.cancel_in_flight_for_job(&j.job_id);
        }
    }

    pub fn cancel_task_ids_for_worker(&self, worker_id: &str) -> Vec<String> {
        let mut inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        let ids: Vec<String> = inner.cancelled.keys().cloned().collect();
        for id in ids {
            match self.disp.lookup(&id) {
                Some(t) if t.worker_id == worker_id => {
                    out.push(id.clone());
                    inner.cancelled.remove(&id);
                }
                Some(_) => {}
                None => {
                    inner.cancelled.remove(&id);
                }
            }
        }
        out
    }

    fn cancel_rating_match(&self, spec_json: &str) {
        if let Some(JobSpecJson::Rating(spec)) = JobSpecJson::parse(spec_json) {
            if spec.match_id != 0 {
                match self.matches.cancel_match(spec.match_id) {
                    Ok(true) => {
                        tracing::info!("[job] rating match {} cancelled（不晋级）", spec.match_id)
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(
                            "[job] ⚠️ rating match 取消失败 match={}: {e}",
                            spec.match_id
                        )
                    }
                }
            }
        }
    }

    pub fn complete_rating_job(&self, job_id: &str) -> CoreResult<()> {
        let Some(job) = self.store.get_job(job_id)? else {
            return Ok(());
        };
        if job.state != "running" && job.state != "paused" {
            return Ok(());
        }
        self.store
            .update_job_state(job_id, "completed", self.now())?;
        self.cancel_in_flight_for_job(job_id);
        tracing::info!(
            "[job] ✅ rating job completed id={job_id}（对打已判停/达上限，在飞已取消）"
        );
        Ok(())
    }

    pub fn rating_spec(&self, job_id: &str) -> CoreResult<Option<RatingSpecJson>> {
        if job_id.is_empty() {
            return Ok(None);
        }
        Ok(self
            .store
            .get_job(job_id)?
            .and_then(|j| match JobSpecJson::parse(&j.spec_json) {
                Some(JobSpecJson::Rating(r)) => Some(r),
                _ => None,
            }))
    }

    pub fn is_rating_match(&self, match_id: i64) -> bool {
        self.store
            .list_jobs(None, Some(JobKind::JobRating as i64))
            .map(|rows| {
                rows.iter().any(|j| {
                    matches!(
                        JobSpecJson::parse(&j.spec_json),
                        Some(JobSpecJson::Rating(r)) if r.match_id == match_id
                    )
                })
            })
            .unwrap_or(false)
    }

    pub fn ensure_selfplay_job(&self) -> CoreResult<()> {
        let existing = self
            .store
            .list_jobs(None, Some(JobKind::JobSelfplay as i64))?;
        if existing
            .iter()
            .any(|j| j.state == "running" || j.state == "paused")
        {
            return Ok(());
        }
        let spec = SelfPlaySpecJson {
            variant: self.cfg.selfplay.variant.clone(),
            data_kind: crate::data_kind::to_proto(self.ctl.data_kind()),
            network_sha: String::new(),
            opponent_sha: String::new(),
            mix_ratio: 0.0,
            search: None,
            total_games: 0,
        };
        let job_id = self.insert_new_job(
            JobKind::JobSelfplay,
            self.default_priority(JobKind::JobSelfplay),
            0,
            &JobSpecJson::SelfPlay(spec),
        )?;
        tracing::info!("[job] bootstrapped persistent selfplay job id={job_id}");
        Ok(())
    }

    pub fn create_eval_job(
        &self,
        network_sha: &str,
        opponent_kind: &str,
        opponent_sha: &str,
    ) -> CoreResult<()> {
        let sha = if network_sha.is_empty() {
            match self.nets.get_best()? {
                Some(b) => b.sha,
                None => return Ok(()),
            }
        } else {
            network_sha.to_string()
        };
        let total = self.cfg.eval.games;
        let existing = self.store.list_jobs(None, Some(JobKind::JobEval as i64))?;
        let dup = existing.iter().any(|j| {
            (j.state == "running" || j.state == "paused")
                && JobSpecJson::parse(&j.spec_json)
                    .map(|s| {
                        matches!(
                            &s,
                            JobSpecJson::Eval(e)
                                if e.network_sha == sha
                                    && e.opponent_kind == opponent_kind
                                    && e.opponent_network_sha == opponent_sha
                        )
                    })
                    .unwrap_or(false)
        });
        if dup {
            return Ok(());
        }
        let spec = JobSpecJson::Eval(EvalSpecJson {
            variant: self.cfg.selfplay.variant.clone(),
            network_sha: sha.clone(),
            opponent_kind: opponent_kind.to_string(),
            opponent_network_sha: opponent_sha.to_string(),
            search: None,
            total_games: total as u64,
        });
        let job_id = self.insert_new_job(
            JobKind::JobEval,
            self.default_priority(JobKind::JobEval),
            total,
            &spec,
        )?;
        tracing::info!(
            "[job] auto eval job id={} network={sha} opponent={opponent_kind} total={total}",
            job_id
        );

        self.preemption_sweep(true);
        Ok(())
    }

    pub fn dispatchable(&self, kind: JobKind) -> CoreResult<Vec<JobRow>> {
        let all = self.store.list_jobs(None, Some(kind as i64))?;
        Ok(all
            .into_iter()
            .filter(|j| {
                j.state == "running" && (j.total_games == 0 || j.completed_games < j.total_games)
            })
            .collect())
    }
}

fn spec_total(spec: &JobSpecJson) -> u64 {
    match spec {
        JobSpecJson::Collect(c) => c.total_games,
        JobSpecJson::SelfPlay(s) => s.total_games,
        JobSpecJson::Eval(e) => e.total_games,
        JobSpecJson::Rating(r) => r.total_games,
    }
}

pub fn valid_scenario(scenario: i32) -> bool {
    OpeningScenario::try_from(scenario).is_ok()
}

#[cfg(test)]
mod tests;
