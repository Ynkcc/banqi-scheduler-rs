use serde::{Deserialize, Serialize};

pub const MAX_SEARCH_JSON_LEN: usize = 8192;

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchJsonIn {
    pub mcts: Option<MctsIn>,
    pub expectimax: Option<ExpectimaxIn>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MctsIn {
    pub sims: Option<u32>,
    pub max_considered_actions: Option<u32>,
    pub c_scale: Option<f64>,
    pub gumbel_scale: Option<f64>,
    pub playout_cap_random: Option<bool>,
    pub fast_sims: Option<u32>,
    pub full_search_prob: Option<f64>,
    pub eta: Option<u32>,
    pub ucb_c: Option<f64>,
    pub crn_enabled: Option<bool>,
    pub health: Option<HealthIn>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthIn {
    pub enabled: Option<bool>,
    pub weight: Option<f64>,
    pub confidence_exp: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExpectimaxIn {
    pub deterministic_depth: Option<u32>,
    pub chance_outcome_depth: Option<i32>,
    pub root_fullwidth: Option<bool>,
    pub expl: Option<ExplIn>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExplIn {
    pub enabled: Option<bool>,
    pub mode: Option<String>,
    pub topk: Option<u32>,
    pub temp_start: Option<f64>,
    pub temp_end: Option<f64>,
    pub temp_decay_steps: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct SearchJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcts: Option<MctsJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expectimax: Option<ExpectimaxJson>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MctsJson {
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
    pub health: HealthJson,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HealthJson {
    pub enabled: bool,
    pub weight: f64,
    pub confidence_exp: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExpectimaxJson {
    pub deterministic_depth: u32,

    pub chance_outcome_depth: i32,
    pub root_fullwidth: bool,
    pub expl: ExplJson,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExplJson {
    pub enabled: bool,
    pub mode: String,
    pub topk: u32,
    pub temp_start: f64,
    pub temp_end: f64,
    pub temp_decay_steps: u32,
}

impl SearchJson {
    pub fn from_defaults(d: &bqs_config::SearchDefaults) -> Self {
        let m = &d.mcts;
        let e = &d.expectimax;
        Self {
            mcts: Some(MctsJson {
                sims: m.sims,
                max_considered_actions: m.max_considered_actions,
                c_scale: m.c_scale,
                gumbel_scale: m.gumbel_scale,
                playout_cap_random: m.playout_cap_random,
                fast_sims: m.fast_sims,
                full_search_prob: m.full_search_prob,
                eta: m.eta,
                ucb_c: m.ucb_c,
                crn_enabled: m.crn_enabled,
                health: HealthJson {
                    enabled: m.health.enabled,
                    weight: m.health.weight,
                    confidence_exp: m.health.confidence_exp,
                },
            }),
            expectimax: Some(ExpectimaxJson {
                deterministic_depth: e.deterministic_depth,
                chance_outcome_depth: e.chance_outcome_depth,
                root_fullwidth: e.root_fullwidth,
                expl: ExplJson {
                    enabled: e.expl.enabled,
                    mode: e.expl.mode.clone(),
                    topk: e.expl.topk,
                    temp_start: e.expl.temp_start,
                    temp_end: e.expl.temp_end,
                    temp_decay_steps: e.expl.temp_decay_steps,
                },
            }),
        }
    }

    pub fn to_canonical(&self) -> Result<String, String> {
        let out = serde_json::to_string(self)
            .map_err(|e| format!("search_json 规范化序列化失败: {e}"))?;
        check_len(&out)?;
        Ok(out)
    }

    pub fn to_canonical_for_kind(&self, kind: &str) -> Result<String, String> {
        let single = match bq_policy::player_kind::canonical(kind) {
            Some("mcts_resnet") => SearchJson {
                mcts: self.mcts.clone(),
                expectimax: None,
            },
            Some("expectimax_nnue" | "expectimax_rule") => SearchJson {
                mcts: None,
                expectimax: self.expectimax.clone(),
            },
            _ => return Ok(String::new()),
        };
        single.to_canonical()
    }
}

fn check_len(s: &str) -> Result<(), String> {
    if s.len() > MAX_SEARCH_JSON_LEN {
        return Err(format!(
            "search_json 过长（{} > {MAX_SEARCH_JSON_LEN} 字节）",
            s.len()
        ));
    }
    Ok(())
}

fn unwrap_or<T>(job: Option<T>, def: T) -> T {
    match job {
        Some(v) => v,
        None => def,
    }
}

pub fn canonicalize(job_json: &str, defaults: &SearchJson, kind: &str) -> Result<String, String> {
    let trimmed = job_json.trim();
    check_len(trimmed)?;
    let job: SearchJsonIn = if trimmed.is_empty() {
        SearchJsonIn::default()
    } else {
        serde_json::from_str(trimmed)
            .map_err(|e| format!("search_json 非法 JSON 或含未知字段: {e}"))?
    };
    let merged = merge(&job, defaults);
    merged.to_canonical_for_kind(kind)
}

fn merge(job: &SearchJsonIn, defaults: &SearchJson) -> SearchJson {
    let d = defaults.mcts.as_ref().expect("defaults 必含 mcts 域");
    let j = job.mcts.clone().unwrap_or_default();
    let jh = j.health.unwrap_or_default();
    let mcts = MctsJson {
        sims: unwrap_or(j.sims, d.sims),
        max_considered_actions: unwrap_or(j.max_considered_actions, d.max_considered_actions),
        c_scale: unwrap_or(j.c_scale, d.c_scale),
        gumbel_scale: unwrap_or(j.gumbel_scale, d.gumbel_scale),
        playout_cap_random: unwrap_or(j.playout_cap_random, d.playout_cap_random),
        fast_sims: unwrap_or(j.fast_sims, d.fast_sims),
        full_search_prob: unwrap_or(j.full_search_prob, d.full_search_prob),
        eta: unwrap_or(j.eta, d.eta),
        ucb_c: unwrap_or(j.ucb_c, d.ucb_c),
        crn_enabled: unwrap_or(j.crn_enabled, d.crn_enabled),
        health: HealthJson {
            enabled: unwrap_or(jh.enabled, d.health.enabled),
            weight: unwrap_or(jh.weight, d.health.weight),
            confidence_exp: unwrap_or(jh.confidence_exp, d.health.confidence_exp),
        },
    };

    let d = defaults
        .expectimax
        .as_ref()
        .expect("defaults 必含 expectimax 域");
    let j = job.expectimax.clone().unwrap_or_default();
    let jx = j.expl.unwrap_or_default();
    let expectimax = ExpectimaxJson {
        deterministic_depth: unwrap_or(j.deterministic_depth, d.deterministic_depth),

        chance_outcome_depth: unwrap_or(j.chance_outcome_depth, d.chance_outcome_depth),
        root_fullwidth: unwrap_or(j.root_fullwidth, d.root_fullwidth),
        expl: ExplJson {
            enabled: unwrap_or(jx.enabled, d.expl.enabled),
            mode: unwrap_or(jx.mode, d.expl.mode.clone()),
            topk: unwrap_or(jx.topk, d.expl.topk),
            temp_start: unwrap_or(jx.temp_start, d.expl.temp_start),
            temp_end: unwrap_or(jx.temp_end, d.expl.temp_end),
            temp_decay_steps: unwrap_or(jx.temp_decay_steps, d.expl.temp_decay_steps),
        },
    };

    SearchJson {
        mcts: Some(mcts),
        expectimax: Some(expectimax),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqs_config::SearchDefaults;

    fn defaults() -> SearchJson {
        SearchJson::from_defaults(&SearchDefaults::default())
    }

    #[test]
    fn empty_job_yields_full_defaults_per_kind() {
        let out = canonicalize("", &defaults(), "mcts_resnet").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["mcts"]["sims"], 64);
        assert_eq!(v["mcts"]["max_considered_actions"], 16);
        assert_eq!(v["mcts"]["eta"], 2);
        assert_eq!(v["mcts"]["health"]["enabled"], false);
        assert!(
            v.get("expectimax").is_none(),
            "mcts_resnet 不得携带 expectimax 域（collector 跨域 fail-fast）: {out}"
        );

        let out = canonicalize("", &defaults(), "expectimax_nnue").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["expectimax"]["deterministic_depth"], 8);
        assert_eq!(v["expectimax"]["chance_outcome_depth"], 0);
        assert_eq!(v["expectimax"]["root_fullwidth"], true);
        assert_eq!(v["expectimax"]["expl"]["mode"], "gumbel");
        assert!(
            v.get("mcts").is_none(),
            "expectimax_nnue 不得携带 mcts 域: {out}"
        );
    }

    #[test]
    fn field_level_merge_overrides_only_given_fields() {
        let out = canonicalize(r#"{"mcts":{"sims":128}}"#, &defaults(), "mcts_resnet").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();

        assert_eq!(v["mcts"]["sims"], 128);

        assert_eq!(v["mcts"]["max_considered_actions"], 16);
        assert_eq!(v["mcts"]["full_search_prob"], 0.25);
        assert!(
            v.get("expectimax").is_none(),
            "mcts_resnet 裁剪后不含 expectimax 域"
        );
    }

    #[test]
    fn explicit_zero_chance_outcome_depth_is_preserved() {
        let out = canonicalize(
            r#"{"expectimax":{"chance_outcome_depth":0}}"#,
            &defaults(),
            "expectimax_nnue",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["expectimax"]["chance_outcome_depth"], 0);

        assert_eq!(v["expectimax"]["deterministic_depth"], 8);
    }

    #[test]
    fn no_search_kind_outputs_empty_string() {
        for kind in [
            "random",
            "rule:capture_first",
            "rule:reveal_first",
            "policy_resnet",
            "dmc_greedy",
        ] {
            let out = canonicalize(r#"{"mcts":{"sims":32}}"#, &defaults(), kind).unwrap();
            assert_eq!(out, "", "NoSearch kind {kind} 的 search_json 应为空串");
        }
    }

    #[test]
    fn alias_rule_material_expectimax_maps_to_expectimax_domain() {
        let out = canonicalize("", &defaults(), "rule:material_expectimax").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(
            v.get("expectimax").is_some() && v.get("mcts").is_none(),
            "别名 rule:material_expectimax 应映射到 expectimax 单域: {out}"
        );
    }

    #[test]
    fn unknown_field_rejected() {
        let err =
            canonicalize(r#"{"mcts":{"sims_zz":1}}"#, &defaults(), "mcts_resnet").unwrap_err();
        assert!(err.contains("search_json"), "{err}");
    }

    #[test]
    fn unknown_top_level_key_rejected() {
        let err = canonicalize(r#"{"nope":{}}"#, &defaults(), "mcts_resnet").unwrap_err();
        assert!(err.contains("search_json"), "{err}");
    }

    #[test]
    fn legacy_flat_format_rejected() {
        let err = canonicalize(r#"{"mcts_sims":64}"#, &defaults(), "mcts_resnet").unwrap_err();
        assert!(err.contains("search_json"), "{err}");
    }

    #[test]
    fn invalid_json_rejected() {
        let err = canonicalize("{oops", &defaults(), "mcts_resnet").unwrap_err();
        assert!(err.contains("search_json 非法 JSON"), "{err}");
    }

    #[test]
    fn oversized_job_rejected() {
        let big = format!(
            "{{\"mcts\":{{\"sims\":1}},\"pad\":\"{}\"}}",
            "x".repeat(9000)
        );
        let err = canonicalize(&big, &defaults(), "mcts_resnet").unwrap_err();
        assert!(err.contains("过长"), "{err}");
    }

    #[test]
    fn canonicalize_is_idempotent() {
        let once = canonicalize(r#"{"mcts":{"sims":32}}"#, &defaults(), "mcts_resnet").unwrap();
        let twice = canonicalize(&once, &defaults(), "mcts_resnet").unwrap();
        assert_eq!(once, twice, "canonical JSON 再合并应幂等");
    }
}
