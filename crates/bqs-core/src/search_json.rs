use bq_engine_config::{
    DmcPartial, DmcSchema, ExpectimaxPartial, ExpectimaxSchema, MctsPartial, MctsSchema,
};
use serde::{Deserialize, Serialize};

pub const MAX_SEARCH_JSON_LEN: usize = 8192;

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchJsonIn {
    pub mcts: Option<MctsPartial>,
    pub expectimax: Option<ExpectimaxPartial>,
    pub dmc: Option<DmcPartial>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct SearchJsonOut<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    mcts: Option<&'a MctsSchema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expectimax: Option<&'a ExpectimaxSchema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dmc: Option<&'a DmcSchema>,
}

fn canonical_for_kind(
    mcts: &MctsSchema,
    expectimax: &ExpectimaxSchema,
    dmc: &DmcSchema,
    kind: &str,
) -> Result<String, String> {
    let out = match bq_policy::player_kind::canonical(kind) {
        Some("mcts_resnet") => serde_json::to_string(&SearchJsonOut {
            mcts: Some(mcts),
            expectimax: None,
            dmc: None,
        }),
        Some("expectimax_nnue" | "expectimax_rule") => serde_json::to_string(&SearchJsonOut {
            mcts: None,
            expectimax: Some(expectimax),
            dmc: None,
        }),
        Some("dmc_greedy") => serde_json::to_string(&SearchJsonOut {
            mcts: None,
            expectimax: None,
            dmc: Some(dmc),
        }),
        _ => return Ok(String::new()),
    };
    let out = out.map_err(|e| format!("search_json 规范化序列化失败: {e}"))?;
    check_len(&out)?;
    Ok(out)
}

pub fn canonicalize(
    job_json: &str,
    defaults: &bqs_config::SearchDefaults,
    kind: &str,
) -> Result<String, String> {
    let trimmed = job_json.trim();
    check_len(trimmed)?;
    let job: SearchJsonIn = if trimmed.is_empty() {
        SearchJsonIn::default()
    } else {
        serde_json::from_str(trimmed)
            .map_err(|e| format!("search_json 非法 JSON 或含未知字段: {e}"))?
    };

    let mut mcts = defaults.mcts;
    if let Some(p) = job.mcts {
        mcts.merge_partial(p);
    }
    let mut expectimax = defaults.expectimax;
    if let Some(p) = job.expectimax {
        expectimax.merge_partial(p);
    }
    let mut dmc = defaults.dmc;
    if let Some(p) = job.dmc {
        dmc.merge_partial(p);
    }

    canonical_for_kind(&mcts, &expectimax, &dmc, kind)
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

#[cfg(test)]
mod tests {
    use super::*;
    use bqs_config::SearchDefaults;

    fn defaults() -> SearchDefaults {
        SearchDefaults::default()
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
        assert_eq!(v["expectimax"]["fullwidth"], false);
        assert!(
            v["expectimax"]["expl"].is_null(),
            "schema 默认 expl = None（探索关闭），canonical 输出 expl:null: {out}"
        );
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
        ] {
            let out = canonicalize(r#"{"mcts":{"sims":32}}"#, &defaults(), kind).unwrap();
            assert_eq!(out, "", "NoSearch kind {kind} 的 search_json 应为空串");
        }
    }

    #[test]
    fn dmc_greedy_yields_dmc_domain() {
        let out = canonicalize("", &defaults(), "dmc_greedy").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["dmc"]["eps"], 0.0, "默认 = 纯 argmax 贪心: {out}");
        assert_eq!(v["dmc"]["near_gap"], 0.0);
        assert!(v.get("mcts").is_none() && v.get("expectimax").is_none());

        let out = canonicalize(
            r#"{"dmc":{"eps":0.15,"near_gap":0.05}}"#,
            &defaults(),
            "dmc_greedy",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["dmc"]["eps"], 0.15);
        assert_eq!(v["dmc"]["near_gap"], 0.05);

        let out = canonicalize(r#"{"mcts":{"sims":32}}"#, &defaults(), "dmc_greedy").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("mcts").is_none(), "mcts 域应被裁剪掉: {out}");
        assert_eq!(v["dmc"]["eps"], 0.0);
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
    fn legacy_domain_keys_are_accepted_as_aliases() {
        let out = canonicalize(r#"{"mcts":{"mcts_sims":64}}"#, &defaults(), "mcts_resnet").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v["mcts"]["sims"], 64,
            "旧平铺 alias mcts_sims 应读入并写出新名"
        );

        let out = canonicalize(
            r#"{"expectimax":{"expectimax_deterministic_depth":4}}"#,
            &defaults(),
            "expectimax_nnue",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["expectimax"]["deterministic_depth"], 4);

        let out = canonicalize(
            r#"{"mcts":{"health_enabled":true,"health_weight":0.2}}"#,
            &defaults(),
            "mcts_resnet",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v["mcts"]["health"]["enabled"], true,
            "打平旧名归并进 health 子结构"
        );
        assert_eq!(v["mcts"]["health"]["weight"], 0.2);

        let out = canonicalize(
            r#"{"expectimax":{"expl_topk":4}}"#,
            &defaults(),
            "expectimax_nnue",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v["expectimax"]["expl"]["topk"], 4,
            "打平旧名应映射进 expl 子结构"
        );
    }

    #[test]
    fn legacy_top_level_flat_format_rejected() {
        let err = canonicalize(r#"{"mcts_sims":64}"#, &defaults(), "mcts_resnet").unwrap_err();
        assert!(err.contains("search_json"), "{err}");
    }

    #[test]
    fn expl_mode_invalid_value_rejected_at_parse() {
        let err = canonicalize(
            r#"{"expectimax":{"expl":{"enabled":true,"mode":"boltzmann"}}}"#,
            &defaults(),
            "expectimax_nnue",
        )
        .unwrap_err();
        assert!(err.contains("search_json 非法 JSON 或含未知字段"), "{err}");
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

        let once =
            canonicalize(r#"{"mcts":{"mcts_sims":32}}"#, &defaults(), "mcts_resnet").unwrap();
        let twice = canonicalize(&once, &defaults(), "mcts_resnet").unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn defaults_schema_dumps_canonical_directly() {
        let d = defaults();
        let mcts_out = serde_json::to_string(&super::SearchJsonOut {
            mcts: Some(&d.mcts),
            expectimax: None,
            dmc: None,
        })
        .unwrap();
        assert_eq!(
            canonicalize("", &d, "mcts_resnet").unwrap(),
            mcts_out,
            "空 job 合并结果 = 默认 schema 的单域 serde dump"
        );
        let e_out = serde_json::to_string(&super::SearchJsonOut {
            mcts: None,
            expectimax: Some(&d.expectimax),
            dmc: None,
        })
        .unwrap();
        assert_eq!(canonicalize("", &d, "expectimax_nnue").unwrap(), e_out);
        let dmc_out = serde_json::to_string(&super::SearchJsonOut {
            mcts: None,
            expectimax: None,
            dmc: Some(&d.dmc),
        })
        .unwrap();
        assert_eq!(canonicalize("", &d, "dmc_greedy").unwrap(), dmc_out);
    }
}
