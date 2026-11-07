use bqs_proto::{CollectPlayerSpec, CollectionJobSpec, RatingJobSpec};

use super::spec::{valid_scenario, PlayerSpecJson};
use super::JobManager;
use crate::consts;

fn resolve_network_sha(
    nets: &dyn crate::ports::NetworkStore,
    sha: &str,
    note: &str,
) -> Result<String, String> {
    let sha = sha.trim();
    if sha.is_empty() {
        return Ok(String::new());
    }
    let exact = nets
        .get_network(sha)
        .map_err(|e| format!("查询网络登记失败: {e}"))?;
    if exact.is_some() {
        return Ok(sha.to_string());
    }
    if sha.len() >= 64 {
        return Err(format!("network_not_registered sha={sha}{note}"));
    }
    let prefix = sha.to_ascii_lowercase();
    let mut hits: Vec<String> = nets
        .list_networks()
        .map_err(|e| format!("查询网络登记失败: {e}"))?
        .into_iter()
        .filter(|n| n.sha.to_ascii_lowercase().starts_with(&prefix))
        .map(|n| n.sha)
        .collect();
    match hits.len() {
        0 => Err(format!(
            "network_not_registered sha={sha}{note}（无前缀匹配的已登记网络）"
        )),
        1 => Ok(hits.remove(0)),
        n => Err(format!(
            "network_sha=\"{sha}\" 前缀匹配到 {n} 个已登记网络，存在歧义: {}",
            hits.iter()
                .map(|s| &s[..12.min(s.len())])
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn check_model_sha(sha: &str, msg: String) -> Result<(), String> {
    if sha.is_empty() {
        Ok(())
    } else {
        Err(msg)
    }
}

fn resolve_player(
    p: &PlayerSpecJson,
    nets: &dyn crate::ports::NetworkStore,
) -> Result<PlayerSpecJson, String> {
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
        let mut out = p.clone();
        out.network_sha =
            resolve_network_sha(nets, &p.network_sha, "（模型类棋手的网络必须已登记）")?;
        Ok(out)
    } else {
        check_model_sha(
            &p.network_sha,
            format!("player_kind={kind} 的 network_sha 必须为空"),
        )?;
        Ok(p.clone())
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

const MAX_START_FENS: usize = 1024;

fn validate_start_fens(start_fens: &[String], variant: &str) -> Result<(), String> {
    if start_fens.is_empty() {
        return Ok(());
    }
    if start_fens.len() > MAX_START_FENS {
        return Err(format!(
            "start_fens 条数超限（{} > {MAX_START_FENS}）",
            start_fens.len()
        ));
    }
    for (i, fen) in start_fens.iter().enumerate() {
        if fen.trim().is_empty() {
            return Err(format!("start_fens[{i}] 为空串"));
        }
        let snap = bq_env::PositionSnapshot::from_fen(fen)
            .map_err(|e| format!("start_fens[{i}] 非法 BFEN: {e}"))?;
        if snap.variant.as_str() != variant {
            return Err(format!(
                "start_fens[{i}] 变体不匹配: BFEN 为 \"{}\"，任务为 \"{variant}\"",
                snap.variant.as_str()
            ));
        }
    }
    Ok(())
}

impl JobManager {
    pub(super) fn resolve_collect(
        &self,
        c: &CollectionJobSpec,
    ) -> Result<CollectionJobSpec, String> {
        validate_variant(&c.variant)?;
        validate_start_fens(&c.start_fens, &c.variant)?;
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
        let pa = resolve_player(&pa, nets)?;
        let pb = resolve_player(&pb, nets)?;
        if c.data_kind != bq_record::DataKind::DataResnet as i32
            && c.data_kind != bq_record::DataKind::DataNnue as i32
        {
            return Err("collect.data_kind 必须为 DATA_RESNET / DATA_NNUE".into());
        }
        let mut out = c.clone();
        out.player_a = Some(pa.to_proto());
        out.player_b = Some(pb.to_proto());
        Ok(out)
    }

    pub(super) fn validate_and_resolve_rating(
        &self,
        r: &RatingJobSpec,
    ) -> Result<(RatingJobSpec, PlayerSpecJson, PlayerSpecJson), String> {
        let mut r = r.clone();
        if !r.variant.is_empty() {
            validate_variant(&r.variant)?;
        }
        if !valid_scenario(r.scenario) {
            return Err(format!(
                "rating.scenario 非法: {}（可选 0=standard / 1=all_revealed）",
                r.scenario
            ));
        }
        if !r.start_fens.is_empty() {
            let resolved = if r.variant.is_empty() {
                self.cfg.selfplay.variant.as_str()
            } else {
                r.variant.as_str()
            };
            validate_start_fens(&r.start_fens, resolved)?;
        }

        if r.total_games != 0 && (r.total_games < 2 || r.total_games % 2 != 0) {
            return Err(format!(
                "rating.total_games 必须为 0（仅 SPRT 判停）或 ≥2 且为偶数，当前 {}",
                r.total_games
            ));
        }

        if r.sprt_elo1 <= r.sprt_elo0 {
            return Err(format!(
                "rating.sprt_elo1 ({}) 必须大于 rating.sprt_elo0 ({})",
                r.sprt_elo1, r.sprt_elo0
            ));
        }
        for (name, v) in [("alpha", r.sprt_alpha), ("beta", r.sprt_beta)] {
            if !(0.0 < v && v < 1.0) {
                return Err(format!("rating.sprt_{name} 必须在 (0,1)，当前 {v}"));
            }
        }

        if !r.candidate_sha.is_empty() {
            r.candidate_sha = resolve_network_sha(
                self.nets.as_ref(),
                &r.candidate_sha,
                "（rating 候选网络必须已登记）",
            )?;
        }
        if !r.opponent_sha.is_empty() {
            r.opponent_sha = resolve_network_sha(
                self.nets.as_ref(),
                &r.opponent_sha,
                "（rating 对手网络必须已登记）",
            )?;
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
            return Ok((r, a, b));
        }

        if r.candidate_sha.is_empty() {
            return Err("rating.candidate_sha required（被测网络必填）".into());
        }

        if r.opponent_sha.is_empty() {
            return Err(
                "rating.opponent_sha required（best 语义已移除，对手网络必须显式指定）".into(),
            );
        }
        let mk = |sha: &str| PlayerSpecJson {
            kind: "expectimax_nnue".into(),
            network_sha: sha.to_string(),
            search: None,
        };
        let (a, b) = (mk(&r.candidate_sha), mk(&r.opponent_sha));
        Ok((r, a, b))
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
                return Err(format!(
                    "rating.{label} 模型类棋手 network_sha 必填（best 语义已移除，不再按 best 回退）"
                ));
            }
            out.network_sha = resolve_network_sha(
                self.nets.as_ref(),
                &out.network_sha,
                &format!("（rating.{label} 模型类网络必须已登记）"),
            )?;
        } else {
            check_model_sha(
                &out.network_sha,
                format!("rating.{label} player_kind={kind} 的 network_sha 必须为空"),
            )?;
        }
        Ok(out)
    }
}
