use bqs_proto::{CollectPlayerSpec, CollectionJobSpec, RatingJobSpec, SelfPlayJobSpec};

use super::JobManager;
use super::spec::PlayerSpecJson;
use crate::consts;

fn require_registered_sha(nets: &dyn crate::ports::NetworkStore, sha: &str, note: &str) -> Result<(), String> {
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

fn validate_player(p: &PlayerSpecJson, nets: &dyn crate::ports::NetworkStore) -> Result<(), String> {
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
    pub(super) fn validate_collect(&self, c: &CollectionJobSpec) -> Result<(), String> {
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
        validate_player(&pa, nets)?;
        validate_player(&pb, nets)?;
        if c.data_kind != bqs_proto::DataKind::DataResnet as i32
            && c.data_kind != bqs_proto::DataKind::DataNnue as i32
        {
            return Err("collect.data_kind 必须为 DATA_RESNET / DATA_NNUE".into());
        }
        Ok(())
    }

    pub(super) fn validate_selfplay(&self, s: &SelfPlayJobSpec) -> Result<(), String> {
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

    pub(super) fn validate_and_resolve_rating(
        &self,
        r: &RatingJobSpec,
    ) -> Result<(PlayerSpecJson, PlayerSpecJson), String> {
        if !r.variant.is_empty() {
            validate_variant(&r.variant)?;
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
}
