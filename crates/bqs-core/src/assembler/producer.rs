use crate::{CoreError, CoreResult, types::{Match, Network}};

use super::{
    TaskAssembler, stack_format_for_data_kind,
};

pub fn network_key(sha: &str, format: &str) -> String {
    format!("networks/{sha}.{format}")
}

pub fn episode_key(network_sha: &str, data_id: &str) -> String {
    format!("episodes/{network_sha}/{data_id}.epb.gz")
}

pub fn games_for_threads(baseline: i32, base: i64, threads: i32) -> i64 {
    if baseline <= 0 {
        return base;
    }
    let t = (threads as i64).max(1);
    let g = base * t / baseline as i64;
    g.max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfplayMode {
    None,

    Pure,

    Mix,
}

#[derive(Debug, Clone)]
pub struct ProducerSelect {
    pub mode: SelfplayMode,

    pub producer: Option<Network>,

    pub m: Option<Match>,

    pub none_message: String,
}

pub fn select_producer(
    best: Option<&Network>,
    cand: Option<&Network>,
    alt: Option<&Network>,
    pm: Option<&Match>,
    data_kind: i32,
) -> Result<ProducerSelect, String> {
    let Some(best) = best else {
        return Ok(ProducerSelect {
            mode: SelfplayMode::None,
            producer: None,
            m: None,
            none_message: "no_best_network_registered".into(),
        });
    };
    if let (Some(pm), Some(cand)) = (pm, cand) {
        return Ok(ProducerSelect {
            mode: SelfplayMode::Mix,
            producer: Some(cand.clone()),
            m: Some(pm.clone()),
            none_message: String::new(),
        });
    }

    let Some(required_format) = stack_format_for_data_kind(data_kind) else {
        return Err(format!("resolve data kind stack: 未知数据类别 {data_kind}"));
    };
    if pm.is_none() && best.format != required_format {
        let Some(alt) = alt else {
            return Ok(ProducerSelect {
                mode: SelfplayMode::None,
                producer: None,
                m: None,
                none_message: format!("no_{required_format}_network_for_data_kind"),
            });
        };
        return Ok(ProducerSelect {
            mode: SelfplayMode::Pure,
            producer: Some(alt.clone()),
            m: None,
            none_message: String::new(),
        });
    }
    Ok(ProducerSelect {
        mode: SelfplayMode::Pure,
        producer: Some(best.clone()),
        m: None,
        none_message: String::new(),
    })
}

impl TaskAssembler {
    pub fn resolve_selfplay(
        &self,
        best: Option<Network>,
        pm: Option<Match>,
        data_kind: i32,
    ) -> CoreResult<ProducerSelect> {
        let mut cand: Option<Network> = None;
        if let Some(ref m) = pm {
            cand = self.nets.get_network(&m.candidate)?;
            if cand.is_none() {
                tracing::warn!(
                    "[task] 混入模式候选记录缺失 sha={}，本批回退纯自对弈",
                    m.candidate
                );
            }
        }

        let mut alt: Option<Network> = None;
        if pm.is_none() {
            if let Some(ref best) = best {
                match stack_format_for_data_kind(data_kind) {
                    Some(required_format) => {
                        if best.format != required_format {
                            alt = self.nets.latest_network_by_format(required_format)?;
                        }
                    }
                    None => {
                        return Err(CoreError::Internal(format!(
                            "resolve data kind stack: 未知数据类别 {data_kind}"
                        )));
                    }
                }
            }
        }
        let sel = select_producer(
            best.as_ref(),
            cand.as_ref(),
            alt.as_ref(),
            pm.as_ref(),
            data_kind,
        )
        .map_err(CoreError::Internal)?;
        if sel.mode == SelfplayMode::Pure {
            if let (Some(best), Some(producer)) = (best.as_ref(), sel.producer.as_ref()) {
                if producer != best {
                    tracing::info!(
                        "[task] best({}) 与数据类别架构不符，纯自对弈改用最新 {} 网络 {}",
                        best.format,
                        producer.format,
                        producer.sha
                    );
                }
            }
        }
        Ok(sel)
    }
}
