mod producer;
mod tasks;

#[cfg(test)]
mod tests;

pub use producer::{episode_key, games_for_threads, network_key};

pub use crate::data_kind::{
    proto_for_format as data_kind_for_format,
    proto_for_weight_format as stack_data_kind_for_weight_format,
    weight_format_for_proto as stack_format_for_data_kind,
};

use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{CollectPlayerSpec, CollectSearchParams};

use crate::{CoreResult, consts, control::Control, ports::ObjectSigner};

pub struct TaskAssembler {
    signer: Arc<dyn ObjectSigner>,
    cfg: Arc<Config>,
    ctl: Arc<Control>,
}

impl TaskAssembler {
    pub fn new(signer: Arc<dyn ObjectSigner>, cfg: Arc<Config>, ctl: Arc<Control>) -> Self {
        Self { signer, cfg, ctl }
    }

    fn ttl_secs(&self) -> i64 {
        consts::PRESIGN_TTL.as_secs() as i64
    }

    pub fn default_search(&self) -> CollectSearchParams {
        let d = &self.cfg.selfplay.search;
        CollectSearchParams {
            mcts_sims: d.mcts_sims,
            mcts_max_considered_actions: d.mcts_max_considered_actions,
            mcts_c_scale: d.mcts_c_scale,
            mcts_gumbel_scale: d.mcts_gumbel_scale,
            temperature: 0.0,
            playout_cap_random: d.playout_cap_random,
            expectimax_node_budget: d.expectimax_node_budget,
            expectimax_max_depth: d.expectimax_max_depth,
            expl_enabled: d.expl_enabled,
            expl_topk: d.expl_topk,
            expl_temp_start: d.expl_temp_start,
            expl_temp_end: d.expl_temp_end,
            expl_temp_decay_steps: d.expl_temp_decay_steps,
            fast_mcts_sims: d.fast_mcts_sims,
            full_search_prob: d.full_search_prob,
            mcts_eta: d.mcts_eta,
            mcts_ucb_c: d.mcts_ucb_c,
            mcts_crn_enabled: d.mcts_crn_enabled,
            health_enabled: d.health_enabled,
            health_weight: d.health_weight,
            health_confidence_exp: d.health_confidence_exp,
            expl_mode: d.expl_mode.clone(),
            expectimax_root_fullwidth: d.expectimax_root_fullwidth,
        }
    }

    pub fn player_with_search(&self, p: CollectPlayerSpec) -> CollectPlayerSpec {
        CollectPlayerSpec {
            search: Some(p.search.unwrap_or_else(|| self.default_search())),
            ..p
        }
    }

    pub fn extra_config(&self) -> String {
        let mut revealed = self.ctl.initial_revealed();
        if revealed < 0 {
            revealed = 0;
        }
        if revealed == 0 {
            return String::new();
        }
        let obj = serde_json::json!({ "initial_revealed_pieces": revealed });
        obj.to_string()
    }

    fn with_network_download(
        &self,
        req: &bqs_proto::TaskRequest,
        resp: &mut bqs_proto::TaskResponse,
        sha: &str,
        key: &str,
    ) -> CoreResult<()> {
        if req.current_network != sha {
            resp.network_url = self.signer.presign_get(key, self.ttl_secs())?;
        }
        Ok(())
    }
}
