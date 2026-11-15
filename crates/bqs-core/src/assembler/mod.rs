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
use bqs_proto::CollectPlayerSpec;

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

    pub fn default_search(&self, kind: &str) -> CoreResult<String> {
        let defaults = crate::search_json::SearchJson::from_defaults(&self.cfg.selfplay.search);
        crate::search_json::canonicalize("", &defaults, kind).map_err(crate::CoreError::Internal)
    }

    pub fn player_with_search(&self, p: CollectPlayerSpec) -> CoreResult<CollectPlayerSpec> {
        let defaults = crate::search_json::SearchJson::from_defaults(&self.cfg.selfplay.search);

        let canonical = crate::search_json::canonicalize(&p.search_json, &defaults, &p.kind)
            .map_err(crate::CoreError::Internal)?;
        Ok(CollectPlayerSpec {
            search_json: canonical,
            ..p
        })
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
