use std::collections::HashMap;

use bqs_proto::*;

use crate::{CoreResult, balance::TrainerHeartbeatInput};

use super::Server;

impl Server {
    pub fn get_train_config(&self, req: GetTrainConfigRequest) -> CoreResult<GetTrainConfigReply> {
        if req.variant != self.cfg.selfplay.variant {
            return Ok(GetTrainConfigReply {
                accepted: false,
                message: format!(
                    "变体不符：请求 \"{}\"，服务端 \"{}\"",
                    req.variant, self.cfg.selfplay.variant
                ),
                ..Default::default()
            });
        }

        let overrides: HashMap<String, String> =
            self.ctl.train_config_overrides().into_iter().collect();
        Ok(GetTrainConfigReply {
            accepted: true,
            overrides,
            ..Default::default()
        })
    }

    pub fn trainer_heartbeat(
        &self,
        req: TrainerHeartbeatRequest,
    ) -> CoreResult<TrainerHeartbeatReply> {
        let outcome = self
            .balance
            .register_trainer_heartbeat(TrainerHeartbeatInput {
                trainer_id: req.trainer_id,
                variant: req.variant,
                kind: req.kind as i64,
                consumed_key: req.consumed_key,
                samples_consumed: req.samples_consumed,
                backlog: req.backlog as i64,
                buffered: req.buffered as i64,
                health_status: req.health_status,
                last_error: req.last_error,
                progress_json: req.progress_json,
            })?;
        Ok(TrainerHeartbeatReply {
            pause_self_play: self.ctl.effective_paused(),
            auto_paused: self.ctl.auto_paused(),
            auto_pause_reason: self.ctl.auto_pause_reason(),
            message: outcome.message,
        })
    }
}
