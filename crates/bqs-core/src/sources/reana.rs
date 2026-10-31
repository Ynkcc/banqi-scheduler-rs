use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{TaskKind, TaskRequest, TaskResponse};

use crate::{
    CoreResult,
    assembler::TaskAssembler,
    control::Control,
    dispatch::{RunningTask, TaskDispatcher},
    reanalysis::ReanaQueue,
};

use super::{TaskSource, new_id};

pub struct ReanaSource {
    pub reana: Arc<ReanaQueue>,
    pub disp: Arc<TaskDispatcher>,
    pub asm: Arc<TaskAssembler>,
    pub ctl: Arc<Control>,
    pub cfg: Arc<Config>,
}

impl TaskSource for ReanaSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        if self.ctl.auto_paused() && self.cfg.balance.pause_reanalysis {
            return Ok(None);
        }

        let Some(item) = self.reana.peek() else {
            return Ok(None);
        };
        let task_id = new_id();
        match self.asm.reanalysis_task(&task_id, &item, req) {
            Ok(resp) => {
                self.reana.commit();
                self.disp.register(
                    &task_id,
                    RunningTask {
                        network_sha: resp.network_sha.clone(),
                        games: item.positions as i64,
                        worker_id: req.worker_id.clone(),
                        ..RunningTask::new(TaskKind::TaskReanalysis)
                    },
                );
                tracing::info!(
                    "[task] reanalysis assigned worker={} task={} network={} positions={}",
                    req.worker_id,
                    task_id,
                    resp.network_sha,
                    item.positions
                );
                Ok(Some(resp))
            }
            Err(e) => {
                tracing::warn!("[reanalysis] 本次无法下发（{e}），保留队列待重试");
                Ok(None)
            }
        }
    }
}
