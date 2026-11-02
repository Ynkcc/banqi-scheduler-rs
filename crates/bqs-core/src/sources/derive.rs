use std::sync::Arc;

use bqs_proto::{TaskKind, TaskRequest, TaskResponse};

use crate::{
    assembler::TaskAssembler,
    dispatch::{RunningTask, TaskDispatcher},
    featurepack::DeriveQueue,
    CoreResult,
};

use super::{new_id, TaskSource};

pub struct DeriveSource {
    pub derive: Arc<DeriveQueue>,
    pub disp: Arc<TaskDispatcher>,
    pub asm: Arc<TaskAssembler>,
}

impl TaskSource for DeriveSource {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>> {
        let Some(item) = self.derive.peek() else {
            return Ok(None);
        };
        let task_id = new_id();
        match self.asm.derive_task(&task_id, &item) {
            Ok(resp) => {
                self.derive.commit();
                self.disp.register(
                    &task_id,
                    RunningTask {
                        games: 1,
                        worker_id: req.worker_id.clone(),
                        ..RunningTask::new(TaskKind::TaskDeriveFeatures)
                    },
                );
                tracing::info!(
                    "[task] derive assigned worker={} task={} episode={} pack_v{}",
                    req.worker_id,
                    task_id,
                    item.episode_object_key,
                    item.pack_version
                );
                Ok(Some(resp))
            }
            Err(e) => {
                tracing::warn!("[derive] 本次无法下发（{e}），保留队列待重试");
                Ok(None)
            }
        }
    }
}
