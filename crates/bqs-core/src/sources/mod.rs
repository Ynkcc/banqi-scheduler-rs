mod job;

#[cfg(test)]
mod tests;

pub use job::JobSource;

use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{TaskRequest, TaskResponse};

use crate::{
    CoreResult,
    assembler::TaskAssembler,
    control::Control,
    dispatch::TaskDispatcher,
    jobs::JobManager,
    ports::{MatchStore, NetworkStore},
};

pub trait TaskSource: Send + Sync {
    fn try_task(&self, req: &TaskRequest) -> CoreResult<Option<TaskResponse>>;
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub struct Sources;

impl Sources {
    #[allow(clippy::too_many_arguments)]
    pub fn assemble(
        jobs: Arc<JobManager>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        disp: Arc<TaskDispatcher>,
        asm: Arc<TaskAssembler>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
    ) -> Vec<Box<dyn TaskSource>> {
        vec![Box::new(JobSource {
            jobs,
            matches,
            nets,
            disp,
            asm,
            ctl,
            cfg,
        })]
    }
}
