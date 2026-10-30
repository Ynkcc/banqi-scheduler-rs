mod derive;
mod job;
mod reana;

#[cfg(test)]
mod tests;

pub use derive::DeriveSource;
pub use job::JobSource;
pub use reana::ReanaSource;

use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{TaskRequest, TaskResponse};

use crate::{
    CoreResult,
    assembler::TaskAssembler,
    control::Control,
    dispatch::TaskDispatcher,
    featurepack::DeriveQueue,
    jobs::JobManager,
    ports::{MatchStore, NetworkStore},
    reanalysis::ReanaQueue,
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
        reana: Arc<ReanaQueue>,
        derive: Arc<DeriveQueue>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        disp: Arc<TaskDispatcher>,
        asm: Arc<TaskAssembler>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
    ) -> Vec<Box<dyn TaskSource>> {
        vec![
            Box::new(ReanaSource {
                reana: reana.clone(),
                disp: disp.clone(),
                asm: asm.clone(),
                ctl: ctl.clone(),
                cfg: cfg.clone(),
            }),
            Box::new(DeriveSource {
                derive,
                disp: disp.clone(),
                asm: asm.clone(),
            }),
            Box::new(JobSource {
                jobs,
                matches,
                nets,
                disp,
                reana,
                asm,
                ctl,
                cfg,
            }),
        ]
    }
}
