mod derive;
mod job;
mod reana;

#[cfg(test)]
mod tests;

pub use derive::DeriveSource;
pub use job::{JobPhase, JobSource};
pub use reana::ReanaSource;

use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{TaskRequest, TaskResponse};

use crate::{
    CoreResult,
    assembler::TaskAssembler,
    control::Control,
    dispatch::TaskDispatcher,
    evalcoord::EvalCoordinator,
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
    pub fn assemble(
        jobs: Arc<JobManager>,
        evalc: Arc<EvalCoordinator>,
        reana: Arc<ReanaQueue>,
        derive: Arc<DeriveQueue>,
        matches: Arc<dyn MatchStore>,
        nets: Arc<dyn NetworkStore>,
        disp: Arc<TaskDispatcher>,
        asm: Arc<TaskAssembler>,
        ctl: Arc<Control>,
        cfg: Arc<Config>,
    ) -> Vec<Box<dyn TaskSource>> {
        let mk = |phase: JobPhase| {
            Box::new(JobSource {
                jobs: jobs.clone(),
                evalc: evalc.clone(),
                matches: matches.clone(),
                nets: nets.clone(),
                disp: disp.clone(),
                reana: reana.clone(),
                asm: asm.clone(),
                ctl: ctl.clone(),
                cfg: cfg.clone(),
                phase,
            }) as Box<dyn TaskSource>
        };
        vec![
            mk(JobPhase::High),
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
            mk(JobPhase::Low),
        ]
    }
}
